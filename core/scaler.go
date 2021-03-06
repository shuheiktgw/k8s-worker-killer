package core

import (
	ctx "context"
	"fmt"
	"time"

	"github.com/shuheiktgw/k8s-worker-killer/cloudprovider"

	apiv1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1beta1"
	kube_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	kube_client "k8s.io/client-go/kubernetes"
	kube_record "k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

const (
	// MaxPodEvictionTime is the maximum time Killer tries to evict a pod before giving up.
	MaxPodEvictionTime = 2 * time.Minute
	// EvictionRetryTime is the time after Killer retries failed pod eviction.
	EvictionRetryTime = 10 * time.Second
	// PodEvictionHeadroom is the extra time we wait to catch situations when the pod is ignoring SIGTERM and
	// is killed with SIGKILL after MaxGracefulTerminationTime
	PodEvictionHeadroom = 30 * time.Second
)

type Scaler interface {
	DeleteNode(node *apiv1.Node, pods []*apiv1.Pod) status.NodeDeleteResult
}

func NewScalerImpl(clientSet kube_client.Interface, provder cloudprovider.CloudProvider, recorder kube_record.EventRecorder, tainter Tainter) Scaler {
	return &ScalerImpl{
		clientSet:     clientSet,
		cloudprocider: provder,
		recorder:      recorder,
		tainter:       tainter,
	}
}

type ScalerImpl struct {
	clientSet     kube_client.Interface
	cloudprocider cloudprovider.CloudProvider
	recorder      kube_record.EventRecorder
	tainter       Tainter
}

// Originally from https://github.com/kubernetes/autoscaler/blob/655b4081f424b3d2dfd5cc2f155f6c2a81d6416b/cluster-autoscaler/core/scale_down.go#L1193-L1267
func (s *ScalerImpl) evictPod(podToEvict *apiv1.Pod, maxGracefulTerminationSec int, retryUntil time.Time, waitBetweenRetries time.Duration) status.PodEvictionResult {
	s.recorder.Eventf(podToEvict, apiv1.EventTypeNormal, "ScaleDown", "deleting pod for node scale down")

	maxTermination := int64(apiv1.DefaultTerminationGracePeriodSeconds)
	if podToEvict.Spec.TerminationGracePeriodSeconds != nil {
		if *podToEvict.Spec.TerminationGracePeriodSeconds < int64(maxGracefulTerminationSec) {
			maxTermination = *podToEvict.Spec.TerminationGracePeriodSeconds
		} else {
			maxTermination = int64(maxGracefulTerminationSec)
		}
	}

	var lastError error
	for first := true; first || time.Now().Before(retryUntil); time.Sleep(waitBetweenRetries) {
		first = false
		eviction := &policyv1.Eviction{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: podToEvict.Namespace,
				Name:      podToEvict.Name,
			},
			DeleteOptions: &metav1.DeleteOptions{
				GracePeriodSeconds: &maxTermination,
			},
		}
		lastError = s.clientSet.CoreV1().Pods(podToEvict.Namespace).Evict(ctx.TODO(), eviction)
		if lastError == nil || kube_errors.IsNotFound(lastError) {
			return status.PodEvictionResult{Pod: podToEvict, TimedOut: false, Err: nil}
		}
	}
	klog.Errorf("Failed to evict pod %s, error: %v", podToEvict.Name, lastError)
	s.recorder.Eventf(podToEvict, apiv1.EventTypeWarning, "ScaleDownFailed", "failed to delete pod for ScaleDown")
	return status.PodEvictionResult{Pod: podToEvict, TimedOut: true, Err: fmt.Errorf("failed to evict pod %s/%s within allowed timeout (last error: %v)", podToEvict.Namespace, podToEvict.Name, lastError)}
}

// Performs drain logic on the node. Marks the node as unschedulable and later removes all pods, giving
// them up to MaxGracefulTerminationTime to finish.
// Originally from https://github.com/kubernetes/autoscaler/blob/655b4081f424b3d2dfd5cc2f155f6c2a81d6416b/cluster-autoscaler/core/scale_down.go#L1193-L1267
func (s *ScalerImpl) drainNode(node *apiv1.Node, pods []*apiv1.Pod, maxGracefulTerminationSec int, maxPodEvictionTime time.Duration, waitBetweenRetries time.Duration, podEvictionHeadroom time.Duration) (evictionResults map[string]status.PodEvictionResult, err error) {
	evictionResults = make(map[string]status.PodEvictionResult)
	toEvict := len(pods)
	retryUntil := time.Now().Add(maxPodEvictionTime)
	confirmations := make(chan status.PodEvictionResult, toEvict)
	for _, pod := range pods {
		evictionResults[pod.Name] = status.PodEvictionResult{Pod: pod, TimedOut: true, Err: nil}
		go func(podToEvict *apiv1.Pod) {
			confirmations <- s.evictPod(podToEvict, maxGracefulTerminationSec, retryUntil, waitBetweenRetries)
		}(pod)
	}

	for range pods {
		select {
		case evictionResult := <-confirmations:
			evictionResults[evictionResult.Pod.Name] = evictionResult
		case <-time.After(retryUntil.Sub(time.Now()) + 5*time.Second):
			// All pods initially had results with TimedOut set to true, so the ones that didn't receive an actual result are correctly marked as timed out.
			return evictionResults, errors.NewAutoscalerError(errors.ApiCallError, "Failed to drain node %s/%s: timeout when waiting for creating evictions", node.Namespace, node.Name)
		}
	}

	evictionErrs := make([]error, 0)
	for _, result := range evictionResults {
		if !result.WasEvictionSuccessful() {
			evictionErrs = append(evictionErrs, result.Err)
		}
	}
	if len(evictionErrs) != 0 {
		return evictionResults, errors.NewAutoscalerError(errors.ApiCallError, "Failed to drain node %s/%s, due to following errors: %v", node.Namespace, node.Name, evictionErrs)
	}

	// Evictions created successfully, wait maxGracefulTerminationSec + podEvictionHeadroom to see if pods really disappeared.
	var allGone bool
	for start := time.Now(); time.Now().Sub(start) < time.Duration(maxGracefulTerminationSec)*time.Second+podEvictionHeadroom; time.Sleep(5 * time.Second) {
		allGone = true
		for _, pod := range pods {
			podreturned, err := s.clientSet.CoreV1().Pods(pod.Namespace).Get(ctx.TODO(), pod.Name, metav1.GetOptions{})
			if err == nil && (podreturned == nil || podreturned.Spec.NodeName == node.Name) {
				klog.Errorf("Not deleted yet %s/%s", pod.Namespace, pod.Name)
				allGone = false
				break
			}
			if err != nil && !kube_errors.IsNotFound(err) {
				klog.Errorf("Failed to check pod %s/%s: %v", pod.Namespace, pod.Name, err)
				allGone = false
				break
			}
		}
		if allGone {
			klog.V(1).Infof("All pods removed from %s", node.Name)
			// Let the deferred function know there is no need for cleanup
			return evictionResults, nil
		}
	}

	for _, pod := range pods {
		podReturned, err := s.clientSet.CoreV1().Pods(pod.Namespace).Get(ctx.TODO(), pod.Name, metav1.GetOptions{})
		if err == nil && (podReturned == nil || podReturned.Spec.NodeName == node.Name) {
			evictionResults[pod.Name] = status.PodEvictionResult{Pod: pod, TimedOut: true, Err: nil}
		} else if err != nil && !kube_errors.IsNotFound(err) {
			evictionResults[pod.Name] = status.PodEvictionResult{Pod: pod, TimedOut: true, Err: err}
		} else {
			evictionResults[pod.Name] = status.PodEvictionResult{Pod: pod, TimedOut: false, Err: nil}
		}
	}

	return evictionResults, errors.NewAutoscalerError(errors.TransientError, "Failed to drain node %s/%s: pods remaining after timeout", node.Namespace, node.Name)
}

// Originally from https://github.com/kubernetes/autoscaler/blob/655b4081f424b3d2dfd5cc2f155f6c2a81d6416b/cluster-autoscaler/core/scale_down.go#L1108-L1154
func (s *ScalerImpl) DeleteNode(node *apiv1.Node, pods []*apiv1.Pod) status.NodeDeleteResult {
	deleteSuccessful := false
	drainSuccessful := false

	if err := s.tainter.MarkToBeDeleted(node); err != nil {
		s.recorder.Eventf(node, apiv1.EventTypeWarning, "ScaleDownFailed", "failed to mark the node as toBeDeleted/unschedulable: %v", err)
		return status.NodeDeleteResult{ResultType: status.NodeDeleteErrorFailedToMarkToBeDeleted, Err: errors.ToAutoscalerError(errors.ApiCallError, err)}
	}

	// If we fail to evict all the pods from the node we want to remove delete taint
	defer func() {
		if !deleteSuccessful {
			s.tainter.CleanToBeDeleted(node)
			if !drainSuccessful {
				s.recorder.Eventf(node, apiv1.EventTypeWarning, "ScaleDownFailed", "failed to drain the node, aborting ScaleDown")
			} else {
				s.recorder.Eventf(node, apiv1.EventTypeWarning, "ScaleDownFailed", "failed to delete the node")
			}
		}
	}()

	// attempt drain
	// TODO: make MaxGracefulTerminationSec configurable
	evictionResults, err := s.drainNode(node, pods, 600, MaxPodEvictionTime, EvictionRetryTime, PodEvictionHeadroom)
	if err != nil {
		return status.NodeDeleteResult{ResultType: status.NodeDeleteErrorFailedToEvictPods, Err: err, PodEvictionResults: evictionResults}
	}
	drainSuccessful = true

	if err := s.cloudprocider.DeleteNode(node); err != nil {
		return status.NodeDeleteResult{ResultType: status.NodeDeleteErrorFailedToDelete, Err: err}
	}

	deleteSuccessful = true // Let the deferred function know there is no need to cleanup
	return status.NodeDeleteResult{ResultType: status.NodeDeleteOk}
}
