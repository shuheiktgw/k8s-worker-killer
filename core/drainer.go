package core

import (
	"fmt"
	"time"

	apiv1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	autoscaler_drain "k8s.io/autoscaler/cluster-autoscaler/utils/drain"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	pod_util "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
)

type Drainer interface {
	GetPodsToEvict(pods []*apiv1.Pod, pdbs []*policyv1.PodDisruptionBudget) ([]*apiv1.Pod, error)
}

type drain interface {
	GetPodsForDeletionOnNodeDrain(pods []*apiv1.Pod, pdbs []*policyv1.PodDisruptionBudget) ([]*apiv1.Pod, error)
}

type drainWrapper struct {
	registry kube_util.ListerRegistry
}

func (d *drainWrapper) GetPodsForDeletionOnNodeDrain(pods []*apiv1.Pod, pdbs []*policyv1.PodDisruptionBudget) ([]*apiv1.Pod, error) {
	// TODO: make those arguments configurable
	podsForDeletion, _, err := autoscaler_drain.GetPodsForDeletionOnNodeDrain(pods, pdbs, false, false, false, d.registry, 0, time.Now())
	return podsForDeletion, err
}

func NewDrainerImpl(registry kube_util.ListerRegistry) Drainer {
	return &DrainerImpl{
		drain: &drainWrapper{registry: registry},
	}
}

type DrainerImpl struct {
	drain drain
}

func (d *DrainerImpl) GetPodsToEvict(pods []*apiv1.Pod, pdbs []*policyv1.PodDisruptionBudget) ([]*apiv1.Pod, error) {
	podsForDeletion, err := d.drain.GetPodsForDeletionOnNodeDrain(pods, pdbs)

	if err != nil {
		return nil, fmt.Errorf("failed to get pods for deletion on node drain: %w", err)
	}

	if _, err := checkPdbs(podsForDeletion, pdbs); err != nil {
		return nil, fmt.Errorf("pdb check failed: %w", err)
	}

	podsToEvict := make([]*apiv1.Pod, 0)

	for _, pod := range podsForDeletion {
		if !pod_util.IsDaemonSetPod(pod) {
			podsToEvict = append(podsToEvict, pod)
		}
	}

	return podsToEvict, err
}

// Originally from https://github.com/kubernetes/autoscaler/blob/73a5cdf928d3b04ac5cbc456a60d5eb084f9cbc1/cluster-autoscaler/simulator/drain.go#L93-L109
func checkPdbs(pods []*apiv1.Pod, pdbs []*policyv1.PodDisruptionBudget) (*autoscaler_drain.BlockingPod, error) {
	// TODO: make it more efficient.
	for _, pdb := range pdbs {
		selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			return nil, err
		}
		for _, pod := range pods {
			if pod.Namespace == pdb.Namespace && selector.Matches(labels.Set(pod.Labels)) {
				if pdb.Status.DisruptionsAllowed < 1 {
					return &autoscaler_drain.BlockingPod{Pod: pod, Reason: autoscaler_drain.NotEnoughPdb}, fmt.Errorf("not enough pod disruption budget to move %s/%s", pod.Namespace, pod.Name)
				}
			}
		}
	}
	return nil, nil
}
