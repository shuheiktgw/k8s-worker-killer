// Originally from https://github.com/kubernetes/autoscaler/blob/655b4081f424b3d2dfd5cc2f155f6c2a81d6416b/cluster-autoscaler/utils/deletetaint/delete.go
package core

import (
	"context"
	"fmt"
	kube_record "k8s.io/client-go/tools/record"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube_client "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const ToBeDeletedTaint = "ToBeDeletedByK8sWorkerKiller"

// Mutable only in unit tests
var (
	maxRetryDeadline      = 5 * time.Second
	conflictRetryInterval = 750 * time.Millisecond
)

type Tainter interface {
	MarkToBeDeleted(node *apiv1.Node) error
	CleanToBeDeleted(node *apiv1.Node) (bool, error)
	CleanAllToBeDeleted(nodes []*apiv1.Node)
}

func NewTainterImpl(clientSet kube_client.Interface, recorder kube_record.EventRecorder) Tainter {
	return &TainterImpl{
		clientSet: clientSet,
		recorder:  recorder,
	}
}

type TainterImpl struct {
	clientSet kube_client.Interface
	recorder  kube_record.EventRecorder
}

// MarkToBeDeleted sets a taint that makes the node unschedulable.
func (t *TainterImpl) MarkToBeDeleted(node *apiv1.Node) error {
	return t.addTaint(node, ToBeDeletedTaint, apiv1.TaintEffectNoSchedule)
}

func (t *TainterImpl) addTaint(node *apiv1.Node, taintKey string, effect apiv1.TaintEffect) error {
	retryDeadline := time.Now().Add(maxRetryDeadline)
	freshNode := node.DeepCopy()
	var err error
	refresh := false
	for {
		if refresh {
			// Get the newest version of the node.
			freshNode, err = t.clientSet.CoreV1().Nodes().Get(context.TODO(), node.Name, metav1.GetOptions{})
			if err != nil || freshNode == nil {
				klog.Warningf("Error while adding %v taint on node %v: %v", taintKey, node.Name, err)
				return fmt.Errorf("failed to get node %v: %v", node.Name, err)
			}
		}

		if !addTaintToSpec(freshNode, taintKey, effect) {
			if !refresh {
				// Make sure we have the latest version before skipping update.
				refresh = true
				continue
			}
			return nil
		}
		_, err = t.clientSet.CoreV1().Nodes().Update(context.TODO(), freshNode, metav1.UpdateOptions{})
		if err != nil && errors.IsConflict(err) && time.Now().Before(retryDeadline) {
			refresh = true
			time.Sleep(conflictRetryInterval)
			continue
		}

		if err != nil {
			klog.Warningf("Error while adding %v taint on node %v: %v", taintKey, node.Name, err)
			return err
		}
		klog.V(1).Infof("Successfully added %v on node %v", taintKey, node.Name)
		return nil
	}
}

func addTaintToSpec(node *apiv1.Node, taintKey string, effect apiv1.TaintEffect) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Key == taintKey {
			klog.V(2).Infof("%v already present on node %v, taint: %v", taintKey, node.Name, taint)
			return false
		}
	}
	node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{
		Key:    taintKey,
		Value:  fmt.Sprint(time.Now().Unix()),
		Effect: effect,
	})
	return true
}

// CleanToBeDeleted cleans CA's NoSchedule taint from a node.
func (t *TainterImpl) CleanToBeDeleted(node *apiv1.Node) (bool, error) {
	return t.cleanTaint(node, ToBeDeletedTaint)
}

func (t *TainterImpl) cleanTaint(node *apiv1.Node, taintKey string) (bool, error) {
	retryDeadline := time.Now().Add(maxRetryDeadline)
	freshNode := node.DeepCopy()
	var err error
	refresh := false
	for {
		if refresh {
			// Get the newest version of the node.
			freshNode, err = t.clientSet.CoreV1().Nodes().Get(context.TODO(), node.Name, metav1.GetOptions{})
			if err != nil || freshNode == nil {
				klog.Warningf("Error while adding %v taint on node %v: %v", taintKey, node.Name, err)
				return false, fmt.Errorf("failed to get node %v: %v", node.Name, err)
			}
		}
		newTaints := make([]apiv1.Taint, 0)
		for _, taint := range freshNode.Spec.Taints {
			if taint.Key == taintKey {
				klog.V(1).Infof("Releasing taint %+v on node %v", taint, node.Name)
			} else {
				newTaints = append(newTaints, taint)
			}
		}
		if len(newTaints) == len(freshNode.Spec.Taints) {
			if !refresh {
				// Make sure we have the latest version before skipping update.
				refresh = true
				continue
			}
			return false, nil
		}

		freshNode.Spec.Taints = newTaints
		_, err = t.clientSet.CoreV1().Nodes().Update(context.TODO(), freshNode, metav1.UpdateOptions{})

		if err != nil && errors.IsConflict(err) && time.Now().Before(retryDeadline) {
			refresh = true
			time.Sleep(conflictRetryInterval)
			continue
		}

		if err != nil {
			klog.Warningf("Error while releasing %v taint on node %v: %v", taintKey, node.Name, err)
			return false, err
		}
		klog.V(1).Infof("Successfully released %v on node %v", taintKey, node.Name)
		return true, nil
	}
}

// CleanAllToBeDeleted cleans ToBeDeleted taints from given nodes.
func (t *TainterImpl) CleanAllToBeDeleted(nodes []*apiv1.Node) {
	t.cleanAllTaints(nodes, ToBeDeletedTaint)
}

func (t *TainterImpl) cleanAllTaints(nodes []*apiv1.Node, taintKey string) {
	for _, node := range nodes {
		if !hasTaint(node, taintKey) {
			continue
		}
		cleaned, err := t.cleanTaint(node, taintKey)
		if err != nil {
			t.recorder.Eventf(node, apiv1.EventTypeWarning, "ClusterAutoscalerCleanup",
				"failed to clean %v on node %v: %v", taintKey, node.Name, err)
		} else if cleaned {
			t.recorder.Eventf(node, apiv1.EventTypeNormal, "ClusterAutoscalerCleanup",
				"removed %v taint from node %v", taintKey, node.Name)
		}
	}
}

func hasTaint(node *apiv1.Node, taintKey string) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Key == taintKey {
			return true
		}
	}
	return false
}
