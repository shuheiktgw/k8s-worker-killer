package core

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/shuheiktgw/k8s-worker-killer/config"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/klog/v2"
)

type Killer interface {
	Run() error
	ExitCleanUp()
}

type BasicKiller struct {
	listerRegistry kube_util.ListerRegistry
	options        *config.KillingOptions
	drainer        Drainer
	scaler         Scaler
	tainter        Tainter
}

func NewBasicKiller(registry kube_util.ListerRegistry, options *config.KillingOptions, drainer Drainer, scaler Scaler, tainter Tainter) Killer {
	return &BasicKiller{
		listerRegistry: registry,
		options:        options,
		drainer:        drainer,
		scaler:         scaler,
		tainter:        tainter,
	}
}

func (k *BasicKiller) Run() error {
	nodes, err := k.listerRegistry.ReadyNodeLister().List()
	if err != nil {
		klog.Errorf("Unable to list ready nodes: %v", err)
		return err
	}

	sort.Slice(nodes, func(i, j int) bool {
		x := nodes[i].GetCreationTimestamp()
		y := nodes[j].GetCreationTimestamp()
		return x.Before(&y)
	})

	var candidates []*apiv1.Node
	now := time.Now()

	for _, node := range nodes {
		creationTimestamp := node.GetCreationTimestamp()
		klog.V(4).Infof("Node %s age: %v", node.GetName(), now.Sub(creationTimestamp.Time))

		if creationTimestamp.Add(k.options.MaxAge).Before(now) {
			candidates = append(candidates, node)
		} else {
			break
		}
	}

	if len(candidates) == 0 {
		klog.V(1).Infof("No candidate node has been found")
		return nil
	}

	klog.V(4).Infof("Candidate count: %v", len(candidates))

	pdbs, err := k.listerRegistry.PodDisruptionBudgetLister().List()
	if err != nil {
		klog.Errorf("Unable to list pdbs: %v", err)
		return err
	}

	podsOnNodes, err := k.getPodsOnNodes()
	if err != nil {
		klog.Errorf("Unable to list pods on nodes: %v", err)
		return err
	}

	targetCount := k.targetCount(len(nodes))
	klog.V(4).Infof("Target count: %v", targetCount)

	for _, candidate := range candidates {
		if targetCount < 1 {
			break
		}

		podsToEvict, err := k.drainer.GetPodsToEvict(podsOnNodes[candidate.Name], pdbs)
		if err != nil {
			klog.Errorf("Failed to get pods to evict on node %s: %v", candidate.Name, err)
			continue
		}

		result := k.scaler.DeleteNode(candidate, podsToEvict)
		if result.ResultType != status.NodeDeleteOk {
			klog.Errorf("Failed to delete node %s: %v", candidate.Name, result.Err)
			continue
		}

		klog.V(4).Infof("Node %s has been deleted successfully", candidate.Name)
		time.Sleep(k.options.KillDelayAfterDelete)
		targetCount -= 1
	}

	return nil
}

func (k *BasicKiller) targetCount(total int) float64 {
	if k.options.KillAtOnce >= 1 {
		return math.Min(k.options.KillAtOnce, float64(total))
	}

	return float64(total) * k.options.KillAtOnce
}

func (k *BasicKiller) getPodsOnNodes() (map[string][]*apiv1.Pod, error) {
	pods, err := k.listerRegistry.ScheduledPodLister().List()
	if err != nil {
		return nil, fmt.Errorf("unable to list scheduled pods on nodes: %w", err)
	}
	podsOnNodes := map[string][]*apiv1.Pod{}
	for _, p := range pods {
		podsOnNodes[p.Spec.NodeName] = append(podsOnNodes[p.Spec.NodeName], p)
	}
	return podsOnNodes, nil
}

func (k *BasicKiller) ExitCleanUp() {
	nodes, err := k.listerRegistry.ReadyNodeLister().List()
	if err != nil {
		klog.Errorf("Failed to list ready nodes: %v", err)
		return
	}

	k.tainter.CleanAllToBeDeleted(nodes)
}
