package aws

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/shuheiktgw/k8s-worker-killer/cloudprovider"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

func Build() (cloudprovider.CloudProvider, error) {
	sess, err := session.NewSession(aws.NewConfig().WithRegion(getRegion()))
	if err != nil {
		return nil, fmt.Errorf("failed to create new session: %w", err)
	}

	return &CloudProvider{Ec2Service: &ec2Wrapper{ec2: ec2.New(sess)}}, nil
}

type CloudProvider struct {
	Ec2Service ec2Service
}

type ec2Service interface {
	TerminateInstances([]string) error
	WaitUntilInstanceTerminated([]string) error
}

type ec2Wrapper struct {
	ec2 *ec2.EC2
}

func (e *ec2Wrapper) TerminateInstances(ids []string) error {
	_, err := e.ec2.TerminateInstances(&ec2.TerminateInstancesInput{
		InstanceIds: aws.StringSlice(ids),
	})

	return err
}

func (e *ec2Wrapper) WaitUntilInstanceTerminated(ids []string) error {
	return e.ec2.WaitUntilInstanceTerminated(&ec2.DescribeInstancesInput{
		InstanceIds: aws.StringSlice(ids),
	})
}

func (c *CloudProvider) DeleteNode(node *apiv1.Node) error {
	id, err := getInstanceId(node)
	if err != nil {
		return fmt.Errorf("failed to retrieve instance id from node %s: %w", node.Name, err)
	}

	klog.V(1).Infof("Terminating ec2 instance %s", id)

	err = c.Ec2Service.TerminateInstances([]string{id})
	if err != nil {
		return fmt.Errorf("failed to terminate instance %s: %w", id, err)
	}

	klog.V(1).Infof("Waiting for ec2 instance %s termination", id)

	err = c.Ec2Service.WaitUntilInstanceTerminated([]string{id})
	if err != nil {
		return fmt.Errorf("failed to wait instance termination %s: %w", id, err)
	}

	return nil
}

func getInstanceId(node *apiv1.Node) (string, error) {
	// ProviderID looks like aws:///us-west-2a/i-0bf293a346d957e06
	providerID := node.Spec.ProviderID

	trimmed := strings.TrimPrefix(providerID, "aws:///")
	split := strings.Split(trimmed, "/")

	if len(split) != 2 {
		return "", fmt.Errorf("unexpected provider id: %s", providerID)
	}

	return split[1], nil
}

func getRegion() string {
	sess, err := session.NewSession()
	if err != nil {
		klog.Errorf("Error getting AWS session while retrieving region: %v", err)
		return ""
	}

	svc := ec2metadata.New(sess)
	region, err := svc.Region()
	if err != nil {
		klog.Errorf("Error getting ec2 metadata: %v", err)
		return ""
	}

	return region
}
