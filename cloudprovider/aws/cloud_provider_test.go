package aws_test

import (
	"errors"
	"testing"

	"github.com/shuheiktgw/k8s-worker-killer/cloudprovider/aws"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
)

func TestBuild(t *testing.T) {
	provider, err := aws.Build()
	if err != nil {
		t.Errorf("error occurred while initializing cloud provider")
		return
	}

	if provider == nil {
		t.Error("provider is not supposed to be empty")
	}
}

type ec2ServiceMock struct {
	MockTerminateInstances          func(ids []string) error
	MockWaitUntilInstanceTerminated func(ids []string) error
}

func (e *ec2ServiceMock) TerminateInstances(ids []string) error {
	return e.MockTerminateInstances(ids)
}

func (e *ec2ServiceMock) WaitUntilInstanceTerminated(ids []string) error {
	return e.MockWaitUntilInstanceTerminated(ids)
}

func TestCloudProviderDeleteNode(t *testing.T) {
	tests := []struct {
		name       string
		ec2Service *ec2ServiceMock
		node       *apiv1.Node
		errString  string
	}{
		{
			name:       "invalid provider id",
			ec2Service: &ec2ServiceMock{},
			node: &apiv1.Node{
				Spec: apiv1.NodeSpec{ProviderID: "invalid"},
			},
			errString: "failed to retrieve instance id from node : unexpected provider id: invalid",
		},
		{
			name:       "instance termination failure",
			ec2Service: &ec2ServiceMock{
				MockTerminateInstances: func(ids []string) error {
					return errors.New("termination error")
				},
			},
			node: &apiv1.Node{
				Spec: apiv1.NodeSpec{ProviderID: "aws:///us-east-1/test"},
			},
			errString: "failed to terminate instance test: termination error",
		},
		{
			name:       "instance termination wait failure",
			ec2Service: &ec2ServiceMock{
				MockTerminateInstances: func(ids []string) error {
					return nil
				},
				MockWaitUntilInstanceTerminated: func(ids []string) error {
					return errors.New("wait error")
				},
			},
			node: &apiv1.Node{
				Spec: apiv1.NodeSpec{ProviderID: "aws:///us-east-1/test"},
			},
			errString: "failed to wait instance termination test: wait error",
		},
		{
			name:       "success",
			ec2Service: &ec2ServiceMock{
				MockTerminateInstances: func(ids []string) error {
					return nil
				},
				MockWaitUntilInstanceTerminated: func(ids []string) error {
					return nil
				},
			},
			node: &apiv1.Node{
				Spec: apiv1.NodeSpec{ProviderID: "aws:///us-east-1/test"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := aws.CloudProvider{Ec2Service: tt.ec2Service}
			err := provider.DeleteNode(tt.node)

			if len(tt.errString) != 0 {
				assert.EqualError(t, err, tt.errString)
				return
			}
		})
	}
}
