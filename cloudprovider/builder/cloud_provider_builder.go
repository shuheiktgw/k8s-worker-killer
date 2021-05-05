package builder

import (
	"fmt"

	"github.com/shuheiktgw/k8s-worker-killer/cloudprovider"
	"github.com/shuheiktgw/k8s-worker-killer/cloudprovider/aws"

	"k8s.io/klog/v2"
)

// NewCloudProvider builds a cloud provider from provided cloud provider name.
func NewCloudProvider(cloudProviderName string) (cloudprovider.CloudProvider, error) {
	klog.V(1).Infof("Building %s cloud provider.", cloudProviderName)

	if cloudProviderName == "" {
		return nil, fmt.Errorf("cloud provider name cannot be empty")
	}

	provider, err := buildCloudProvider(cloudProviderName)
	if err != nil {
		return nil, err
	}

	return provider, nil
}

func buildCloudProvider(cloudProviderName string) (cloudprovider.CloudProvider, error) {
	switch cloudProviderName {
	case cloudprovider.AwsProviderName:
		return aws.Build()
	default:
		return nil, fmt.Errorf("unknown cloud provider: %s", cloudProviderName)
	}
}
