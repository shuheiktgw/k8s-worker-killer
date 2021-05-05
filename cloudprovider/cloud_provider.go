package cloudprovider

import (
	apiv1 "k8s.io/api/core/v1"
)

var AvailableCloudProviders = []string{
	AwsProviderName,
}

const (
	// AwsProviderName gets the provider name of aws
	AwsProviderName = "aws"
)

// CloudProvider contains functions for interacting with cloud provider.
type CloudProvider interface {
	// DeleteNode deletes a node for this cloud provider.
	DeleteNode(node *apiv1.Node) error
}
