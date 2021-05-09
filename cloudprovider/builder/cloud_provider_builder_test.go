package builder_test

import (
	"testing"

	"github.com/shuheiktgw/k8s-worker-killer/cloudprovider/builder"
	"github.com/stretchr/testify/assert"
)

func TestNewCloudProvider(t *testing.T) {
	tests := []struct {
		name     string
		provider string
		wantErr  bool
	}{
		{
			name:     "AWS provider",
			provider: "aws",
			wantErr:  false,
		},
		{
			name:     "Unknown provider",
			provider: "unknown",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, err := builder.NewCloudProvider(tt.provider)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("error occurred while initializing cloud provider")
				}
				return
			}

			assert.NotNil(t, provider)
		})
	}
}
