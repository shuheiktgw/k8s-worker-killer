package main

import (
	ctx "context"
	"flag"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/shuheiktgw/k8s-worker-killer/cloudprovider"
	"github.com/shuheiktgw/k8s-worker-killer/cloudprovider/builder"
	"github.com/shuheiktgw/k8s-worker-killer/config"
	"github.com/shuheiktgw/k8s-worker-killer/core"
	"github.com/shuheiktgw/k8s-worker-killer/version"

	clientv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	kube_client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	kube_record "k8s.io/client-go/tools/record"
	kube_flag "k8s.io/component-base/cli/flag"
	componentbaseconfig "k8s.io/component-base/config"
	"k8s.io/klog/v2"
)

var (
	cloudProviderFlag = flag.String("cloud-provider", "",
		"Cloud provider type. Available values: ["+strings.Join(cloudprovider.AvailableCloudProviders, ",")+"]")
	killAtOnce           = flag.Float64("kill-at-once", 1, "The number of nodes killed at once.")
	killDelayAfterDelete = flag.Duration("kill-delay-after-delete", 3*time.Minute, "How long after node deletion that kill evaluation resumes.")
	kubernetes           = flag.String("kubernetes", "", "Kubernetes master location. Leave blank for default.")
	kubeConfigFile       = flag.String("kubeconfig", "", "Path to kubeconfig file with authorization and master location information.")
	maxAge               = flag.Duration("max-age", 24*time.Hour, "Maximum age one node can reach.")
	namespace            = flag.String("namespace", "kube-system", "Namespace in which K8s Worker Killer run.")
	scanInterval         = flag.Duration("scan-interval", 10*time.Minute, "How often cluster is reevaluated for killing node.")
)

func createKillingOptions() *config.KillingOptions {
	return &config.KillingOptions{
		KillAtOnce:           *killAtOnce,
		KillDelayAfterDelete: *killDelayAfterDelete,
		MaxAge:               *maxAge,
	}
}

func main() {
	klog.InitFlags(nil)

	leaderElection := defaultLeaderElectionConfiguration()
	leaderElection.LeaderElect = true

	kube_flag.InitFlags()

	klog.V(1).Infof("K8s Worker Killer %s", version.K8sWorkerKillerVersion)

	kubeClient := createKubeClient(getKubeConfig())
	eventRecorder := createEventRecorder(kubeClient)

	if !leaderElection.LeaderElect {
		run(kubeClient, eventRecorder)
	} else {
		id, err := os.Hostname()
		if err != nil {
			klog.Fatalf("Unable to get hostname: %w", err)
		}

		// Validate that the client is ok.
		_, err = kubeClient.CoreV1().Nodes().List(ctx.TODO(), metav1.ListOptions{})
		if err != nil {
			klog.Fatalf("Failed to get nodes from apiserver: %w", err)
		}

		lock, err := resourcelock.New(
			leaderElection.ResourceLock,
			*namespace,
			leaderElection.ResourceName,
			kubeClient.CoreV1(),
			kubeClient.CoordinationV1(),
			resourcelock.ResourceLockConfig{
				Identity:      id,
				EventRecorder: eventRecorder,
			},
		)
		if err != nil {
			klog.Fatalf("Unable to create leader election lock: %w", err)
		}

		leaderelection.RunOrDie(ctx.TODO(), leaderelection.LeaderElectionConfig{
			Lock:          lock,
			LeaseDuration: leaderElection.LeaseDuration.Duration,
			RenewDeadline: leaderElection.RenewDeadline.Duration,
			RetryPeriod:   leaderElection.RetryPeriod.Duration,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(_ ctx.Context) {
					// Since we are committing a suicide after losing
					// mastership, we can safely ignore the argument.
					run(kubeClient, eventRecorder)
				},
				OnStoppedLeading: func() {
					klog.Fatalf("lost master")
				},
			},
		})
	}
}

func run(client kube_client.Interface, recorder kube_record.EventRecorder) {
	killer := buildKiller(client, recorder)

	// Register signal handlers for graceful shutdown.
	registerSignalHandlers(killer)

	for {
		select {
		case <-time.After(*scanInterval):
			{
				err := killer.Run()
				if err != nil {
					klog.Errorf("Failed to run killer: %w", err)
				} else {
					klog.V(1).Info("Killer runs successfully")
				}
			}
		}
	}
}

func defaultLeaderElectionConfiguration() componentbaseconfig.LeaderElectionConfiguration {
	return componentbaseconfig.LeaderElectionConfiguration{
		LeaderElect:   false,
		LeaseDuration: metav1.Duration{Duration: defaultLeaseDuration},
		RenewDeadline: metav1.Duration{Duration: defaultRenewDeadline},
		RetryPeriod:   metav1.Duration{Duration: defaultRetryPeriod},
		ResourceLock:  resourcelock.LeasesResourceLock,
		ResourceName:  "k8s-worker-killer",
	}
}

const (
	defaultLeaseDuration = 15 * time.Second
	defaultRenewDeadline = 10 * time.Second
	defaultRetryPeriod   = 2 * time.Second
)

func getKubeConfig() *rest.Config {
	if *kubeConfigFile != "" {
		klog.V(1).Infof("Using kubeconfig file: %s", *kubeConfigFile)
		// use the current context in kubeconfig
		config, err := clientcmd.BuildConfigFromFlags("", *kubeConfigFile)
		if err != nil {
			klog.Fatalf("Failed to build config: %v", err)
		}
		return config
	}
	url, err := url.Parse(*kubernetes)
	if err != nil {
		klog.Fatalf("Failed to parse Kubernetes url: %v", err)
	}

	kubeConfig, err := config.GetKubeClientConfig(url)
	if err != nil {
		klog.Fatalf("Failed to build Kubernetes client configuration: %v", err)
	}

	return kubeConfig
}

func createKubeClient(kubeConfig *rest.Config) kube_client.Interface {
	return kube_client.NewForConfigOrDie(kubeConfig)
}

func registerSignalHandlers(killer core.Killer) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGQUIT)
	klog.V(1).Info("Registered cleanup signal handler")

	go func() {
		<-sigs
		klog.V(1).Info("Received signal, attempting cleanup")
		killer.ExitCleanUp()
		klog.V(1).Info("Cleaned up, exiting...")
		klog.Flush()
		os.Exit(0)
	}()
}

func createEventRecorder(kubeClient kube_client.Interface) kube_record.EventRecorder {
	eventBroadcaster := kube_record.NewBroadcasterWithCorrelatorOptions(getCorrelationOptions())
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})
	return eventBroadcaster.NewRecorder(scheme.Scheme, clientv1.EventSource{Component: "k8s-node-killer"})
}

func getCorrelationOptions() kube_record.CorrelatorOptions {
	return kube_record.CorrelatorOptions{
		QPS:          defaultQPS,
		BurstSize:    defaultBurstSize,
		LRUCacheSize: defaultLRUCache,
	}
}

const (
	defaultQPS       = 1. / 300.
	defaultBurstSize = 1
	defaultLRUCache  = 8192
)

func buildKiller(client kube_client.Interface, recorder kube_record.EventRecorder) core.Killer {
	options := createKillingOptions()

	stopChannel := make(chan struct{})
	listerRegistry := kube_util.NewListerRegistryWithDefaultListers(client, stopChannel)
	provider, err := builder.NewCloudProvider(*cloudProviderFlag)
	if err != nil {
		klog.Fatalf("Failed to initialize cloud provider: %w", err)
	}
	drainer := core.NewDrainerImpl(listerRegistry)
	tainter := core.NewTainterImpl(client, recorder)
	scaler := core.NewScalerImpl(client, provider, recorder, tainter)

	return core.NewBasicKiller(listerRegistry, options, drainer, scaler, tainter)
}
