package common

import (
	"context"
	"fmt"
	"strings"

	"github.com/prometheus/common/model"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement"
	measurementutil "k8s.io/perf-tests/clusterloader2/pkg/measurement/util"
	"k8s.io/perf-tests/clusterloader2/pkg/util"
)

const (
	kruiseLatencyMetricsName = "KruiseLatencyMetrics"
	kruiseMetricsPort        = 8080
	kruiseNamespacae         = "kruise-system"
	kruiseLeaseName          = "kruise-manager"

	kruiseReconcileDurationMetricsName = model.LabelValue("controller_runtime_reconcile_time_seconds_bucket")
	kruiseReconcileTotalMetricsName    = model.LabelValue("controller_runtime_reconcile_total")
)

var kruiseControllerList = []string{
	"NodePodProbe-controller",
	"cloneset-controller",
	"nodeimage-controller",
	"pod-readiness-controller",
	"podunavailablebudget-controller",
	"workloadspread-controller",
	"statefulset-controller",
	"sidecarset-controller",
	"podunavailablebudget-controller",
	"pod-readiness-controller",
	"persistentpodstate-controller",
	"imagepulljob-controller",
	"imagelistpulljob-controller",
	"daemonset-controller",
	"containerrecreaterequest-controller",
	"container-launch-priority-controller",
	"broadcastjob-controller",
	"advancedcronjob-controller",
	"PodProbeMarker-controller",
}

func init() {
	if err := measurement.Register(kruiseLatencyMetricsName, createKruiseLatencyMeasurement); err != nil {
		klog.Fatalf("Cannot register %s: %v", kruiseLatencyMetricsName, err)
	}
}

type kruiseLatencyMeasurement struct {
	initialLatency kruiseLatencyMetrics
}

type kruiseLatencyMetrics struct {
	reconcileDurationHist map[string]*measurementutil.Histogram
	reconcileTotalCount   map[string]int64
}

func createKruiseLatencyMeasurement() measurement.Measurement {
	return &kruiseLatencyMeasurement{}
}

func (k *kruiseLatencyMeasurement) Execute(config *measurement.Config) ([]measurement.Summary, error) {
	action, err := util.GetString(config.Params, "action")
	if err != nil {
		return nil, err
	}

	switch action {
	case "reset":
		klog.V(2).Infof("%s: start collecting latency initial metrics in kruise...", k)
		return nil, k.getKruiseInitialLatency(config.ClusterFramework.GetClientSets().GetClient())
	case "start":
		klog.V(2).Infof("%s: start collecting latency metrics in kruise...", k)
		return nil, k.getKruiseInitialLatency(config.ClusterFramework.GetClientSets().GetClient())
	case "gather":
		klog.V(2).Infof("%s: gathering latency metrics in kruise...", k)
		return k.getKruiseLatency(config.ClusterFramework.GetClientSets().GetClient())
	default:
		return nil, fmt.Errorf("unknown action %v", action)
	}
}

func (*kruiseLatencyMeasurement) Dispose() {}

func (*kruiseLatencyMeasurement) String() string {
	return kruiseLatencyMetricsName
}

func (k *kruiseLatencyMeasurement) getKruiseInitialLatency(c clientset.Interface) error {
	var err error
	k.initialLatency, err = k.getKruiseMetrics(c)
	if err != nil {
		return err
	}
	return nil
}

func (k *kruiseLatencyMeasurement) getKruiseLatency(c clientset.Interface) ([]measurement.Summary, error) {
	kruiseMetrics, err := k.getKruiseMetrics(c)
	if err != nil {
		return nil, err
	}

	kruiseMetrics.substract(k.initialLatency)
	result, err := k.setQuantiles(kruiseMetrics)
	if err != nil {
		return nil, err
	}

	content, err := util.PrettyPrintJSON(result)
	if err != nil {
		return nil, err
	}
	summary := measurement.CreateSummary(schedulerLatencyMetricName, "json", content)
	return []measurement.Summary{summary}, nil
}

func (k *kruiseLatencyMeasurement) setQuantiles(metrics kruiseLatencyMetrics) (kuriseMetrics, error) {
	result := kuriseMetrics{
		Latency: make(map[string]*measurementutil.LatencyMetric),
		Total:   make(map[string]int64),
	}
	for _, controller := range kruiseControllerList {
		result.Latency[controller] = &measurementutil.LatencyMetric{}
		if err := SetQuantileFromHistogram(result.Latency[controller], metrics.reconcileDurationHist[controller]); err != nil {
			return result, err
		}
	}

	result.Total = metrics.reconcileTotalCount
	return result, nil
}

func (k *kruiseLatencyMetrics) substract(sub kruiseLatencyMetrics) {
	for _, controller := range kruiseControllerList {
		if sub.reconcileDurationHist[controller] != nil {
			k.reconcileDurationHist[controller] = HistogramSub(k.reconcileDurationHist[controller], sub.reconcileDurationHist[controller])
		}

		if sub.reconcileTotalCount[controller] != 0 {
			k.reconcileTotalCount[controller] -= sub.reconcileTotalCount[controller]
		}
	}
}

func (k *kruiseLatencyMeasurement) getKruiseMetrics(c clientset.Interface) (kruiseLatencyMetrics, error) {
	reconcileDurationHist := make(map[string]*measurementutil.Histogram)
	reconcileTotalCount := make(map[string]int64)
	for _, controller := range kruiseControllerList {
		reconcileDurationHist[controller] = measurementutil.NewHistogram(nil)
		reconcileTotalCount[controller] = 0
	}
	kruiseMetrics := kruiseLatencyMetrics{
		reconcileDurationHist: reconcileDurationHist,
		reconcileTotalCount:   reconcileTotalCount,
	}
	data, err := k.sendRequestToKuriseController(c, "GET")
	if err != nil {
		return kruiseMetrics, err
	}

	samples, err := measurementutil.ExtractMetricSamples(data)
	if err != nil {
		return kruiseMetrics, err
	}

	for _, sample := range samples {
		switch sample.Metric[model.MetricNameLabel] {
		case kruiseReconcileDurationMetricsName:
			controller := string(sample.Metric["controller"])
			if hist, exists := reconcileDurationHist[controller]; exists {
				measurementutil.ConvertSampleToHistogram(sample, hist)
				klog.V(2).Infof("%s: %s", k, sample)
			}
		case kruiseReconcileTotalMetricsName:
			controller := string(sample.Metric["controller"])
			if _, exists := reconcileTotalCount[controller]; exists {
				reconcileTotalCount[controller] += int64(sample.Value)
				klog.V(2).Infof("%s: %s", k, sample)
			}
		}
	}
	return kruiseMetrics, nil
}

func (k *kruiseLatencyMeasurement) sendRequestToKuriseController(c clientset.Interface, op string) (string, error) {
	opUpper := strings.ToUpper(op)
	if opUpper != "GET" && opUpper != "DELETE" {
		return "", fmt.Errorf("unknown REST request")
	}
	lease, err := c.CoordinationV1().Leases(kruiseNamespacae).Get(context.TODO(), kruiseLeaseName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if lease.Spec.HolderIdentity == nil {
		return "", fmt.Errorf("kruise-manager is not leader")
	}
	holderIdentify := strings.Split(*lease.Spec.HolderIdentity, "_")
	if len(holderIdentify) != 2 {
		return "", fmt.Errorf("invalid kruise-manager lease")
	}
	podName := holderIdentify[0]

	var responseText string
	ctx, cancel := context.WithTimeout(context.Background(), singleRestCallTimeout)
	defer cancel()

	body, err := c.CoreV1().RESTClient().Verb(opUpper).
		Namespace(kruiseNamespacae).
		Resource("pods").
		Name(fmt.Sprintf("%v:%v", podName, kruiseMetricsPort)).
		SubResource("proxy").
		Suffix("metrics").
		Do(ctx).Raw()

	if err != nil {
		klog.Errorf("Send request to scheduler failed with err: %v", err)
		return "", err
	}
	responseText = string(body)
	return responseText, nil
}

type kuriseMetrics struct {
	Latency map[string]*measurementutil.LatencyMetric `json:"reconcileLatency"`
	Total   map[string]int64                          `json:"reconcileTotal"`
}
