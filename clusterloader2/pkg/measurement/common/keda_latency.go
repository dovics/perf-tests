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
	kedaLatencyMetricsName = "KedaLatencyMetrics"
	kedaMetricsPort        = 8080
	kedaNamespacae         = "keda"
	kedaLeaseName          = "operator.keda.sh"

	kedaReconcileDurationMetricsName = model.LabelValue("controller_runtime_reconcile_time_seconds_bucket")
	kedaReconcileTotalMetricsName    = model.LabelValue("controller_runtime_reconcile_total")

	kedaInternalMetricsServiceGrpcServerHandlingMetricsName = model.LabelValue("keda_internal_metricsservice_grpc_server_handling_seconds_bucket")
	kedaWorkerQueueDurationMetricsName                      = model.LabelValue("workqueue_queue_duration_seconds_bucket")
)

var kedaControllerList = []string{
	"cert-ratotor",
	"cloudeventsource",
	"clustercloudeventsource",
	"clustertriggerauthentication",
	"scaledjob",
	"scaledobject",
	"triggerauthentication",
}

func init() {
	if err := measurement.Register(kedaLatencyMetricsName, createKedaLatencyMeasurement); err != nil {
		klog.Fatalf("Cannot register %s: %v", kedaLatencyMetricsName, err)
	}
}

type kedaLatencyMeasurement struct {
	initialLatency kedaLatencyMetrics
}

type kedaLatencyMetrics struct {
	reconcileDurationHist map[string]*measurementutil.Histogram
	reconcileTotalCount   map[string]int64

	internalMetricsServiceHandling *measurementutil.Histogram
	workerQueueDuration            map[string]*measurementutil.Histogram
}

func createKedaLatencyMeasurement() measurement.Measurement {
	return &kedaLatencyMeasurement{}
}

func (k *kedaLatencyMeasurement) Execute(config *measurement.Config) ([]measurement.Summary, error) {
	action, err := util.GetString(config.Params, "action")
	if err != nil {
		return nil, err
	}

	switch action {
	case "reset":
		klog.V(2).Infof("%s: start collecting latency initial metrics in keda...", k)
		return nil, k.getKedaInitialLatency(config.ClusterFramework.GetClientSets().GetClient())
	case "start":
		klog.V(2).Infof("%s: start collecting latency metrics in keda...", k)
		return nil, k.getKedaInitialLatency(config.ClusterFramework.GetClientSets().GetClient())
	case "gather":
		klog.V(2).Infof("%s: gathering latency metrics in keda...", k)
		return k.getKedaLatency(config.ClusterFramework.GetClientSets().GetClient())
	default:
		return nil, fmt.Errorf("unknown action %v", action)
	}
}

func (*kedaLatencyMeasurement) Dispose() {}

func (*kedaLatencyMeasurement) String() string {
	return kedaLatencyMetricsName
}

func (k *kedaLatencyMeasurement) getKedaInitialLatency(c clientset.Interface) error {
	var err error
	k.initialLatency, err = k.getKedaMetrics(c)
	if err != nil {
		return err
	}
	return nil
}

func (k *kedaLatencyMeasurement) getKedaLatency(c clientset.Interface) ([]measurement.Summary, error) {
	kedaMetrics, err := k.getKedaMetrics(c)
	if err != nil {
		return nil, err
	}

	kedaMetrics.substract(k.initialLatency)
	result, err := k.setQuantiles(kedaMetrics)
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

func (k *kedaLatencyMeasurement) setQuantiles(metrics kedaLatencyMetrics) (kedaMetrics, error) {
	result := kedaMetrics{
		Latency:             make(map[string]*measurementutil.LatencyMetric),
		Total:               make(map[string]int64),
		WorkerQueueDuration: make(map[string]*measurementutil.LatencyMetric),
	}
	for _, controller := range kedaControllerList {
		result.Latency[controller] = &measurementutil.LatencyMetric{}
		if err := SetQuantileFromHistogram(result.Latency[controller], metrics.reconcileDurationHist[controller]); err != nil {
			return result, err
		}

		result.WorkerQueueDuration[controller] = &measurementutil.LatencyMetric{}
		if err := SetQuantileFromHistogram(result.WorkerQueueDuration[controller], metrics.workerQueueDuration[controller]); err != nil {
			return result, err
		}
	}
	result.Total = metrics.reconcileTotalCount

	result.InternalMetricsServiceHandling = &measurementutil.LatencyMetric{}
	if err := SetQuantileFromHistogram(result.InternalMetricsServiceHandling, metrics.internalMetricsServiceHandling); err != nil {
		return result, err
	}
	return result, nil
}

func (k *kedaLatencyMetrics) substract(sub kedaLatencyMetrics) {
	for _, controller := range kedaControllerList {
		if sub.reconcileDurationHist[controller] != nil {
			k.reconcileDurationHist[controller] = HistogramSub(k.reconcileDurationHist[controller], sub.reconcileDurationHist[controller])
		}

		if sub.reconcileTotalCount[controller] != 0 {
			k.reconcileTotalCount[controller] -= sub.reconcileTotalCount[controller]
		}

		if sub.workerQueueDuration[controller] != nil {
			k.workerQueueDuration[controller] = HistogramSub(k.workerQueueDuration[controller], sub.workerQueueDuration[controller])
		}
	}

	k.internalMetricsServiceHandling = HistogramSub(k.internalMetricsServiceHandling, sub.internalMetricsServiceHandling)
}

func (k *kedaLatencyMeasurement) getKedaMetrics(c clientset.Interface) (kedaLatencyMetrics, error) {
	reconcileDurationHist := make(map[string]*measurementutil.Histogram)
	reconcileTotalCount := make(map[string]int64)
	workerQueueDuration := make(map[string]*measurementutil.Histogram)
	for _, controller := range kedaControllerList {
		workerQueueDuration[controller] = measurementutil.NewHistogram(nil)
		reconcileDurationHist[controller] = measurementutil.NewHistogram(nil)
		reconcileTotalCount[controller] = 0
	}

	internalMetricsServiceHandlingHist := measurementutil.NewHistogram(nil)
	kedaMetrics := kedaLatencyMetrics{
		reconcileDurationHist:          reconcileDurationHist,
		reconcileTotalCount:            reconcileTotalCount,
		workerQueueDuration:            workerQueueDuration,
		internalMetricsServiceHandling: internalMetricsServiceHandlingHist,
	}
	data, err := k.sendRequestToKuriseController(c, "GET")
	if err != nil {
		return kedaMetrics, err
	}

	samples, err := measurementutil.ExtractMetricSamples(data)
	if err != nil {
		return kedaMetrics, err
	}

	for _, sample := range samples {
		switch sample.Metric[model.MetricNameLabel] {
		case kedaReconcileDurationMetricsName:
			controller := string(sample.Metric["controller"])
			if hist, exists := reconcileDurationHist[controller]; exists {
				measurementutil.ConvertSampleToHistogram(sample, hist)
				klog.V(2).Infof("%s: %s", k, sample)
			}
		case kedaReconcileTotalMetricsName:
			controller := string(sample.Metric["controller"])
			if _, exists := reconcileTotalCount[controller]; exists {
				reconcileTotalCount[controller] += int64(sample.Value)
				klog.V(2).Infof("%s: %s", k, sample)
			}
		case kedaInternalMetricsServiceGrpcServerHandlingMetricsName:
			measurementutil.ConvertSampleToHistogram(sample, internalMetricsServiceHandlingHist)
			klog.V(2).Infof("%s: %s", k, sample)
		case kedaWorkerQueueDurationMetricsName:
			controller := string(sample.Metric["controller"])
			if hist, exists := workerQueueDuration[controller]; exists {
				measurementutil.ConvertSampleToHistogram(sample, hist)
				klog.V(2).Infof("%s: %s", k, sample)
			}
		}
	}
	return kedaMetrics, nil
}

func (k *kedaLatencyMeasurement) sendRequestToKuriseController(c clientset.Interface, op string) (string, error) {
	opUpper := strings.ToUpper(op)
	if opUpper != "GET" && opUpper != "DELETE" {
		return "", fmt.Errorf("unknown REST request")
	}
	lease, err := c.CoordinationV1().Leases(kedaNamespacae).Get(context.TODO(), kedaLeaseName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if lease.Spec.HolderIdentity == nil {
		return "", fmt.Errorf("keda-manager is not leader")
	}
	holderIdentify := strings.Split(*lease.Spec.HolderIdentity, "_")
	if len(holderIdentify) != 2 {
		return "", fmt.Errorf("invalid keda-manager lease")
	}
	podName := holderIdentify[0]

	var responseText string
	ctx, cancel := context.WithTimeout(context.Background(), singleRestCallTimeout)
	defer cancel()

	body, err := c.CoreV1().RESTClient().Verb(opUpper).
		Namespace(kedaNamespacae).
		Resource("pods").
		Name(fmt.Sprintf("%v:%v", podName, kedaMetricsPort)).
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

type kedaMetrics struct {
	Latency                        map[string]*measurementutil.LatencyMetric `json:"reconcileLatency"`
	Total                          map[string]int64                          `json:"reconcileTotal"`
	WorkerQueueDuration            map[string]*measurementutil.LatencyMetric `json:"workerQueueDuration"`
	InternalMetricsServiceHandling *measurementutil.LatencyMetric            `json:"internalMetricsServiceHandling"`
}
