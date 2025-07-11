/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package slos

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/perf-tests/clusterloader2/pkg/errors"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement/common"
	measurementutil "k8s.io/perf-tests/clusterloader2/pkg/measurement/util"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement/util/informer"
	"k8s.io/perf-tests/clusterloader2/pkg/util"
)

const (
	defaultResourceCreateLatencyThreshold = 5 * time.Second

	resourceCreateLatencyMeasurementName = "ResourceCreateLatency"
	resourceInformerSyncTimeout          = time.Minute

	ownerCreate = "owner"
	selfCreate  = "self"
	watchTime   = "watch"
)

func init() {
	if err := measurement.Register(resourceCreateLatencyMeasurementName, createResourceCreateLatencyMeasurement); err != nil {
		klog.Fatalf("cant register service %v", err)
	}
}

func createResourceCreateLatencyMeasurement() measurement.Measurement {
	return &resourceCreateLatencyMeasurement{
		selector:        util.NewObjectSelector(),
		resourceEntries: measurementutil.NewObjectTransitionTimes(resourceCreateLatencyMeasurementName),
		eventQueue:      workqueue.New(),
	}
}

type resourceCreateLatencyMeasurement struct {
	selector  *util.ObjectSelector
	gvr       schema.GroupVersionResource
	isRunning bool
	stopCh    chan struct{}
	// This queue can potentially grow indefinitely, beacause we put all changes here.
	// Usually it's not recommended pattern, but we need it for measuring ResourceCreateLatency.
	eventQueue      *workqueue.Type
	resourceEntries *measurementutil.ObjectTransitionTimes
	threshold       time.Duration
	// Threshold for resource create latency by percentile. The default value is threshold.
	perc50Threshold time.Duration
	perc90Threshold time.Duration
	perc99Threshold time.Duration
}

// Execute supports two actions:
// - start - Starts to observe pods and pods events.
// - gather - Gathers and prints current pod latency data.
// Does NOT support concurrency. Multiple calls to this measurement
// shouldn't be done within one step.
func (p *resourceCreateLatencyMeasurement) Execute(config *measurement.Config) ([]measurement.Summary, error) {
	action, err := util.GetString(config.Params, "action")
	if err != nil {
		return nil, err
	}

	switch action {
	case "start":
		if err := p.selector.Parse(config.Params); err != nil {
			return nil, err
		}
		p.threshold, err = util.GetDurationOrDefault(config.Params, "threshold", defaultResourceCreateLatencyThreshold)
		if err != nil {
			return nil, err
		}
		p.perc50Threshold, err = util.GetDurationOrDefault(config.Params, "perc50Threshold", p.threshold)
		if err != nil {
			return nil, err
		}
		p.perc90Threshold, err = util.GetDurationOrDefault(config.Params, "perc90Threshold", p.threshold)
		if err != nil {
			return nil, err
		}
		p.perc99Threshold, err = util.GetDurationOrDefault(config.Params, "perc99Threshold", p.threshold)
		if err != nil {
			return nil, err
		}

		p.gvr, err = common.GetGroupVersionResource(config.Params)
		if err != nil {
			return nil, err
		}

		return nil, p.start(config.ClusterFramework.GetDynamicClients().GetClient())
	case "gather":
		return p.gather(config.Identifier)
	default:
		return nil, fmt.Errorf("unknown action %v", action)
	}

}

// Dispose cleans up after the measurement.
func (p *resourceCreateLatencyMeasurement) Dispose() {
	p.stop()
}

// String returns string representation of this measurement.
func (p *resourceCreateLatencyMeasurement) String() string {
	return resourceCreateLatencyMeasurementName + ": " + p.selector.String()
}

func (p *resourceCreateLatencyMeasurement) start(c dynamic.Interface) error {
	if p.isRunning {
		klog.V(2).Infof("%s: resource create latancy measurement already running", p)
		return nil
	}
	klog.V(2).Infof("%s: starting resource create latency measurement...", p)
	p.isRunning = true
	p.stopCh = make(chan struct{})
	i := informer.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				p.selector.ApplySelectors(&options)
				return c.Resource(p.gvr).List(context.TODO(), options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				p.selector.ApplySelectors(&options)
				return c.Resource(p.gvr).Watch(context.TODO(), options)
			},
		},
		p.addEvent,
	)
	go p.processEvents(c)
	return informer.StartAndSync(i, p.stopCh, informerSyncTimeout)
}

func (p *resourceCreateLatencyMeasurement) addEvent(_, obj interface{}) {
	event := &eventData{obj: obj, recvTime: time.Now()}
	p.eventQueue.Add(event)
}

func (p *resourceCreateLatencyMeasurement) processEvents(c dynamic.Interface) {
	for p.processNextWorkItem(c) {
	}
}

func (p *resourceCreateLatencyMeasurement) processNextWorkItem(c dynamic.Interface) bool {
	item, quit := p.eventQueue.Get()
	if quit {
		klog.Warningf("%s: stopping processing", p)
		return false
	}
	defer p.eventQueue.Done(item)

	event, ok := item.(*eventData)
	if !ok {
		klog.Warningf("Couldn't convert work item to evetData: %v", item)
		return true
	}
	p.processEvent(event, c)
	return true
}

func (p *resourceCreateLatencyMeasurement) stop() {
	if p.isRunning {
		p.isRunning = false
		close(p.stopCh)
		p.eventQueue.ShutDown()
	}
}

var resourceCreateTransitions = map[string]measurementutil.Transition{
	"create": {
		From: ownerCreate,
		To:   selfCreate,
	},
	"create_to_watch": {
		From: selfCreate,
		To:   watchPhase,
	},
	"owner_create_to_watch": {
		From: ownerCreate,
		To:   watchPhase,
	},
}

func resourceCreateTransitionsWithThreshold(threshold time.Duration) map[string]measurementutil.Transition {
	result := make(map[string]measurementutil.Transition)
	for key, value := range resourceCreateTransitions {
		result[key] = value
	}
	resourceCreateTransition := result["create"]
	resourceCreateTransition.Threshold = threshold
	result["create"] = resourceCreateTransition
	return result
}

type resourceCreateLatencyCheck struct {
	namePrefix string
	filter     measurementutil.KeyFilterFunc
}

func (p *resourceCreateLatencyMeasurement) gather(identifier string) ([]measurement.Summary, error) {
	klog.V(2).Infof("%s: gathering resource create latency measurement...", p)
	if !p.isRunning {
		return nil, fmt.Errorf("metric %s has not been started", resourceCreateLatencyMeasurementName)
	}

	p.stop()

	var summaries []measurement.Summary
	var err error

	transitions := resourceCreateTransitionsWithThreshold(p.threshold)
	resourceCreateLatency := p.resourceEntries.CalculateTransitionsLatency(transitions, func(s string) bool { return true })

	if slosErr := resourceCreateLatency["create"].VerifyThresholdByPercentile(p.perc50Threshold, p.perc90Threshold, p.perc99Threshold); slosErr != nil {
		err = errors.NewMetricViolationError("resource create", slosErr.Error())
		klog.Errorf("%s: %v", p, err)
	}

	content, jsonErr := util.PrettyPrintJSON(measurementutil.LatencyMapToPerfData(resourceCreateLatency))
	if jsonErr != nil {
		return nil, jsonErr
	}
	summaryName := fmt.Sprintf("%s_%s", resourceCreateLatencyMeasurementName, identifier)
	summaries = append(summaries, measurement.CreateSummary(summaryName, "json", content))

	return summaries, err
}

func (p *resourceCreateLatencyMeasurement) processEvent(event *eventData, c dynamic.Interface) {
	obj, recvTime := event.obj, event.recvTime
	if obj == nil {
		klog.Warningf("%s: nil object in event", p)
		return
	}
	unstructuredObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		klog.Warningf("%s: could not convert object to meta object: %v", p, obj)
		return
	}
	namespace := unstructuredObj.GetNamespace()
	name := unstructuredObj.GetName()
	createTime := unstructuredObj.GetCreationTimestamp().Time
	key := createMetaNamespaceKey(namespace, name)

	if _, found := p.resourceEntries.Get(key, createPhase); !found {
		p.resourceEntries.Set(key, watchPhase, recvTime)
		p.resourceEntries.Set(key, createPhase, createTime)

		var ownerCreateTime time.Time
		setOwnerCreateTime := func(createTime time.Time) {
			if !ownerCreateTime.IsZero() {
				klog.Warning("Owner create time is already set, overwriting")
			}
			ownerCreateTime = createTime
		}
		for _, ownerRef := range unstructuredObj.GetOwnerReferences() {
			if ownerRef.Controller != nil && *ownerRef.Controller {
				ownerKey := createMetaNamespaceKey(namespace, ownerRef.Name)
				if createTime, found := p.resourceEntries.Get(ownerKey, selfCreate); found {
					setOwnerCreateTime(createTime)
					continue
				}

				gvk := schema.FromAPIVersionAndKind(ownerRef.APIVersion, ownerRef.Kind)
				gvr, _ := meta.UnsafeGuessKindToResource(gvk)

				owner, err := c.Resource(gvr).Namespace(namespace).Get(context.TODO(), ownerRef.Name, metav1.GetOptions{})
				if err != nil {
					klog.Warningf("Failed to get owner object %s/%s: %v", namespace, ownerRef.Name, err)
					continue
				}

				p.resourceEntries.Set(key, ownerCreate, owner.GetCreationTimestamp().Time)
				setOwnerCreateTime(owner.GetCreationTimestamp().Time)
			}
		}
		if ownerCreateTime.IsZero() {
			klog.Warningf("can't not found controller owner for %s/%s", namespace, name)
		} else {
			p.resourceEntries.Set(key, ownerCreate, ownerCreateTime)
		}

	}

}
