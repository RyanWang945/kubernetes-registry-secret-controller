package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const bytesPerMiB = 1024 * 1024

type resourcePeak struct {
	CPUMillicores float64 `json:"cpuMillicores"`
	MemoryMiB     float64 `json:"memoryMiB"`
}

type resourceSummary struct {
	Samples        int                     `json:"samples"`
	Errors         int                     `json:"errors"`
	ControllerPeak resourcePeak            `json:"controllerPeak"`
	NodePeaks      map[string]resourcePeak `json:"nodePeaks"`
}

type resourceRecorder struct {
	file    *os.File
	writer  *csv.Writer
	summary resourceSummary
}

type metricsList struct {
	Items []metricsItem `json:"items"`
}

type metricsItem struct {
	Metadata   metav1.ObjectMeta   `json:"metadata"`
	Usage      corev1.ResourceList `json:"usage"`
	Containers []struct {
		Name  string              `json:"name"`
		Usage corev1.ResourceList `json:"usage"`
	} `json:"containers"`
}

func newResourceRecorder(path string) (*resourceRecorder, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create resource CSV: %w", err)
	}
	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"timestamp",
		"phase",
		"scope",
		"name",
		"cpu_millicores",
		"memory_mib",
	}); err != nil {
		file.Close()
		return nil, err
	}
	writer.Flush()
	return &resourceRecorder{
		file:   file,
		writer: writer,
		summary: resourceSummary{
			NodePeaks: make(map[string]resourcePeak),
		},
	}, nil
}

func (r *resourceRecorder) Sample(ctx context.Context, clientset kubernetes.Interface, phase phaseKind) {
	timestamp := time.Now().UTC()
	pods, podErr := readMetrics(
		ctx,
		clientset,
		"/apis/metrics.k8s.io/v1beta1/namespaces/"+controllerNamespace+"/pods",
	)
	nodes, nodeErr := readMetrics(ctx, clientset, "/apis/metrics.k8s.io/v1beta1/nodes")
	if podErr != nil || nodeErr != nil {
		r.summary.Errors++
		return
	}

	for _, pod := range pods.Items {
		if pod.Metadata.Labels["app.kubernetes.io/name"] != controllerApplication {
			continue
		}
		usage := corev1.ResourceList{}
		for _, container := range pod.Containers {
			addUsage(usage, container.Usage)
		}
		peak := usageValues(usage)
		r.summary.ControllerPeak = maximumPeak(r.summary.ControllerPeak, peak)
		r.write(timestamp, phase, "controller", pod.Metadata.Name, peak)
	}
	for _, node := range nodes.Items {
		peak := usageValues(node.Usage)
		r.summary.NodePeaks[node.Metadata.Name] = maximumPeak(r.summary.NodePeaks[node.Metadata.Name], peak)
		r.write(timestamp, phase, "node", node.Metadata.Name, peak)
	}
	r.summary.Samples++
	r.writer.Flush()
}

func readMetrics(ctx context.Context, clientset kubernetes.Interface, path string) (metricsList, error) {
	raw, err := clientset.CoreV1().RESTClient().Get().AbsPath(path).Do(ctx).Raw()
	if err != nil {
		return metricsList{}, err
	}
	var result metricsList
	if err := json.Unmarshal(raw, &result); err != nil {
		return metricsList{}, err
	}
	return result, nil
}

func addUsage(total corev1.ResourceList, usage corev1.ResourceList) {
	for name, quantity := range usage {
		current := total[name]
		current.Add(quantity)
		total[name] = current
	}
}

func usageValues(usage corev1.ResourceList) resourcePeak {
	cpu := usage[corev1.ResourceCPU]
	memory := usage[corev1.ResourceMemory]
	return resourcePeak{
		CPUMillicores: float64(cpu.MilliValue()),
		MemoryMiB:     float64(memory.Value()) / bytesPerMiB,
	}
}

func maximumPeak(left, right resourcePeak) resourcePeak {
	return resourcePeak{
		CPUMillicores: max(left.CPUMillicores, right.CPUMillicores),
		MemoryMiB:     max(left.MemoryMiB, right.MemoryMiB),
	}
}

func (r *resourceRecorder) write(
	timestamp time.Time,
	phase phaseKind,
	scope string,
	name string,
	peak resourcePeak,
) {
	_ = r.writer.Write([]string{
		timestamp.Format(time.RFC3339Nano),
		string(phase),
		scope,
		name,
		strconv.FormatFloat(peak.CPUMillicores, 'f', 3, 64),
		strconv.FormatFloat(peak.MemoryMiB, 'f', 3, 64),
	})
}

func (r *resourceRecorder) Summary() resourceSummary {
	return r.summary
}

func (r *resourceRecorder) Close() error {
	r.writer.Flush()
	writerErr := r.writer.Error()
	fileErr := r.file.Close()
	if writerErr != nil {
		return writerErr
	}
	return fileErr
}
