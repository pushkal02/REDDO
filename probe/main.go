package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

//go:embed static/*
var staticFS embed.FS

type PodTelemetry struct {
	Name     string `json:"name"`
	Status   string `json:"status"`
	Restarts int    `json:"restarts"`
	CPU      string `json:"cpu"`
	Memory   string `json:"memory"`
	Age      string `json:"age"`
}

type TelemetryPayload struct {
	Pods     []PodTelemetry `json:"pods"`
	RabbitMQ struct {
		JavaQueueDepth int `json:"java_queue_depth"`
		NodeQueueDepth int `json:"node_queue_depth"`
	} `json:"rabbitmq"`
	Timestamp string `json:"timestamp"`
}

var (
	k8sClient    *http.Client
	k8sLogClient *http.Client
	k8sToken     string
	rabbitmqURL  = "http://guest:guest@localhost:15672"
)

func main() {
	if url := os.Getenv("RABBITMQ_URL"); url != "" {
		rabbitmqURL = url
	}
	port := "8082"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}

	initK8sClient()

	mux := http.NewServeMux()
	
	// Server-Sent Events streams
	mux.HandleFunc("GET /api/v1/telemetry/stream", handleSSEStream)
	mux.HandleFunc("GET /api/v1/logs/stream", handleLogsStream)
	
	// Serve embedded React SPA files
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/" {
			path = "/index.html"
		}
		
		filePath := "static" + path
		fileBytes, err := staticFS.ReadFile(filePath)
		if err != nil {
			// Fallback to index.html for React SPA Router support
			fileBytes, err = staticFS.ReadFile("static/index.html")
			if err != nil {
				http.Error(w, "Not Found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			w.Write(fileBytes)
			return
		}

		// Set explicit MIME content types
		if strings.HasSuffix(path, ".html") {
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		} else if strings.HasSuffix(path, ".css") {
			w.Header().Set("Content-Type", "text/css")
		} else if strings.HasSuffix(path, ".js") {
			w.Header().Set("Content-Type", "application/javascript")
		} else if strings.HasSuffix(path, ".svg") {
			w.Header().Set("Content-Type", "image/svg+xml")
		}
		
		w.Write(fileBytes)
	})

	log.Printf("[Telemetry Observer] Server starting on port %s...", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func initK8sClient() {
	tokenPath := "/var/run/secrets/kubernetes.io/serviceaccount/token"
	caPath := "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		log.Printf("[Warning] K8s ServiceAccount token not found. Running in localized fallback mode: %v", err)
		k8sClient = http.DefaultClient
		k8sLogClient = http.DefaultClient
		return
	}
	k8sToken = strings.TrimSpace(string(tokenBytes))

	caCert, err := os.ReadFile(caPath)
	if err != nil {
		log.Printf("[Warning] K8s CA cert not found. Fallback to insecure client: %v", err)
		k8sClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: 3 * time.Second,
		}
		k8sLogClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: 0, // No timeout for streaming pod log streams!
		}
		return
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	k8sClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: caCertPool,
			},
		},
		Timeout: 3 * time.Second,
	}

	k8sLogClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: caCertPool,
			},
		},
		Timeout: 0, // No timeout for streaming pod log streams!
	}
	log.Println("[Telemetry Observer] Kubernetes API Clients (polling + streaming) initialized successfully.")
}

func handleSSEStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	log.Printf("[SSE] Client connected from %s", r.RemoteAddr)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			log.Printf("[SSE] Client disconnected: %s", r.RemoteAddr)
			return
		case <-ticker.C:
			payload := scrapeTelemetry()
			dataBytes, err := json.Marshal(payload)
			if err != nil {
				continue
			}

			fmt.Fprintf(w, "data: %s\n\n", string(dataBytes))
			flusher.Flush()
		}
	}
}

func handleLogsStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	log.Printf("[Logs SSE] Client connected from %s", r.RemoteAddr)

	logChan := make(chan string, 500)
	done := make(chan struct{})
	defer close(done)

	var mu sync.Mutex
	tailingPods := make(map[string]bool)

	discoverPods := func() {
		if k8sToken == "" {
			mockPayload, _ := json.Marshal(map[string]string{
				"pod":     "gate-mock-12345",
				"service": "gate",
				"message": fmt.Sprintf("2026-07-07T00:00:00Z [INFO ] [gate] - Mock gateway log tick %d", time.Now().Unix()),
			})
			select {
			case logChan <- string(mockPayload):
			case <-done:
				return
			default:
			}
			return
		}

		podsURL := "https://kubernetes.default.svc/api/v1/namespaces/reddo/pods"
		req, err := http.NewRequest("GET", podsURL, nil)
		if err != nil {
			return
		}
		req.Header.Set("Authorization", "Bearer "+k8sToken)
		resp, err := k8sClient.Do(req)
		if err != nil {
			return
		}

		var podList struct {
			Items []struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
				Status struct {
					Phase string `json:"phase"`
				} `json:"status"`
			} `json:"items"`
		}

		if json.NewDecoder(resp.Body).Decode(&podList) == nil {
			for _, item := range podList.Items {
				podName := item.Metadata.Name
				if item.Status.Phase == "Running" {
					mu.Lock()
					alreadyTailing := tailingPods[podName]
					if !alreadyTailing {
						tailingPods[podName] = true
						go func(name string) {
							defer func() {
								mu.Lock()
								delete(tailingPods, name)
								mu.Unlock()
							}()
							tailPodLogs(name, logChan, done)
						}(podName)
					}
					mu.Unlock()
				}
			}
		}
		resp.Body.Close()
	}

	// Run initial immediate discovery scan
	go discoverPods()

	// Dynamic pod discovery loop ticker
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				discoverPods()
			}
		}
	}()

	for {
		select {
		case <-r.Context().Done():
			log.Printf("[Logs SSE] Client disconnected: %s", r.RemoteAddr)
			return
		case msg := <-logChan:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

func tailPodLogs(podName string, logChan chan string, done chan struct{}) {
	url := fmt.Sprintf("https://kubernetes.default.svc/api/v1/namespaces/reddo/pods/%s/log?follow=true&tailLines=50", podName)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+k8sToken)

	// Use k8sLogClient which has NO TIMEOUT!
	resp, err := k8sLogClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	serviceName := "infra"
	if strings.HasPrefix(podName, "gate") {
		serviceName = "gate"
	} else if strings.HasPrefix(podName, "broker") {
		serviceName = "broker"
	} else if strings.HasPrefix(podName, "worker") {
		serviceName = "worker"
	} else if strings.HasPrefix(podName, "cannon") {
		serviceName = "cannon"
	} else if strings.HasPrefix(podName, "probe") {
		serviceName = "probe"
	} else if strings.HasPrefix(podName, "rabbitmq") {
		serviceName = "rabbitmq"
	} else if strings.HasPrefix(podName, "postgres") {
		serviceName = "postgres"
	} else if strings.HasPrefix(podName, "redis") {
		serviceName = "redis"
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		payload, err := json.Marshal(map[string]string{
			"pod":     podName,
			"service": serviceName,
			"message": line,
		})
		if err != nil {
			continue
		}
		select {
		case logChan <- string(payload):
		case <-done:
			return
		}
	}
}

func scrapeTelemetry() TelemetryPayload {
	payload := TelemetryPayload{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	payload.RabbitMQ.JavaQueueDepth = getRabbitMQQueueDepth("java-tasks")
	payload.RabbitMQ.NodeQueueDepth = getRabbitMQQueueDepth("node-tasks")

	if k8sToken == "" {
		payload.Pods = []PodTelemetry{
			{Name: "gate-mock", Status: "Running", Restarts: 0, CPU: "12m", Memory: "32Mi", Age: "1h"},
			{Name: "broker-mock", Status: "Running", Restarts: 0, CPU: "85m", Memory: "180Mi", Age: "1h"},
			{Name: "worker-mock", Status: "Running", Restarts: 0, CPU: "5m", Memory: "25Mi", Age: "1h"},
		}
		return payload
	}

	podsMap := make(map[string]*PodTelemetry)

	podsURL := "https://kubernetes.default.svc/api/v1/namespaces/reddo/pods"
	req, _ := http.NewRequest("GET", podsURL, nil)
	req.Header.Set("Authorization", "Bearer "+k8sToken)
	resp, err := k8sClient.Do(req)
	if err != nil {
		return payload
	}

	var podList struct {
		Items []struct {
			Metadata struct {
				Name              string    `json:"name"`
				CreationTimestamp time.Time `json:"creationTimestamp"`
			} `json:"metadata"`
			Status struct {
				Phase             string `json:"phase"`
				ContainerStatuses []struct {
					RestartCount int `json:"restartCount"`
					State        struct {
						Waiting *struct {
							Reason string `json:"reason"`
						} `json:"waiting"`
					} `json:"state"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}

	if json.NewDecoder(resp.Body).Decode(&podList) == nil {
		for _, item := range podList.Items {
			restarts := 0
			status := item.Status.Phase
			
			for _, cs := range item.Status.ContainerStatuses {
				restarts += cs.RestartCount
				if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
					status = cs.State.Waiting.Reason
				}
			}

			age := time.Since(item.Metadata.CreationTimestamp).Round(time.Second).String()
			podsMap[item.Metadata.Name] = &PodTelemetry{
				Name:     item.Metadata.Name,
				Status:   status,
				Restarts: restarts,
				CPU:      "0m",
				Memory:   "0Mi",
				Age:      age,
			}
		}
	}
	resp.Body.Close()

	metricsURL := "https://kubernetes.default.svc/apis/metrics.k8s.io/v1beta1/namespaces/reddo/pods"
	req, _ = http.NewRequest("GET", metricsURL, nil)
	req.Header.Set("Authorization", "Bearer "+k8sToken)
	resp, err = k8sClient.Do(req)
	if err == nil {
		var metricsList struct {
			Items []struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
				Containers []struct {
					Usage struct {
						CPU    string `json:"cpu"`
						Memory string `json:"memory"`
					} `json:"usage"`
				} `json:"containers"`
			} `json:"items"`
		}

		if json.NewDecoder(resp.Body).Decode(&metricsList) == nil {
			for _, item := range metricsList.Items {
				if p, exists := podsMap[item.Metadata.Name]; exists {
					var totalCPU int64
					var totalMem int64
					for _, c := range item.Containers {
						totalCPU += parseCPU(c.Usage.CPU)
						totalMem += parseMem(c.Usage.Memory)
					}
					p.CPU = fmt.Sprintf("%dm", totalCPU)
					p.Memory = fmt.Sprintf("%dMi", totalMem)
				}
			}
		}
		resp.Body.Close()
	}

	for _, p := range podsMap {
		payload.Pods = append(payload.Pods, *p)
	}

	return payload
}

func getRabbitMQQueueDepth(queueName string) int {
	url := fmt.Sprintf("%s/api/queues/%%2F/%s", rabbitmqURL, queueName)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0
	}

	var username, password string
	if strings.Contains(rabbitmqURL, "://") {
		parts := strings.Split(rabbitmqURL, "://")
		if len(parts) > 1 && strings.Contains(parts[1], "@") {
			authParts := strings.Split(parts[1], "@")[0]
			creds := strings.Split(authParts, ":")
			if len(creds) > 1 {
				username = creds[0]
				password = creds[1]
			}
		}
	}

	if username != "" {
		auth := username + ":" + password
		req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(auth)))
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0
	}

	var data struct {
		Messages int `json:"messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return 0
	}

	return data.Messages
}

func parseCPU(cpuStr string) int64 {
	if strings.HasSuffix(cpuStr, "n") {
		var nanocores int64
		fmt.Sscanf(cpuStr, "%dn", &nanocores)
		return nanocores / 1000000
	}
	if strings.HasSuffix(cpuStr, "u") {
		var microcores int64
		fmt.Sscanf(cpuStr, "%du", &microcores)
		return microcores / 1000
	}
	return 0
}

func parseMem(memStr string) int64 {
	if strings.HasSuffix(memStr, "Ki") {
		var ki int64
		fmt.Sscanf(memStr, "%dKi", &ki)
		return ki / 1024
	}
	if strings.HasSuffix(memStr, "Mi") {
		var mi int64
		fmt.Sscanf(memStr, "%dMi", &mi)
		return mi
	}
	return 0
}
