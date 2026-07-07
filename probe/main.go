package main

import (
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
	"time"
)

//go:embed static/*
var staticFS embed.FS

type PodTelemetry struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	Restarts  int    `json:"restarts"`
	CPU       string `json:"cpu"`
	Memory    string `json:"memory"`
	Age       string `json:"age"`
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
	k8sClient   *http.Client
	k8sToken    string
	rabbitmqURL = "http://guest:guest@localhost:15672"
)

func main() {
	if url := os.Getenv("RABBITMQ_URL"); url != "" {
		rabbitmqURL = url
	}
	port := "8082"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}

	// Initialize K8s HTTP client using ServiceAccount credentials if available
	initK8sClient()

	mux := http.NewServeMux()
	
	// Server-Sent Events stream
	mux.HandleFunc("GET /api/v1/telemetry/stream", handleSSEStream)
	
	// Embed and serve dashboard static assets
	mux.Handle("GET /static/", http.FileServer(http.FS(staticFS)))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		htmlBytes, err := staticFS.ReadFile("static/index.html")
		if err != nil {
			http.Error(w, "Dashboard file not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		w.Write(htmlBytes)
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
	log.Println("[Telemetry Observer] Kubernetes API Client initialized successfully.")
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

func scrapeTelemetry() TelemetryPayload {
	payload := TelemetryPayload{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	// 1. Scrape RabbitMQ Queue Depths
	payload.RabbitMQ.JavaQueueDepth = getRabbitMQQueueDepth("java-tasks")
	payload.RabbitMQ.NodeQueueDepth = getRabbitMQQueueDepth("node-tasks")

	// 2. Scrape K8s Pod statuses & metrics if in cluster
	if k8sToken == "" {
		// Mock stats fallback for local development
		payload.Pods = []PodTelemetry{
			{Name: "gate-mock", Status: "Running", Restarts: 0, CPU: "12m", Memory: "32Mi", Age: "1h"},
			{Name: "broker-mock", Status: "Running", Restarts: 0, CPU: "85m", Memory: "180Mi", Age: "1h"},
			{Name: "worker-mock", Status: "Running", Restarts: 0, CPU: "5m", Memory: "25Mi", Age: "1h"},
		}
		return payload
	}

	podsMap := make(map[string]*PodTelemetry)

	// Fetch Pod status list
	podsURL := "https://kubernetes.default.svc/api/v1/namespaces/reddo/pods"
	req, _ := http.NewRequest("GET", podsURL, nil)
	req.Header.Set("Authorization", "Bearer "+k8sToken)

	resp, err := k8sClient.Do(req)
	if err == nil && resp.StatusCode == http.StatusOK {
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
				status := item.Status.Phase
				// Check for waiting CrashLoopBackOff or Error states
				for _, cs := range item.Status.ContainerStatuses {
					if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
						status = cs.State.Waiting.Reason
					}
				}

				restarts := 0
				if len(item.Status.ContainerStatuses) > 0 {
					restarts = item.Status.ContainerStatuses[0].RestartCount
				}

				age := time.Since(item.Metadata.CreationTimestamp).Round(time.Second).String()

				p := &PodTelemetry{
					Name:     item.Metadata.Name,
					Status:   status,
					Restarts: restarts,
					Age:      age,
					CPU:      "0m",
					Memory:   "0Mi",
				}
				podsMap[item.Metadata.Name] = p
			}
		}
		resp.Body.Close()
	}

	// Fetch Pod CPU/Memory metrics
	metricsURL := "https://kubernetes.default.svc/apis/metrics.k8s.io/v1beta1/namespaces/reddo/pods"
	req, _ = http.NewRequest("GET", metricsURL, nil)
	req.Header.Set("Authorization", "Bearer "+k8sToken)

	resp, err = k8sClient.Do(req)
	if err == nil && resp.StatusCode == http.StatusOK {
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
					// Sum containers usages
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

	// Extract username & password from raw RabbitMQ URL for basic auth
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
