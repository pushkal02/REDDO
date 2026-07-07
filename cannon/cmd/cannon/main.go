package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// BlastConfig holds the payload parameters for a chaos run
type BlastConfig struct {
	TotalRequests          int      `json:"total_requests"`
	Concurrency            int      `json:"concurrency"`
	ZombieHangProbability  float64  `json:"zombie_hang_probability"`
	FatalCrashProbability  float64  `json:"fatal_crash_probability"`
	BusinessFailProbability float64  `json:"business_fail_probability"`
	WorkflowTemplates      []string `json:"workflow_templates"`
	Seed                   int64    `json:"seed"`
	ScheduleDelaySeconds   int      `json:"schedule_delay_seconds"`
}

// BlastStatus represents the runtime status of the blaster
type BlastStatus struct {
	Active                    bool    `json:"active"`
	Status                    string  `json:"status"` // "IDLE", "RUNNING", "SCHEDULED"
	Progress                  int32   `json:"progress"`
	Total                     int     `json:"total"`
	Concurrency               int     `json:"concurrency"`
	Errors                    int32   `json:"errors"`
	SuccessRate               float64 `json:"success_rate"`
	Seed                      int64   `json:"seed"`
	ScheduleRemainingSeconds  int     `json:"schedule_remaining_seconds"`
}

// Blaster manages the active chaos blast execution
type Blaster struct {
	mu           sync.Mutex
	status       string
	active       bool
	progress     int32
	errors       int32
	config       BlastConfig
	cancelFunc   context.CancelFunc
	timer        *time.Timer
	scheduledTime time.Time
}

var (
	blaster    = &Blaster{status: "IDLE"}
	gatewayURL = "http://localhost:8080"
)

func main() {
	if url := os.Getenv("GATEWAY_URL"); url != "" {
		gatewayURL = url
	}
	port := "8083"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}

	mux := http.NewServeMux()
	mux.HandleFunc("OPTIONS /", handleCORS)
	mux.HandleFunc("POST /api/v1/chaos/fire", handleFire)
	mux.HandleFunc("POST /api/v1/chaos/stop", handleStop)
	mux.HandleFunc("GET /api/v1/chaos/status", handleStatus)
	mux.HandleFunc("GET /health", handleHealth)

	// Apply CORS middleware to all endpoints
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Correlation-ID, X-Request-ID")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		mux.ServeHTTP(w, r)
	})

	log.Printf("[Chaos Cannon] Service starting on port %s targeting Gateway %s...", port, gatewayURL)
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}

func handleCORS(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, `{"status":"healthy"}`)
}

func handleFire(w http.ResponseWriter, r *http.Request) {
	var cfg BlastConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"invalid JSON: %v"}`, err), http.StatusBadRequest)
		return
	}

	// Validate inputs
	if cfg.TotalRequests <= 0 {
		cfg.TotalRequests = 100
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if len(cfg.WorkflowTemplates) == 0 {
		cfg.WorkflowTemplates = []string{"valid_dag"}
	}
	if cfg.Seed == 0 {
		cfg.Seed = time.Now().UnixNano()
	}

	blaster.mu.Lock()
	defer blaster.mu.Unlock()

	// Stop any existing runs/timers
	blaster.stopInternal()

	blaster.config = cfg
	blaster.progress = 0
	blaster.errors = 0

	if cfg.ScheduleDelaySeconds > 0 {
		blaster.status = "SCHEDULED"
		blaster.active = false
		blaster.scheduledTime = time.Now().Add(time.Duration(cfg.ScheduleDelaySeconds) * time.Second)
		
		log.Printf("[Chaos Cannon] Scheduling chaos blast in %d seconds with seed %d...", cfg.ScheduleDelaySeconds, cfg.Seed)
		
		blaster.timer = time.AfterFunc(time.Duration(cfg.ScheduleDelaySeconds)*time.Second, func() {
			blaster.mu.Lock()
			if blaster.status != "SCHEDULED" {
				blaster.mu.Unlock()
				return
			}
			blaster.status = "RUNNING"
			blaster.active = true
			ctx, cancel := context.WithCancel(context.Background())
			blaster.cancelFunc = cancel
			blaster.mu.Unlock()

			go blaster.runBlast(ctx)
		})

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"message":  "Chaos blast scheduled successfully",
			"status":   blaster.status,
			"delay_s":  cfg.ScheduleDelaySeconds,
			"seed":     cfg.Seed,
		})
		return
	}

	// Immediate execution
	blaster.status = "RUNNING"
	blaster.active = true
	ctx, cancel := context.WithCancel(context.Background())
	blaster.cancelFunc = cancel

	log.Printf("[Chaos Cannon] Launching immediate chaos blast (Total: %d, Concurrency: %d) with seed %d...", cfg.TotalRequests, cfg.Concurrency, cfg.Seed)
	go blaster.runBlast(ctx)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message": "Chaos blast started successfully",
		"status":  blaster.status,
		"seed":    cfg.Seed,
	})
}

func handleStop(w http.ResponseWriter, r *http.Request) {
	blaster.mu.Lock()
	defer blaster.mu.Unlock()

	blaster.stopInternal()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, `{"message":"Chaos blast successfully terminated"}`)
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	blaster.mu.Lock()
	defer blaster.mu.Unlock()

	var remainingSecs int
	if blaster.status == "SCHEDULED" {
		rem := time.Until(blaster.scheduledTime)
		if rem > 0 {
			remainingSecs = int(rem.Seconds())
		}
	}

	progress := atomic.LoadInt32(&blaster.progress)
	errors := atomic.LoadInt32(&blaster.errors)
	successRate := 1.0
	if progress > 0 {
		successRate = float64(progress-errors) / float64(progress)
	}

	status := BlastStatus{
		Active:                    blaster.active,
		Status:                    blaster.status,
		Progress:                  progress,
		Total:                     blaster.config.TotalRequests,
		Concurrency:               blaster.config.Concurrency,
		Errors:                    errors,
		SuccessRate:               successRate,
		Seed:                      blaster.config.Seed,
		ScheduleRemainingSeconds:  remainingSecs,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// stopInternal helper cancels active contexts/timers. MUST hold mutex lock before calling.
func (b *Blaster) stopInternal() {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	if b.cancelFunc != nil {
		b.cancelFunc()
		b.cancelFunc = nil
	}
	b.active = false
	b.status = "IDLE"
}

// runBlast contains the main execution loop sending requests concurrently
func (b *Blaster) runBlast(ctx context.Context) {
	cfg := b.config
	total := cfg.TotalRequests
	concurrency := cfg.Concurrency

	// Calculate fault indices using seed deterministically
	r := rand.New(rand.NewSource(cfg.Seed))
	indices := make([]int, total)
	for i := 0; i < total; i++ {
		indices[i] = i
	}
	// Fisher-Yates shuffle
	r.Shuffle(total, func(i, j int) {
		indices[i], indices[j] = indices[j], indices[i]
	})

	// Slice off targets based on probabilities
	numZombie := int(float64(total) * cfg.ZombieHangProbability)
	numCrash := int(float64(total) * cfg.FatalCrashProbability)
	numFail := int(float64(total) * cfg.BusinessFailProbability)

	zombieSet := make(map[int]bool)
	crashSet := make(map[int]bool)
	failSet := make(map[int]bool)

	for i := 0; i < numZombie && i < total; i++ {
		zombieSet[indices[i]] = true
	}
	for i := numZombie; i < numZombie+numCrash && i < total; i++ {
		crashSet[indices[i]] = true
	}
	for i := numZombie + numCrash; i < numZombie+numCrash+numFail && i < total; i++ {
		failSet[indices[i]] = true
	}

	// Channel to queue indices
	indexChan := make(chan int, total)
	for i := 0; i < total; i++ {
		indexChan <- i
	}
	close(indexChan)

	var wg sync.WaitGroup
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	log.Printf("[Chaos Cannon] Starting worker pool of size %d...", concurrency)

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case idx, ok := <-indexChan:
					if !ok {
						return
					}

					// Build payload based on shuffled parameters
					payload := b.generatePayload(idx, cfg.Seed, cfg.WorkflowTemplates, zombieSet[idx], crashSet[idx], failSet[idx])

					// Execute POST call to Gateway
					err := b.submitWorkflow(ctx, client, payload)
					
					// Increment progress atomic counter
					atomic.AddInt32(&b.progress, 1)

					if err != nil {
						atomic.AddInt32(&b.errors, 1)
						log.Printf("[Chaos Cannon] Submitting request %d failed: %v", idx, err)
					}
				}
			}
		}(w)
	}

	wg.Wait()

	b.mu.Lock()
	b.active = false
	b.status = "IDLE"
	b.mu.Unlock()

	log.Printf("[Chaos Cannon] Blast completed. Progress: %d/%d, Errors: %d", b.progress, total, b.errors)
}

// submitWorkflow shoots an HTTP request containing trace headers to the Gateway
func (b *Blaster) submitWorkflow(ctx context.Context, client *http.Client, payload map[string]interface{}) error {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", gatewayURL+"/api/v1/workflows", bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	
	// Propagate trace IDs
	wfID := payload["id"].(string)
	req.Header.Set("X-Correlation-ID", "blast-corr-"+wfID)
	req.Header.Set("X-Request-ID", "blast-req-"+wfID)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("gateway returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// generatePayload builds a workflow DAG inserting relevant failure instructions
func (b *Blaster) generatePayload(idx int, blastSeed int64, templates []string, zombie, crash, fail bool) map[string]interface{} {
	// Select template using index-derived RNG to maintain deterministic runs
	templateIndex := (int(blastSeed) + idx) % len(templates)
	templateName := templates[templateIndex]

	wfID := fmt.Sprintf("blast-%d-%d", blastSeed, idx)

	var name string
	var tasks map[string]interface{}

	switch templateName {
	case "saga_rollback":
		name = "Saga Rollback Chaos Run"
		taskAInput := map[string]interface{}{}
		if fail {
			taskAInput["fail"] = true
		} else if zombie {
			taskAInput["chaos_command"] = "ZOMBIE_HANG"
		} else if crash {
			taskAInput["chaos_command"] = "FATAL_CRASH"
		}

		tasks = map[string]interface{}{
			"task_a": map[string]interface{}{
				"worker":       "java",
				"input_data":   taskAInput,
				"dependencies": []string{},
			},
			"task_b": map[string]interface{}{
				"worker":       "node",
				"input_data":   map[string]interface{}{},
				"dependencies": []string{"task_a"},
			},
		}

	case "saga_compensation":
		name = "Saga Compensation Chaos Run"
		taskBInput := map[string]interface{}{}
		if fail {
			taskBInput["fail"] = true
		} else if zombie {
			taskBInput["chaos_command"] = "ZOMBIE_HANG"
		} else if crash {
			taskBInput["chaos_command"] = "FATAL_CRASH"
		}

		tasks = map[string]interface{}{
			"task_a": map[string]interface{}{
				"worker":       "java",
				"input_data":   map[string]interface{}{},
				"dependencies": []string{},
			},
			"task_b": map[string]interface{}{
				"worker":       "node",
				"input_data":   taskBInput,
				"dependencies": []string{"task_a"},
				"compensation": "task_a_comp",
			},
			"task_a_comp": map[string]interface{}{
				"worker":       "java",
				"input_data":   map[string]interface{}{},
				"dependencies": []string{"__SAGA_FAIL__"},
			},
			"__SAGA_FAIL__": map[string]interface{}{
				"worker":       "java",
				"input_data":   map[string]interface{}{},
				"dependencies": []string{"task_b"},
			},
		}

	default: // "valid_dag"
		name = "Valid Workflow Chaos Run"
		taskAInput := map[string]interface{}{}
		taskBInput := map[string]interface{}{}

		// In normal run, zombie/crash faults can still hit either Java (task_a) or Node (task_b)
		if zombie {
			taskBInput["chaos_command"] = "ZOMBIE_HANG"
		} else if crash {
			taskBInput["chaos_command"] = "FATAL_CRASH"
		} else if fail {
			// For a normal valid_dag, we don't throw failures unless it's a fault simulation
			taskBInput["fail"] = true
		}

		tasks = map[string]interface{}{
			"task_a": map[string]interface{}{
				"worker":       "java",
				"input_data":   taskAInput,
				"dependencies": []string{},
			},
			"task_b": map[string]interface{}{
				"worker":       "node",
				"input_data":   taskBInput,
				"dependencies": []string{"task_a"},
			},
		}
	}

	return map[string]interface{}{
		"id":      wfID,
		"name":    name,
		"payload": map[string]interface{}{},
		"dag": map[string]interface{}{
			"tasks": tasks,
		},
	}
}
