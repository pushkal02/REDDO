// Package mq coordinates message broker interactions with RabbitMQ.
package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"reddo/gate/internal/config"
)

// TaskMessage is the JSON payload sent to workers via RabbitMQ queues.
type TaskMessage struct {
	TaskExecutionID    string          `json:"task_execution_id"`
	WorkflowInstanceID string          `json:"workflow_instance_id"`
	TaskKey            string          `json:"task_key"`
	InputData          json.RawMessage `json:"input_data"`
	CorrelationID      string          `json:"correlation_id"`
	RequestID          string          `json:"request_id"`
}

// Client wraps the RabbitMQ connection and channel, asserting topology on startup.
type Client struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewClient establishes a connection to RabbitMQ, opens a channel, and
// declares the required exchange, queues, and bindings.
func NewClient(cfg *config.Config) (*Client, error) {
	log.Printf("[RabbitMQ] Connecting to RabbitMQ at %s...", redactBrokerURL(cfg.RabbitMQURL))

	conn, err := amqp.Dial(cfg.RabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open RabbitMQ channel: %w", err)
	}

	client := &Client{
		conn:    conn,
		channel: channel,
	}

	// Initialize the topology idempotently on boot.
	if err := client.setupTopology(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to setup RabbitMQ topology: %w", err)
	}

	return client, nil
}

// setupTopology configures the exchange, queues, and bindings.
// If RabbitMQ is reset or starts fresh, this ensures the required topology is restored.
func (c *Client) setupTopology() error {
	exchangeName := "workflow-exchange"
	log.Printf("[RabbitMQ] Declaring topic exchange %q...", exchangeName)

	// Declare direct exchange
	err := c.channel.ExchangeDeclare(
		exchangeName, // name
		"direct",         // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return err
	}

	queues := []struct {
		name       string
		routingKey string
	}{
		{name: "java-tasks", routingKey: "tasks.java"},
		{name: "node-tasks", routingKey: "tasks.node"},
	}

	for _, q := range queues {
		log.Printf("[RabbitMQ] Declaring durable queue %q...", q.name)
		_, err := c.channel.QueueDeclare(
			q.name, // name
			true,   // durable
			false,  // delete when unused
			false,  // exclusive
			false,  // no-wait
			nil,    // arguments
		)
		if err != nil {
			return err
		}

		log.Printf("[RabbitMQ] Binding queue %q to exchange %q with routing key %q...", q.name, exchangeName, q.routingKey)
		err = c.channel.QueueBind(
			q.name,       // queue name
			q.routingKey, // routing key
			exchangeName, // exchange
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// PublishTask marshals and publishes a task execution payload to the exchange.
func (c *Client) PublishTask(ctx context.Context, worker string, msg TaskMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal task message: %w", err)
	}

	var routingKey string
	switch worker {
	case "java":
		routingKey = "tasks.java"
	case "node":
		routingKey = "tasks.node"
	default:
		return fmt.Errorf("unsupported worker type %q", worker)
	}

	log.Printf("[RabbitMQ] Publishing task %q for workflow %s to routing key %q", msg.TaskKey, msg.WorkflowInstanceID, routingKey)

	// Publish with confirmation/delivery mode set to Persistent (2) so messages survive broker crashes.
	return c.channel.PublishWithContext(
		ctx,
		"workflow-exchange", // exchange
		routingKey,          // routing key
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         body,
		},
	)
}

// Close gracefully tears down connections.
func (c *Client) Close() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
	log.Println("[RabbitMQ] Connection closed.")
}

func redactBrokerURL(url string) string {
	// A simple redacter for amqp://guest:guest@localhost:5672/ format
	// Locate colon after amqp:// and '@' character.
	const amqpPrefix = "amqp://"
	const amqpsPrefix = "amqps://"
	
	prefix := amqpPrefix
	if len(url) >= len(amqpsPrefix) && url[:len(amqpsPrefix)] == amqpsPrefix {
		prefix = amqpsPrefix
	}
	
	schemeEnd := len(url)
	if idx := byteIndex(url, prefix); idx != -1 {
		schemeEnd = idx + len(prefix)
	}

	atIdx := byteIndex(url[schemeEnd:], "@")
	if atIdx == -1 {
		return url
	}
	atIdx += schemeEnd

	colonIdx := byteIndex(url[schemeEnd:atIdx], ":")
	if colonIdx == -1 {
		return url
	}
	colonIdx += schemeEnd

	return url[:colonIdx] + ":****" + url[atIdx:]
}

func byteIndex(s, substr string) int {
	// Helper avoiding imports (implemented simply for safety)
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
