// Handles the communication with the RabbitMQ Management API to discover nodes and queue leaders.
package rmq

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Parameters for connecting to the RabbitMQ Management API
type Controller struct {
	ManagementURL string
	User          string
	Password      string
}

func NewController(managementURL, user, password string) *Controller {
	return &Controller{
		ManagementURL: managementURL,
		User:          user,
		Password:      password,
	}
}

// JSON fields returned by the Management API for a specific queue endpoint (api/queues/vhost/queueName).
type QueueInfo struct {
	Node   string `json:"node"`   // The Erlang node name where the queue is located (e.g., rabbit@hostname)
	Leader string `json:"leader"` // The leader node name for Quorum queues
}

// Query the RabbitMQ Management API to find out which node is the leader for the given queue.
func (controller *Controller) GetQueueLeaderNode(vhost, queueName string) (string, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	if vhost == "/" {
		vhost = "%2f"
	}

	url := fmt.Sprintf("%s/api/queues/%s/%s", controller.ManagementURL, vhost, queueName)

	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	request.SetBasicAuth(controller.User, controller.Password)

	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return "", fmt.Errorf("failed to get queue info: status %d", response.StatusCode)
	}

	var info QueueInfo
	if err := json.NewDecoder(response.Body).Decode(&info); err != nil {
		return "", err
	}

	leader := info.Node
	if info.Leader != "" {
		leader = info.Leader
	}

	// Extract hostname from rabbit@hostname format of the API
	if idx := strings.Index(leader, "@"); idx != -1 {
		return leader[idx+1:], nil
	}
	return leader, nil
}

// Erlang node name (e.g., rabbit@hostname) returned by the Management API for the nodes endpoint (api/nodes).
type NodeInfo struct {
	Name string `json:"name"`
}

// Fetches all active nodes in the RabbitMQ cluster
func (controller *Controller) GetNodes() ([]string, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	url := fmt.Sprintf("%s/api/nodes", controller.ManagementURL)

	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	request.SetBasicAuth(controller.User, controller.Password)

	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get nodes: status %d", response.StatusCode)
	}

	var nodes []NodeInfo
	if err := json.NewDecoder(response.Body).Decode(&nodes); err != nil {
		return nil, err
	}

	var hostnames []string
	for _, nodeInfo := range nodes {
		// Extract hostname from rabbit@hostname
		if idx := strings.Index(nodeInfo.Name, "@"); idx != -1 {
			hostnames = append(hostnames, nodeInfo.Name[idx+1:])
		} else {
			hostnames = append(hostnames, nodeInfo.Name)
		}
	}
	return hostnames, nil
}

// Create a new AMQP connection with custom buffer settings.
func (controller *Controller) Connect(url string) (*amqp.Connection, error) {
	config := amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			connection, err := net.DialTimeout(network, addr, 10*time.Second)
			if err != nil {
				return nil, err
			}
			if tcpConn, ok := connection.(*net.TCPConn); ok {
				// Keep tcp connections alive and send keep-alive packets every 30 seconds
				// => prevent unexpected connection drops during the benchmark
				tcpConn.SetKeepAlive(true)
				tcpConn.SetKeepAlivePeriod(30 * time.Second)
				// Reduce latency by disabling Nagle's algorithm,
				// => removes another factor that impacts our latency measurements
				tcpConn.SetNoDelay(true)
			}
			return connection, nil
		},
	}
	return amqp.DialConfig(url, config)
}

// DeleteQueue deletes a queue via the Management API.
// This is useful when queue arguments need to change between runs.
func (controller *Controller) DeleteQueue(vhost, queueName string) error {
	client := &http.Client{Timeout: 10 * time.Second}
	encodedVhost := vhost
	if vhost == "/" {
		encodedVhost = "%2f"
	}

	url := fmt.Sprintf("%s/api/queues/%s/%s", controller.ManagementURL, encodedVhost, queueName)

	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	request.SetBasicAuth(controller.User, controller.Password)

	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	// 204 No Content = success, 404 = queue doesn't exist (also fine)
	if response.StatusCode != 204 && response.StatusCode != 404 {
		return fmt.Errorf("failed to delete queue: status %d", response.StatusCode)
	}

	// For quorum queues, we need to wait until the deletion has propagated across all nodes.
	// Poll the queue status until it returns 404 (not found) or timeout after 10 seconds.
	if response.StatusCode == 204 {
		checkURL := fmt.Sprintf("%s/api/queues/%s/%s", controller.ManagementURL, encodedVhost, queueName)
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			time.Sleep(200 * time.Millisecond)
			checkRequest, _ := http.NewRequest("GET", checkURL, nil)
			checkRequest.SetBasicAuth(controller.User, controller.Password)
			checkResponse, err := client.Do(checkRequest)
			if err != nil {
				continue
			}
			checkResponse.Body.Close()
			if checkResponse.StatusCode == 404 {
				// Queue is confirmed deleted
				return nil
			}
		}
		return fmt.Errorf("queue deletion timed out - queue still exists after 10s")
	}

	return nil
}