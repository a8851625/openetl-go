package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type Level string

const (
	LevelInfo    Level = "info"
	LevelWarning Level = "warning"
	LevelError   Level = "error"
)

type Event struct {
	Level     Level             `json:"level"`
	Title     string            `json:"title"`
	Message   string            `json:"message"`
	JobName   string            `json:"job_name,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

type Channel interface {
	Send(ctx context.Context, event Event) error
	Name() string
}

type WebhookChannel struct {
	URL    string
	client *http.Client
}

func NewWebhookChannel(url string) *WebhookChannel {
	return &WebhookChannel{
		URL:    url,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *WebhookChannel) Send(ctx context.Context, event Event) error {
	event.Timestamp = time.Now()
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal alert: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("send alert: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("alert response %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (c *WebhookChannel) Name() string { return "webhook" }

type LogChannel struct{}

func (c *LogChannel) Send(ctx context.Context, event Event) error {
	event.Timestamp = time.Now()
	data, _ := json.Marshal(event)
	fmt.Printf("[ALERT] %s\n", string(data))
	return nil
}

func (c *LogChannel) Name() string { return "log" }

type Manager struct {
	mu       sync.RWMutex
	channels []Channel
	queue    chan Event
	wg       sync.WaitGroup

	// Dedup: track recently-fired alerts by fingerprint to suppress
	// repeats within the suppression window (default 5 minutes).
	dedup       map[string]time.Time
	dedupWindow time.Duration
}

func NewManager() *Manager {
	m := &Manager{
		queue:       make(chan Event, 256),
		dedup:       make(map[string]time.Time),
		dedupWindow: 5 * time.Minute,
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case event, ok := <-m.queue:
				if !ok {
					return
				}
				m.mu.RLock()
				channels := make([]Channel, len(m.channels))
				copy(channels, m.channels)
				m.mu.RUnlock()
				for _, ch := range channels {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					if err := ch.Send(ctx, event); err != nil {
						fmt.Printf("alert channel %s failed: %v\n", ch.Name(), err)
					}
					cancel()
				}
			case <-ticker.C:
				// Clean up expired dedup entries
				m.mu.Lock()
				now := time.Now()
				for k, t := range m.dedup {
					if now.Sub(t) > m.dedupWindow {
						delete(m.dedup, k)
					}
				}
				m.mu.Unlock()
			}
		}
	}()
	return m
}

func (m *Manager) Register(ch Channel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.channels = append(m.channels, ch)
}

// alertFingerprint generates a dedup key from title + job name.
func alertFingerprint(e Event) string {
	return e.JobName + "|" + e.Title
}

func (m *Manager) Send(ctx context.Context, event Event) {
	// Dedup: suppress if the same alert fingerprint was sent recently.
	fp := alertFingerprint(event)
	m.mu.Lock()
	if lastSent, ok := m.dedup[fp]; ok {
		if time.Since(lastSent) < m.dedupWindow {
			m.mu.Unlock()
			return // suppressed
		}
	}
	m.dedup[fp] = time.Now()
	m.mu.Unlock()

	select {
	case m.queue <- event:
	default:
		fmt.Printf("[ALERT] queue full, dropping event: %s\n", event.Title)
	}
}

func (m *Manager) SendAll(ctx context.Context, events []Event) {
	for _, e := range events {
		m.Send(ctx, e)
	}
}

func (m *Manager) Close() {
	close(m.queue)
	m.wg.Wait()
}
