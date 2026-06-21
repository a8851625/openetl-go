package alert

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// DingTalkChannel sends alerts to a DingTalk (钉钉) group robot via webhook.
// Supports optional secret-based signing for secure robots.
type DingTalkChannel struct {
	WebhookURL string
	Secret     string
	client     *http.Client
}

func NewDingTalkChannel(webhookURL, secret string) *DingTalkChannel {
	return &DingTalkChannel{
		WebhookURL: webhookURL,
		Secret:     secret,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *DingTalkChannel) Name() string { return "dingtalk" }

func (c *DingTalkChannel) Send(ctx context.Context, event Event) error {
	title := fmt.Sprintf("[%s] %s", string(event.Level), event.Title)
	text := fmt.Sprintf("### %s\n\n**Pipeline:** %s\n\n%s\n\n**Time:** %s",
		title,
		event.JobName,
		event.Message,
		time.Now().Format("2006-01-02 15:04:05"))

	payload := map[string]any{
		"msgtype": "markdown",
		"markdown": map[string]string{
			"title": title,
			"text":  text,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal dingtalk payload: %w", err)
	}

	reqURL := c.WebhookURL
	if c.Secret != "" {
		timestamp := time.Now().UnixMilli()
		stringToSign := fmt.Sprintf("%d\n%s", timestamp, c.Secret)
		mac := hmac.New(sha256.New, []byte(c.Secret))
		mac.Write([]byte(stringToSign))
		sign := base64.StdEncoding.EncodeToString(mac.Sum(nil))
		reqURL = fmt.Sprintf("%s&timestamp=%d&sign=%s", reqURL, timestamp, url.QueryEscape(sign))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create dingtalk request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("send dingtalk alert: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("dingtalk response %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// FeishuChannel sends alerts to a Feishu (飞书) group robot via webhook.
// Supports optional secret-based signing.
type FeishuChannel struct {
	WebhookURL string
	Secret     string
	client     *http.Client
}

func NewFeishuChannel(webhookURL, secret string) *FeishuChannel {
	return &FeishuChannel{
		WebhookURL: webhookURL,
		Secret:     secret,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *FeishuChannel) Name() string { return "feishu" }

func (c *FeishuChannel) Send(ctx context.Context, event Event) error {
	levelEmoji := map[Level]string{
		LevelInfo:    "ℹ️",
		LevelWarning: "⚠️",
		LevelError:   "🔴",
	}
	emoji := levelEmoji[event.Level]
	if emoji == "" {
		emoji = "📢"
	}

	title := fmt.Sprintf("%s [%s] %s", emoji, string(event.Level), event.Title)
	content := fmt.Sprintf("**Pipeline:** %s\n**Message:** %s\n**Time:** %s",
		event.JobName, event.Message, time.Now().Format("2006-01-02 15:04:05"))

	payload := map[string]any{
		"msg_type": "interactive",
		"card": map[string]any{
			"header": map[string]any{
				"title": map[string]string{
					"tag":     "plain_text",
					"content": title,
				},
				"template": feishuColor(event.Level),
			},
			"elements": []map[string]any{
				{
					"tag": "div",
					"text": map[string]string{
						"tag":     "lark_md",
						"content": content,
					},
				},
			},
		},
	}

	// Feishu signing: when a Secret is configured, include timestamp + sign
	// in the top-level payload. sign = base64(HMAC-SHA256(timestamp + "\n" + secret, "")).
	if c.Secret != "" {
		ts := fmt.Sprintf("%d", time.Now().Unix())
		sign, err := feishuSign(ts, c.Secret)
		if err != nil {
			return fmt.Errorf("feishu sign: %w", err)
		}
		payload["timestamp"] = ts
		payload["sign"] = sign
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal feishu payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create feishu request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("send feishu alert: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("feishu response %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// feishuSign computes the Feishu webhook signature:
// sign = base64(HMAC-SHA256(key = timestamp + "\n" + secret, message = "")).
func feishuSign(timestamp, secret string) (string, error) {
	stringToSign := timestamp + "\n" + secret
	h := hmac.New(sha256.New, []byte(stringToSign))
	if _, err := h.Write(nil); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}

func feishuColor(level Level) string {
	switch level {
	case LevelError:
		return "red"
	case LevelWarning:
		return "orange"
	default:
		return "blue"
	}
}

// SlackChannel sends alerts to a Slack incoming webhook URL.
type SlackChannel struct {
	WebhookURL string
	client     *http.Client
}

func NewSlackChannel(webhookURL string) *SlackChannel {
	return &SlackChannel{
		WebhookURL: webhookURL,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *SlackChannel) Name() string { return "slack" }

func (c *SlackChannel) Send(ctx context.Context, event Event) error {
	color := map[Level]string{
		LevelInfo:    "#36a64f",
		LevelWarning: "#ff9900",
		LevelError:   "#ff0000",
	}[event.Level]
	if color == "" {
		color = "#cccccc"
	}

	payload := map[string]any{
		"attachments": []map[string]any{
			{
				"color":    color,
				"title":    fmt.Sprintf("[%s] %s", string(event.Level), event.Title),
				"text":     event.Message,
				"fallback": fmt.Sprintf("%s: %s", event.Title, event.Message),
				"fields": []map[string]string{
					{"title": "Pipeline", "value": event.JobName, "short": "true"},
					{"title": "Time", "value": time.Now().Format(time.RFC3339), "short": "true"},
				},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create slack request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("send slack alert: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("slack response %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}
