package source

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterSource("salesforce", func(config map[string]any) (core.Source, error) {
		return NewSalesforceSource(config)
	})
	registry.RegisterSource("github", func(config map[string]any) (core.Source, error) {
		return NewGitHubSource(config)
	})
	registry.RegisterSource("hubspot", func(config map[string]any) (core.Source, error) {
		return NewHubSpotSource(config)
	})
	registry.RegisterSource("stripe", func(config map[string]any) (core.Source, error) {
		return NewStripeSource(config)
	})
	registry.RegisterSource("notion", func(config map[string]any) (core.Source, error) {
		return NewNotionSource(config)
	})
}

// mergeRestConfig 将 defaults 与 user 配置合并，user 覆盖 defaults。
// 不会覆盖 user 中显式存在的键（即使值为空字符串也保留 user）。
func mergeRestConfig(defaults, user map[string]any) map[string]any {
	out := make(map[string]any, len(defaults)+len(user))
	for k, v := range defaults {
		out[k] = v
	}
	for k, v := range user {
		out[k] = v
	}
	return out
}

func configString(config map[string]any, key, def string) string {
	if v, ok := config[key].(string); ok && v != "" {
		return v
	}
	return def
}

// BuildRestTemplateConfig 返回模板展开后的 rest_source 配置（便于单测断言）。
// template 为 salesforce/github/hubspot/stripe/notion。
func BuildRestTemplateConfig(template string, config map[string]any) (map[string]any, error) {
	if config == nil {
		config = map[string]any{}
	}
	switch strings.ToLower(template) {
	case "salesforce":
		return buildSalesforceConfig(config)
	case "github":
		return buildGitHubConfig(config)
	case "hubspot":
		return buildHubSpotConfig(config)
	case "stripe":
		return buildStripeConfig(config)
	case "notion":
		return buildNotionConfig(config)
	default:
		return nil, fmt.Errorf("unknown rest template %q", template)
	}
}

// ── Salesforce ────────────────────────────────────────────────────────

// NewSalesforceSource 使用 OAuth2 client_credentials 拉取 SOQL/sObject 列表。
// 配置：object（sObject 名，默认 Account）、api_version、instance_url、token_url、client_id、client_secret。
func NewSalesforceSource(config map[string]any) (core.Source, error) {
	cfg, err := buildSalesforceConfig(config)
	if err != nil {
		return nil, err
	}
	return NewRestSource(cfg)
}

func buildSalesforceConfig(config map[string]any) (map[string]any, error) {
	object := configString(config, "object", "Account")
	apiVersion := configString(config, "api_version", "v59.0")
	// login 主机用于 token；数据 API 使用 instance_url（可与 login 相同，生产通常是 *.my.salesforce.com）
	loginURL := strings.TrimRight(configString(config, "login_url", "https://login.salesforce.com"), "/")
	instanceURL := strings.TrimRight(configString(config, "instance_url", loginURL), "/")
	defaults := map[string]any{
		"name":        "salesforce",
		"method":      "GET",
		"auth":        "oauth2_client_credentials",
		"token_url":   loginURL + "/services/oauth2/token",
		"pagination":  "offset",
		"page_param":  "offset",
		"size_param":  "limit",
		"page_size":   200,
		"result_key":  "records",
		"total_field": "totalSize",
	}
	if _, ok := config["url"]; !ok {
		// 默认 SOQL：SELECT Id FROM <object>；完整字段查询可覆盖 url
		defaults["url"] = fmt.Sprintf("%s/services/data/%s/query?q=SELECT%%20Id%%20FROM%%20%s",
			instanceURL, apiVersion, url.QueryEscape(object))
	}
	// 若用户只给了 oauth2_* 别名，合并后 rest_source 会识别
	merged := mergeRestConfig(defaults, config)
	return merged, nil
}

// ── GitHub ────────────────────────────────────────────────────────────

// NewGitHubSource 使用 Bearer token 拉取仓库资源（issues/prs/commits 等）。
// 配置：repo（owner/name）、resource（默认 issues）、token、base_url。
func NewGitHubSource(config map[string]any) (core.Source, error) {
	cfg, err := buildGitHubConfig(config)
	if err != nil {
		return nil, err
	}
	return NewRestSource(cfg)
}

func buildGitHubConfig(config map[string]any) (map[string]any, error) {
	repo := configString(config, "repo", "")
	resource := configString(config, "resource", "issues")
	baseURL := strings.TrimRight(configString(config, "base_url", "https://api.github.com"), "/")

	defaults := map[string]any{
		"name":          "github",
		"method":        "GET",
		"auth":          "bearer",
		"pagination":    "cursor",
		"cursor_header": "Link",
		"cursor_param":  "page",
		"size_param":    "per_page",
		"page_size":     100,
		"result_key":    "", // GitHub 列表接口返回顶层数组
		"headers": map[string]any{
			"Accept":               "application/vnd.github+json",
			"X-GitHub-Api-Version": "2022-11-28",
		},
	}
	if _, ok := config["url"]; !ok {
		if repo == "" {
			return nil, fmt.Errorf("github source requires repo (owner/name) or url")
		}
		// PRs 走 pulls 端点
		pathResource := resource
		if resource == "prs" || resource == "pull_requests" {
			pathResource = "pulls"
		}
		defaults["url"] = fmt.Sprintf("%s/repos/%s/%s", baseURL, strings.Trim(repo, "/"), pathResource)
	}
	// token 字段映射
	if _, has := config["token"]; !has {
		if v, ok := config["auth_token"].(string); ok {
			config["token"] = v
		}
	}
	return mergeRestConfig(defaults, config), nil
}

// ── HubSpot ───────────────────────────────────────────────────────────

// NewHubSpotSource 使用 API key（或 Bearer private app token）拉取 CRM 对象。
// 配置：object（contacts/deals/tickets/...）、api_key_value 或 token、base_url。
func NewHubSpotSource(config map[string]any) (core.Source, error) {
	cfg, err := buildHubSpotConfig(config)
	if err != nil {
		return nil, err
	}
	return NewRestSource(cfg)
}

func buildHubSpotConfig(config map[string]any) (map[string]any, error) {
	object := configString(config, "object", "contacts")
	baseURL := strings.TrimRight(configString(config, "base_url", "https://api.hubapi.com"), "/")

	defaults := map[string]any{
		"name":         "hubspot",
		"method":       "GET",
		"auth":         "api_key",
		"api_key_query": "hapikey",
		"pagination":   "cursor",
		"cursor_param": "after",
		"cursor_field": "paging.next.after",
		"size_param":   "limit",
		"page_size":    100,
		"result_key":   "results",
	}
	if _, ok := config["url"]; !ok {
		defaults["url"] = fmt.Sprintf("%s/crm/v3/objects/%s", baseURL, object)
	}
	// 若用户提供 bearer token（private app），切换为 bearer
	if tok := configString(config, "token", ""); tok != "" {
		defaults["auth"] = "bearer"
		delete(defaults, "api_key_query")
	} else if tok := configString(config, "auth_token", ""); tok != "" {
		config["token"] = tok
		defaults["auth"] = "bearer"
		delete(defaults, "api_key_query")
	}
	if key := configString(config, "api_key", ""); key != "" {
		if _, ok := config["api_key_value"]; !ok {
			config["api_key_value"] = key
		}
	}
	return mergeRestConfig(defaults, config), nil
}

// ── Stripe ────────────────────────────────────────────────────────────

// NewStripeSource 使用 Bearer secret key 拉取 charges/customers/invoices 等。
// 配置：resource（默认 charges）、token、base_url。
func NewStripeSource(config map[string]any) (core.Source, error) {
	cfg, err := buildStripeConfig(config)
	if err != nil {
		return nil, err
	}
	return NewRestSource(cfg)
}

func buildStripeConfig(config map[string]any) (map[string]any, error) {
	resource := configString(config, "resource", "charges")
	baseURL := strings.TrimRight(configString(config, "base_url", "https://api.stripe.com"), "/")

	defaults := map[string]any{
		"name":         "stripe",
		"method":       "GET",
		"auth":         "bearer",
		"pagination":   "cursor",
		"cursor_param": "starting_after",
		"cursor_field": "data[-1].id", // 特殊：用最后一条 id；下面会在 rest 之外做兼容
		"size_param":   "limit",
		"page_size":    100,
		"result_key":   "data",
	}
	// Stripe 游标实际是最后一条记录的 id；用 has_more + data 最后 id
	// rest_source 的 cursor_field 不支持 [-1]，改为读取自定义字段：我们在响应中用
	// "next_cursor" 约定；这里改用 body 里的常见扩展：cursor_field 留空 + 用
	// result 后处理。为简洁，使用 Stripe 的 starting_after，并在 cursor_field
	// 设置为空时从 data 最后一条取 id（在 rest.go 已不支持），因此这里用
	// 一个实用约定：cursor_field = "" 且依赖 Stripe 的 has_more 通过
	// page_token 风格不合适。改为：
	// 使用 cursor_field 指向不存在字段时会停止；我们改为 page_token 风格？
	// Stripe list: { data: [...], has_more: true }，下一页 starting_after=<last.id>
	// 实现：cursor_field 特殊值 "__last_id__" 在 rest 中处理。
	defaults["cursor_field"] = "__last_id__"

	if _, ok := config["url"]; !ok {
		defaults["url"] = fmt.Sprintf("%s/v1/%s", baseURL, strings.Trim(resource, "/"))
	}
	if _, has := config["token"]; !has {
		if v, ok := config["auth_token"].(string); ok {
			config["token"] = v
		} else if v, ok := config["api_key"].(string); ok {
			config["token"] = v
		} else if v, ok := config["secret_key"].(string); ok {
			config["token"] = v
		}
	}
	return mergeRestConfig(defaults, config), nil
}

// ── Notion ────────────────────────────────────────────────────────────

// NewNotionSource 使用 Integration token 查询 database。
// 配置：database_id、token、api_version。
func NewNotionSource(config map[string]any) (core.Source, error) {
	cfg, err := buildNotionConfig(config)
	if err != nil {
		return nil, err
	}
	return NewRestSource(cfg)
}

func buildNotionConfig(config map[string]any) (map[string]any, error) {
	databaseID := configString(config, "database_id", "")
	baseURL := strings.TrimRight(configString(config, "base_url", "https://api.notion.com"), "/")
	apiVersion := configString(config, "api_version", "2022-06-28")

	defaults := map[string]any{
		"name":        "notion",
		"method":      "POST",
		"auth":        "bearer",
		"pagination":  "page_token",
		"token_param": "start_cursor",
		"token_field": "next_cursor",
		"page_size":   100,
		"result_key":  "results",
		"body":        `{"page_size":100}`,
		"body_type":   "json",
		"headers": map[string]any{
			"Notion-Version": apiVersion,
			"Content-Type":   "application/json",
		},
	}
	if _, ok := config["url"]; !ok {
		if databaseID == "" {
			return nil, fmt.Errorf("notion source requires database_id or url")
		}
		defaults["url"] = fmt.Sprintf("%s/v1/databases/%s/query", baseURL, databaseID)
	}
	if _, has := config["token"]; !has {
		if v, ok := config["auth_token"].(string); ok {
			config["token"] = v
		} else if v, ok := config["integration_token"].(string); ok {
			config["token"] = v
		}
	}
	// 若用户给了 page_size，同步进 body
	if ps := readInt(config, "page_size", 0); ps > 0 {
		defaults["body"] = fmt.Sprintf(`{"page_size":%d}`, ps)
		defaults["page_size"] = ps
	}
	return mergeRestConfig(defaults, config), nil
}
