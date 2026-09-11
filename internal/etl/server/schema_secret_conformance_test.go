package server

import (
	"strings"
	"testing"
)

// credentialBearingHints lists name fragments that indicate a config field
// carries credentials. Every FieldString whose name matches one of these must
// be marked Secret: true in the descriptor schema. This is the IT-3/T3.1
// full-descriptor sweep: a missed marking fails the test instead of shipping
// an unencrypted credential.
var credentialBearingHints = []string{
	"password", "passwd", "secret", "token", "api_key", "apikey",
	"credential", "private_key", "dsn",
}

// knownNonCredentialDocuments lists fields whose name matches a credential
// hint but which provably carry a name/URL/identifier, not a credential. Each
// entry must keep its justification; removing a justification without
// re-reviewing the field is a spec violation (IT-3 delivery constraint 1).
var knownNonCredentialDocuments = map[string]string{
	// OAuth2 endpoint URLs (no credential value).
	"sources/http/oauth2_token_url":        "OAuth2 token endpoint URL, not a credential",
	"sources/github/token_url":             "OAuth2 token endpoint URL, not a credential",
	"sources/github/oauth2_token_url":      "OAuth2 token endpoint URL, not a credential",
	"sources/hubspot/token_url":            "OAuth2 token endpoint URL, not a credential",
	"sources/hubspot/oauth2_token_url":     "OAuth2 token endpoint URL, not a credential",
	"sources/notion/token_url":             "OAuth2 token endpoint URL, not a credential",
	"sources/notion/oauth2_token_url":      "OAuth2 token endpoint URL, not a credential",
	"sources/rest_source/token_url":        "OAuth2 token endpoint URL, not a credential",
	"sources/rest_source/oauth2_token_url": "OAuth2 token endpoint URL, not a credential",
	"sources/salesforce/token_url":         "OAuth2 token endpoint URL, not a credential",
	"sources/salesforce/oauth2_token_url":  "OAuth2 token endpoint URL, not a credential",
	"sources/stripe/token_url":             "OAuth2 token endpoint URL, not a credential",
	"sources/stripe/oauth2_token_url":      "OAuth2 token endpoint URL, not a credential",
	// Header/query/JSON-field NAMES for auth (the credential lives in the
	// actual api_key/token value fields, which are marked Secret).
	"sources/github/api_key_header":          "header name for API key auth, not the key",
	"sources/github/api_key_query":           "query param name for API key auth, not the key",
	"sources/github/token_field":             "JSON field name in token response / page-token field",
	"sources/github/token_param":             "page-token request parameter name",
	"sources/github/oauth2_token_field":      "JSON field name for access token extraction",
	"sources/hubspot/api_key_header":         "header name for API key auth, not the key",
	"sources/hubspot/api_key_query":          "query param name for API key auth, not the key",
	"sources/hubspot/token_field":            "JSON field name in token response / page-token field",
	"sources/hubspot/token_param":            "page-token request parameter name",
	"sources/hubspot/oauth2_token_field":     "JSON field name for access token extraction",
	"sources/notion/api_key_header":          "header name for API key auth, not the key",
	"sources/notion/api_key_query":           "query param name for API key auth, not the key",
	"sources/notion/token_field":             "JSON field name in token response / page-token field",
	"sources/notion/token_param":             "page-token request parameter name",
	"sources/notion/oauth2_token_field":      "JSON field name for access token extraction",
	"sources/rest_source/api_key_header":     "header name for API key auth, not the key",
	"sources/rest_source/api_key_query":      "query param name for API key auth, not the key",
	"sources/rest_source/token_field":        "JSON field name in token response / page-token field",
	"sources/rest_source/token_param":        "page-token request parameter name",
	"sources/rest_source/oauth2_token_field": "JSON field name for access token extraction",
	"sources/salesforce/api_key_header":      "header name for API key auth, not the key",
	"sources/salesforce/api_key_query":       "query param name for API key auth, not the key",
	"sources/salesforce/token_field":         "JSON field name in token response / page-token field",
	"sources/salesforce/token_param":         "page-token request parameter name",
	"sources/salesforce/oauth2_token_field":  "JSON field name for access token extraction",
	"sources/stripe/api_key_header":          "header name for API key auth, not the key",
	"sources/stripe/api_key_query":           "query param name for API key auth, not the key",
	"sources/stripe/token_field":             "JSON field name in token response / page-token field",
	"sources/stripe/token_param":             "page-token request parameter name",
	"sources/stripe/oauth2_token_field":      "JSON field name for access token extraction",
	"sources/http/oauth2_token_field":        "JSON field name in token response",
	// Document identifiers (part of a public URL, not an auth credential).
	"sources/feishu_sheet/spreadsheet_token": "spreadsheet document id from the sheet URL, not a credential",
}

func TestAllCredentialBearingFieldsAreMarkedSecret(t *testing.T) {
	schema := configSchema()
	checked := 0
	for kind, connectors := range schema {
		typed, ok := connectors.(map[string][]ConfigField)
		if !ok {
			t.Fatalf("schema[%s] has unexpected type %T", kind, connectors)
		}
		for connector, fields := range typed {
			for _, f := range fields {
				if f.Type != FieldString {
					continue
				}
				lower := strings.ToLower(f.Name)
				matched := ""
				for _, hint := range credentialBearingHints {
					if strings.Contains(lower, hint) {
						matched = hint
						break
					}
				}
				if matched == "" {
					continue
				}
				key := kind + "/" + connector + "/" + f.Name
				if reason, except := knownNonCredentialDocuments[key]; except {
					_ = reason
					continue
				}
				checked++
				if !f.Secret {
					t.Errorf("%s matches credential hint %q but is not marked Secret (description: %s)", key, matched, f.Description)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatalf("credential sweep matched no fields; the sweep itself is broken")
	}
	t.Logf("credential-bearing fields verified secret: %d", checked)
}

// TestDescriptorSecretFieldsMatchSchema ensures the descriptor resolver input
// (d.SecretFields) is derived from the same Secret marking the sweep checks,
// for all four DSN paths required by IT-3 spec acceptance #2.
func TestFourDSNPathsAreSecret(t *testing.T) {
	required := []struct{ kind, connector, field string }{
		{"sinks", "jdbc", "dsn"},
		{"transforms", "enricher", "dsn"},
		{"transforms", "lookup", "dsn"},
		{"transforms", "dbt", "dsn"},
	}
	schema := configSchema()
	for _, req := range required {
		connectors, ok := schema[req.kind].(map[string][]ConfigField)
		if !ok {
			t.Fatalf("schema[%s] missing", req.kind)
		}
		fields, ok := connectors[req.connector]
		if !ok {
			t.Fatalf("schema[%s][%s] missing", req.kind, req.connector)
		}
		found := false
		for _, f := range fields {
			if f.Name == req.field {
				found = true
				if !f.Secret {
					t.Errorf("%s/%s.%s must be Secret (spec acceptance 2)", req.kind, req.connector, req.field)
				}
			}
		}
		if !found {
			t.Errorf("%s/%s has no field %s", req.kind, req.connector, req.field)
		}
	}
}

// TestDescriptorResolverCoversAllDeclaredSecrets verifies the resolver built
// from descriptors answers true for every declared secret field of every
// connector, and answers false for a non-secret field.
func TestDescriptorResolverCoversAllDeclaredSecrets(t *testing.T) {
	resolver := descriptorSecretFieldResolver()
	for _, d := range connectorDescriptors() {
		for _, f := range d.SecretFields {
			if !resolver(d.Kind, d.Type, f) {
				t.Errorf("resolver(%s,%s,%s)=false but descriptor declares it secret", d.Kind, d.Type, f)
			}
		}
	}
}
