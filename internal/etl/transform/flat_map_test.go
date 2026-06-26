//go:build !nolua

package transform

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func TestFlatMapTransformExpandsLuaArrayResults(t *testing.T) {
	transform, err := registry.BuildTransform("flat_map", map[string]any{
		"script": `
local out = {}
for i, item in ipairs(record.data.items) do
  out[i] = {
    data = {
      order_id = record.data.id,
      sku = item.sku,
      qty = item.qty,
    },
    metadata = {
      table = "order_items",
    },
  }
end
return out
`,
	})
	if err != nil {
		t.Fatalf("BuildTransform(flat_map): %v", err)
	}
	flatMap := transform.(*FlatMapTransform)
	defer flatMap.Close()

	in := core.Record{
		Operation: core.OpInsert,
		Data: map[string]any{
			"id": "order-1",
			"items": []any{
				map[string]any{"sku": "A", "qty": 2},
				map[string]any{"sku": "B", "qty": 3},
			},
		},
		Metadata: core.Metadata{Source: "kafka", Table: "orders", Offset: 42},
	}
	out, err := flatMap.ApplyBatch(context.Background(), []core.Record{in})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("outputs = %d, want 2: %#v", len(out), out)
	}
	if out[0].Data["order_id"] != "order-1" || out[0].Data["sku"] != "A" || out[0].Data["qty"] != float64(2) {
		t.Fatalf("first output data = %#v", out[0].Data)
	}
	if out[1].Data["order_id"] != "order-1" || out[1].Data["sku"] != "B" || out[1].Data["qty"] != float64(3) {
		t.Fatalf("second output data = %#v", out[1].Data)
	}
	if out[0].Operation != core.OpInsert || out[0].Metadata.Source != "kafka" || out[0].Metadata.Table != "order_items" || out[0].Metadata.Offset != 42 {
		t.Fatalf("first output envelope = %#v", out[0])
	}

	metrics := flatMap.TransformMetrics().Counters
	if metrics["input_records"] != 1 || metrics["output_records"] != 2 || metrics["dropped_records"] != 0 || metrics["parse_errors"] != 0 {
		t.Fatalf("metrics = %#v", metrics)
	}
}

func TestFlatMapTransformReportsPartialParseFailures(t *testing.T) {
	transform, err := NewFlatMapTransform("flat_map", map[string]any{
		"script": `
if record.data.bad then
  error("bad payload")
end
return { data = { id = record.data.id } }
`,
	})
	if err != nil {
		t.Fatalf("NewFlatMapTransform: %v", err)
	}
	defer transform.Close()

	out, err := transform.ApplyBatch(context.Background(), []core.Record{
		{Data: map[string]any{"id": 1}},
		{Data: map[string]any{"id": 2, "bad": true}},
	})
	if len(out) != 1 || out[0].Data["id"] != float64(1) {
		t.Fatalf("outputs = %#v, want one survivor id=1", out)
	}
	var partial core.PartialTransformError
	if !errors.As(err, &partial) {
		t.Fatalf("ApplyBatch error = %T %v, want core.PartialTransformError", err, err)
	}
	failures := partial.FailedRecords()
	if len(failures) != 1 || failures[0].Record.Data["id"] != 2 {
		t.Fatalf("failures = %#v, want input id=2", failures)
	}
	var classified core.ClassifiedError
	if !errors.As(failures[0].Err, &classified) || classified.Class != core.ErrorClassData {
		t.Fatalf("failure error = %T %v, want data-classified error", failures[0].Err, failures[0].Err)
	}

	metrics := transform.TransformMetrics().Counters
	if metrics["input_records"] != 2 || metrics["output_records"] != 1 || metrics["parse_errors"] != 1 {
		t.Fatalf("metrics = %#v", metrics)
	}
}

func TestFlatMapTransformDropsNilResults(t *testing.T) {
	transform, err := NewFlatMapTransform("flat_map", map[string]any{
		"script": "return nil",
	})
	if err != nil {
		t.Fatalf("NewFlatMapTransform: %v", err)
	}
	defer transform.Close()

	out, err := transform.ApplyBatch(context.Background(), []core.Record{{Data: map[string]any{"id": 1}}})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("outputs = %#v, want none", out)
	}
	metrics := transform.TransformMetrics().Counters
	if metrics["input_records"] != 1 || metrics["output_records"] != 0 || metrics["dropped_records"] != 1 || metrics["parse_errors"] != 0 {
		t.Fatalf("metrics = %#v", metrics)
	}
}

func TestFlatMapTransformReinitializesAfterClose(t *testing.T) {
	transform, err := NewFlatMapTransform("flat_map", map[string]any{
		"script": "return { data = { id = record.data.id, ok = true } }",
	})
	if err != nil {
		t.Fatalf("NewFlatMapTransform: %v", err)
	}
	if err := transform.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	defer transform.Close()

	out, err := transform.ApplyBatch(context.Background(), []core.Record{{Data: map[string]any{"id": 7}}})
	if err != nil {
		t.Fatalf("ApplyBatch after Close: %v", err)
	}
	if len(out) != 1 || out[0].Data["id"] != float64(7) || out[0].Data["ok"] != true {
		t.Fatalf("outputs after Close = %#v", out)
	}
}

func TestUDTFAliasBuildsFlatMapTransform(t *testing.T) {
	transform, err := registry.BuildTransform("udtf", map[string]any{
		"script": "return { data = { ok = true } }",
	})
	if err != nil {
		t.Fatalf("BuildTransform(udtf): %v", err)
	}
	defer transform.(*FlatMapTransform).Close()

	if transform.Name() != "udtf" {
		t.Fatalf("Name() = %q, want udtf", transform.Name())
	}
	out, err := transform.(core.BatchTransform).ApplyBatch(context.Background(), []core.Record{{Data: map[string]any{"id": 1}}})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if len(out) != 1 || out[0].Data["ok"] != true {
		t.Fatalf("outputs = %#v", out)
	}
}

func TestFlatMapTransformParsesGB32960Fixture(t *testing.T) {
	fixture := gb32960FixtureByName(t, "vehicle_realtime_two_metrics")
	transform, err := NewFlatMapTransform("flat_map", map[string]any{
		"script": gb32960LuaParserScript,
	})
	if err != nil {
		t.Fatalf("NewFlatMapTransform: %v", err)
	}
	defer transform.Close()

	out, err := transform.ApplyBatch(context.Background(), []core.Record{{
		Operation: core.OpInsert,
		Data:      map[string]any{"payload_hex": fixture.PayloadHex},
		Metadata:  core.Metadata{Source: "kafka", Table: "gb32960-raw", Offset: 9},
	}})
	if err != nil {
		var partial core.PartialTransformError
		if errors.As(err, &partial) {
			t.Fatalf("ApplyBatch partial failures: %#v", partial.FailedRecords())
		}
		t.Fatalf("ApplyBatch: %v", err)
	}
	if len(out) != len(fixture.Expected) {
		t.Fatalf("outputs = %d, want %d: %#v", len(out), len(fixture.Expected), out)
	}
	for i, exp := range fixture.Expected {
		got := out[i]
		if got.Operation != core.OpInsert || got.Metadata.Source != "kafka" || got.Metadata.Table != "gb32960_vehicle_ods" {
			t.Fatalf("output[%d] envelope = %#v", i, got)
		}
		if got.Metadata.Key == "" {
			t.Fatalf("output[%d] metadata key is empty", i)
		}
		if got.Data["vin"] != exp.VIN || got.Data["event_time"] != exp.EventTime || got.Data["dt"] != exp.DT || got.Data["metric_type"] != exp.MetricType {
			t.Fatalf("output[%d] data = %#v, want %#v", i, got.Data, exp)
		}
		if !almostEqual(toTestFloat(t, got.Data["metric_value"]), exp.MetricValue) {
			t.Fatalf("output[%d] metric_value = %#v, want %v", i, got.Data["metric_value"], exp.MetricValue)
		}
	}

	metrics := transform.TransformMetrics().Counters
	if metrics["input_records"] != 1 || metrics["output_records"] != int64(len(fixture.Expected)) || metrics["parse_errors"] != 0 {
		t.Fatalf("metrics = %#v", metrics)
	}
}

func TestFlatMapTransformGB32960ChecksumFailureIsDataClassified(t *testing.T) {
	fixture := gb32960FixtureByName(t, "invalid_checksum")
	transform, err := NewFlatMapTransform("flat_map", map[string]any{
		"script": gb32960LuaParserScript,
	})
	if err != nil {
		t.Fatalf("NewFlatMapTransform: %v", err)
	}
	defer transform.Close()

	out, err := transform.ApplyBatch(context.Background(), []core.Record{{Data: map[string]any{"payload_hex": fixture.PayloadHex}}})
	if len(out) != 0 {
		t.Fatalf("outputs = %#v, want none", out)
	}
	var partial core.PartialTransformError
	if !errors.As(err, &partial) {
		t.Fatalf("ApplyBatch error = %T %v, want partial transform error", err, err)
	}
	failures := partial.FailedRecords()
	if len(failures) != 1 || !strings.Contains(failures[0].Err.Error(), fixture.WantError) {
		t.Fatalf("failures = %#v, want %q", failures, fixture.WantError)
	}
	var classified core.ClassifiedError
	if !errors.As(failures[0].Err, &classified) || classified.Class != core.ErrorClassData {
		t.Fatalf("failure error = %T %v, want data-classified error", failures[0].Err, failures[0].Err)
	}
}

type gb32960Fixture struct {
	Name       string                  `json:"name"`
	PayloadHex string                  `json:"payload_hex"`
	Expected   []gb32960ExpectedMetric `json:"expected"`
	WantError  string                  `json:"want_error"`
}

type gb32960ExpectedMetric struct {
	VIN         string  `json:"vin"`
	EventTime   string  `json:"event_time"`
	DT          string  `json:"dt"`
	MetricType  string  `json:"metric_type"`
	MetricValue float64 `json:"metric_value"`
}

func gb32960FixtureByName(t *testing.T, name string) gb32960Fixture {
	t.Helper()
	path := filepath.Join("..", "..", "..", "testdata", "gb32960", "realtime-vehicle.jsonl")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open GB32960 fixture: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var fixture gb32960Fixture
		if err := json.Unmarshal(scanner.Bytes(), &fixture); err != nil {
			t.Fatalf("decode GB32960 fixture: %v", err)
		}
		if fixture.Name == name {
			return fixture
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan GB32960 fixture: %v", err)
	}
	t.Fatalf("GB32960 fixture %q not found", name)
	return gb32960Fixture{}
}

func toTestFloat(t *testing.T, v any) float64 {
	t.Helper()
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		t.Fatalf("value %T=%#v is not numeric", v, v)
		return 0
	}
}

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) < 0.000001
}

const gb32960LuaParserScript = `
local hex = data.payload_hex
if hex == nil or hex == "" then
  error("gb32960: missing payload_hex")
end

local bytes = {}
for i = 1, #hex, 2 do
  local byte = tonumber(string.sub(hex, i, i + 1), 16)
  if byte == nil then
    error("gb32960: invalid hex payload")
  end
  bytes[#bytes + 1] = byte
end

local function u16(pos)
  return bytes[pos] * 256 + bytes[pos + 1]
end

local function u32(pos)
  return ((bytes[pos] * 256 + bytes[pos + 1]) * 256 + bytes[pos + 2]) * 256 + bytes[pos + 3]
end

local function ascii(pos, len)
  local out = {}
  for i = pos, pos + len - 1 do
    out[#out + 1] = string.char(bytes[i])
  end
  return table.concat(out)
end

local function bxor(a, b)
  local out = 0
  local bit = 1
  while a > 0 or b > 0 do
    local abit = a % 2
    local bbit = b % 2
    if abit ~= bbit then
      out = out + bit
    end
    a = math.floor(a / 2)
    b = math.floor(b / 2)
    bit = bit * 2
  end
  return out
end

if #bytes < 26 or bytes[1] ~= 0x23 or bytes[2] ~= 0x23 then
  error("gb32960: invalid frame header")
end

local command = bytes[3]
local response_flag = bytes[4]
local vin = ascii(5, 17)
local encryption = bytes[22]
local data_len = u16(23)
local body_start = 25
local checksum_pos = body_start + data_len
if checksum_pos ~= #bytes then
  error("gb32960: data length mismatch")
end

local checksum = 0
for i = 3, checksum_pos - 1 do
  checksum = bxor(checksum, bytes[i])
end
if checksum ~= bytes[checksum_pos] then
  error("gb32960 checksum mismatch")
end

local year = 2000 + bytes[body_start]
local month = bytes[body_start + 1]
local day = bytes[body_start + 2]
local hour = bytes[body_start + 3]
local minute = bytes[body_start + 4]
local second = bytes[body_start + 5]
local event_time = string.format("%04d-%02d-%02dT%02d:%02d:%02dZ", year, month, day, hour, minute, second)
local dt = string.sub(event_time, 1, 10)

local info_type = bytes[body_start + 6]
if info_type ~= 0x01 then
  error("gb32960: first information block is not vehicle data")
end

local vehicle_data = body_start + 7
local speed_kmh = u16(vehicle_data + 2) / 10
local soc_percent = bytes[vehicle_data + 12]

local function metric(name, value)
  local event_id = vin .. ":" .. event_time .. ":" .. name
  return {
    operation = "INSERT",
    data = {
      event_id = event_id,
      vin = vin,
      event_time = event_time,
      dt = dt,
      metric_type = name,
      metric_value = value,
      gb32960_command = command,
      response_flag = response_flag,
      encryption = encryption
    },
    metadata = {
      table = "gb32960_vehicle_ods",
      key = event_id
    }
  }
end

return {
  metric("speed_kmh", speed_kmh),
  metric("soc_percent", soc_percent)
}
`
