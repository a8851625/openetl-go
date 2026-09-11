package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type fixtureRow struct {
	ID      int64  `json:"id"`
	Group   int64  `json:"grp"`
	Amount  int64  `json:"amount"`
	Payload string `json:"payload"`
}

func expectedRow(id int) fixtureRow {
	return fixtureRow{int64(id), int64(id % 97), int64(id * 7), fmt.Sprintf("row-%09d-%s", id, strings.Repeat("x", 48))}
}

func rowBytes(r fixtureRow) []byte {
	b, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return append(b, '\n')
}

type producerResult struct {
	Rows           int           `json:"rows"`
	SHA256         string        `json:"canonical_sha256"`
	StartNS        int64         `json:"start_unix_ns"`
	EndNS          int64         `json:"end_unix_ns"`
	ElapsedSeconds float64       `json:"elapsed_seconds"`
	FirstOffset    int64         `json:"first_offset,omitempty"`
	LastOffset     int64         `json:"last_offset,omitempty"`
	Acks           []producerAck `json:"acks"`
}

type producerAck struct {
	Rows   int   `json:"rows"`
	TimeNS int64 `json:"time_unix_ns"`
}

// Pace at the publisher. The measured engine's batching/defaults are not changed.
func pace(start time.Time, rows, rate int) {
	if rate > 0 {
		due := start.Add(time.Duration(float64(rows) / float64(rate) * float64(time.Second)))
		if delay := time.Until(due); delay > 0 {
			time.Sleep(delay)
		}
	}
}

func publishKafka(o options) (producerResult, error) {
	if o.topic == "" {
		return producerResult{}, errors.New("missing topic")
	}
	cfg := sarama.NewConfig()
	// Match the built-in Kafka source/sink contract and the pinned Redpanda
	// fixture. v2.8 asks for Metadata v10, unsupported by this fixture image.
	cfg.Version = sarama.V2_1_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Partitioner = sarama.NewManualPartitioner
	producer, err := sarama.NewSyncProducer(strings.Split(o.brokers, ","), cfg)
	if err != nil {
		return producerResult{}, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = producer.Close()
		}
	}()
	start := time.Now()
	result := producerResult{StartNS: start.UnixNano(), FirstOffset: 0}
	h := sha256.New()
	batch := 1000
	if o.rate > 0 {
		batch = max(1, o.rate/20)
	}
	for first := 1; first <= o.rows; first += batch {
		last := min(first+batch-1, o.rows)
		pace(start, first-1, o.rate)
		messages := make([]*sarama.ProducerMessage, 0, last-first+1)
		for id := first; id <= last; id++ {
			b := rowBytes(expectedRow(id))
			h.Write(b)
			messages = append(messages, &sarama.ProducerMessage{Topic: o.topic, Partition: 0,
				Key: sarama.StringEncoder(fmt.Sprint(id)), Value: sarama.ByteEncoder(b[:len(b)-1])})
		}
		if err := producer.SendMessages(messages); err != nil {
			return result, err
		}
		for i, message := range messages {
			if message.Partition != 0 || message.Offset != int64(first+i-1) {
				return result, fmt.Errorf("publisher offset mismatch at row %d", first+i)
			}
		}
		result.Rows = last
		result.Acks = append(result.Acks, producerAck{last, time.Now().UnixNano()})
	}
	err = producer.Close()
	closed = true
	if err != nil {
		return result, err
	}
	result.EndNS = time.Now().UnixNano()
	result.ElapsedSeconds = time.Since(start).Seconds()
	result.LastOffset = int64(result.Rows - 1)
	result.SHA256 = hex.EncodeToString(h.Sum(nil))
	return result, nil
}

var identifier = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func fixtureDB() (*sql.DB, error) {
	db, err := sql.Open("mysql", os.Getenv("CAPACITY_MYSQL_DSN"))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func seedMySQL(o options) (producerResult, error) {
	if !identifier.MatchString(o.table) {
		return producerResult{}, errors.New("invalid fixture table")
	}
	db, err := fixtureDB()
	if err != nil {
		return producerResult{}, err
	}
	defer db.Close()
	start := time.Now()
	r := producerResult{StartNS: start.UnixNano()}
	h := sha256.New()
	for first := 1; first <= o.rows; first += 1000 {
		pace(start, first-1, o.rate)
		last := min(first+999, o.rows)
		values := make([]string, 0, last-first+1)
		args := make([]any, 0, (last-first+1)*4)
		for id := first; id <= last; id++ {
			row := expectedRow(id)
			h.Write(rowBytes(row))
			values = append(values, "(?,?,?,?)")
			args = append(args, row.ID, row.Group, row.Amount, row.Payload)
		}
		_, err := db.Exec("INSERT INTO `"+o.table+"` (id,grp,amount,payload) VALUES "+strings.Join(values, ","), args...)
		if err != nil {
			return r, err
		}
		r.Rows = last
		r.Acks = append(r.Acks, producerAck{last, time.Now().UnixNano()})
	}
	r.EndNS, r.ElapsedSeconds, r.SHA256 = time.Now().UnixNano(), time.Since(start).Seconds(), hex.EncodeToString(h.Sum(nil))
	return r, nil
}

type verifier struct {
	seen                 []bool
	rows, maxID, objects int
	bytes                int64
}

func newVerifier(rows int) *verifier { return &verifier{seen: make([]bool, rows+1)} }

func (v *verifier) add(row fixtureRow) error {
	if row.ID <= 0 || row.ID >= int64(len(v.seen)) {
		return errors.New("target has an out-of-range business key")
	}
	id := int(row.ID)
	if row != expectedRow(id) {
		return fmt.Errorf("target content differs at business key %d", id)
	}
	if v.seen[id] {
		return fmt.Errorf("duplicate business key %d in target", id)
	}
	v.seen[id], v.rows, v.maxID = true, v.rows+1, max(id, v.maxID)
	return nil
}

func (v *verifier) read(r io.Reader) error {
	v.objects++
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 4096), 1<<20)
	for s.Scan() {
		var row fixtureRow
		line := s.Text()
		dec := json.NewDecoder(strings.NewReader(line))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&row); err != nil {
			return err
		}
		var extra any
		if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
			return errors.New("trailing JSON in target row")
		}
		if err := v.add(row); err != nil {
			return err
		}
		v.bytes += int64(len(line) + 1)
	}
	return s.Err()
}

type verification struct {
	Rows             int    `json:"rows"`
	ExpectedRows     int    `json:"expected_rows"`
	CommittedMinimum int    `json:"committed_minimum"`
	Full             bool   `json:"full"`
	SHA256           string `json:"canonical_sha256"`
	Objects          int    `json:"objects"`
	Bytes            int64  `json:"bytes"`
	Method           string `json:"method"`
}

func hashPrefix(h hash.Hash, last int) string {
	for id := 1; id <= last; id++ {
		h.Write(rowBytes(expectedRow(id)))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (v *verifier) finish(minimum int) (verification, error) {
	expected := len(v.seen) - 1
	if minimum < 0 {
		minimum = expected
	}
	// Every field was compared to independently generated input. Hashing that
	// canonical sequence is valid only after proving a unique, gap-free prefix.
	if v.rows < minimum || v.rows != v.maxID {
		return verification{}, fmt.Errorf("target has missing rows before its committed prefix: actual=%d max_id=%d minimum=%d expected=%d", v.rows, v.maxID, minimum, expected)
	}
	return verification{Rows: v.rows, ExpectedRows: expected, CommittedMinimum: minimum,
		Full: v.rows == expected, SHA256: hashPrefix(sha256.New(), v.rows), Objects: v.objects, Bytes: v.bytes,
		Method: "every field vs deterministic input; unique gap-free business-key prefix"}, nil
}

func verifyMySQL(o options) (verification, error) {
	if !identifier.MatchString(o.table) || !identifier.MatchString(o.database) {
		return verification{}, errors.New("invalid table")
	}
	db, err := fixtureDB()
	if err != nil {
		return verification{}, err
	}
	defer db.Close()
	rows, err := db.Query("SELECT id,grp,amount,payload FROM `" + o.database + "`.`" + o.table + "` ORDER BY id")
	if err != nil {
		return verification{}, err
	}
	defer rows.Close()
	v := newVerifier(o.rows)
	for rows.Next() {
		var row fixtureRow
		if err := rows.Scan(&row.ID, &row.Group, &row.Amount, &row.Payload); err != nil {
			return verification{}, err
		}
		if err := v.add(row); err != nil {
			return verification{}, err
		}
	}
	if err := rows.Err(); err != nil {
		return verification{}, err
	}
	return v.finish(o.minimum)
}

func verifyClickHouse(o options) (verification, error) {
	if !identifier.MatchString(o.table) {
		return verification{}, errors.New("invalid table")
	}
	u, err := url.Parse(os.Getenv("CAPACITY_CLICKHOUSE_URL"))
	if err != nil {
		return verification{}, err
	}
	q := u.Query()
	q.Set("query", "SELECT id,grp,amount,payload FROM capacity."+o.table+" ORDER BY id FORMAT JSONEachRow")
	q.Set("output_format_json_quote_64bit_integers", "0")
	u.RawQuery = q.Encode()
	req, err := http.NewRequest(http.MethodPost, u.String(), nil)
	if err != nil {
		return verification{}, err
	}
	req.SetBasicAuth("capacity", os.Getenv("CAPACITY_PASSWORD"))
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return verification{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return verification{}, fmt.Errorf("ClickHouse verification HTTP %d", resp.StatusCode)
	}
	v := newVerifier(o.rows)
	if err := v.read(resp.Body); err != nil {
		return verification{}, err
	}
	return v.finish(o.minimum)
}

func verifyFiles(o options) (verification, error) {
	// A single caller-selected owned output directory, including date subdirs.
	v := newVerifier(o.rows)
	err := filepath.WalkDir(filepath.Join(o.dir, "output", o.table), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".jsonl" {
			return fmt.Errorf("unexpected incomplete/foreign sink file %s", filepath.Base(path))
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		err = v.read(f)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		return closeErr
	})
	if err != nil {
		return verification{}, err
	}
	return v.finish(o.minimum)
}

func verifyS3(o options) (verification, error) {
	client, err := minio.New("objects:9000", &minio.Options{Region: "us-east-1",
		Creds: credentials.NewStaticV4("capacity", os.Getenv("CAPACITY_PASSWORD"), ""), Secure: false})
	if err != nil {
		return verification{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	v := newVerifier(o.rows)
	for object := range client.ListObjects(ctx, "capacity", minio.ListObjectsOptions{Prefix: o.table + "/", Recursive: true}) {
		if object.Err != nil {
			return verification{}, object.Err
		}
		if !strings.HasSuffix(object.Key, ".jsonl") {
			return verification{}, errors.New("unexpected object type")
		}
		reader, err := client.GetObject(ctx, "capacity", object.Key, minio.GetObjectOptions{})
		if err != nil {
			return verification{}, err
		}
		err = v.read(reader)
		closeErr := reader.Close()
		if err != nil {
			return verification{}, err
		}
		if closeErr != nil {
			return verification{}, closeErr
		}
	}
	return v.finish(o.minimum)
}
