// A mock Amazon Kinesis data-plane server for benchmarking Tenzir's
// from_amazon_kinesis and to_amazon_kinesis operators.
//
// The server replays a pre-loaded NDJSON dataset through the Kinesis JSON
// protocol (X-Amz-Target dispatch, application/x-amz-json-1.1). It is
// stateless: shard iterators encode the read offset, so concurrent runs and
// replays need no coordination. Artificial latency and probabilistic
// throttling make the latency-bound and overload regimes reproducible.
//
// Throttling uses HTTP 400 with __type ProvisionedThroughputExceededException
// because Tenzir's HTTP pool transparently retries 429 and 5xx responses;
// only a 400 reaches the operator's own backoff path.
package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type server struct {
	stream       string
	shards       int
	latency      time.Duration
	throttle     float64
	maxRecords   int
	maxRecordKiB int
	// encoded[g] is the base64-encoded dataset line g; record j of shard s is
	// global line j*shards + s.
	encoded   []string
	arrival   string // epoch seconds as JSON double
	startTime time.Time

	getRecordsCalls  atomic.Int64
	getRecordsTotal  atomic.Int64
	putRecordsCalls  atomic.Int64
	putRecordsTotal  atomic.Int64
	putBytes         atomic.Int64
	throttledCalls   atomic.Int64
	throttledRecords atomic.Int64
}

func (s *server) shardCount(shard int) int {
	total := len(s.encoded)
	if shard >= total {
		return 0
	}
	return (total - shard + s.shards - 1) / s.shards
}

func shardID(index int) string {
	return fmt.Sprintf("shardId-%012d", index)
}

func parseShardID(id string) (int, error) {
	digits := strings.TrimPrefix(id, "shardId-")
	return strconv.Atoi(digits)
}

// Iterator tokens are "<shard>:<offset>".
func iterator(shard, offset int) string {
	return fmt.Sprintf("%d:%d", shard, offset)
}

func parseIterator(token string) (shard, offset int, err error) {
	colon := strings.IndexByte(token, ':')
	if colon < 0 {
		return 0, 0, fmt.Errorf("malformed iterator %q", token)
	}
	shard, err = strconv.Atoi(token[:colon])
	if err != nil {
		return 0, 0, err
	}
	offset, err = strconv.Atoi(token[colon+1:])
	return shard, offset, err
}

// Sequence numbers are the zero-padded per-shard offset so that
// AFTER_SEQUENCE_NUMBER recovery round-trips.
func sequenceNumber(offset int) string {
	return fmt.Sprintf("%020d", offset)
}

func awsError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"__type":%q,"message":%q}`, code, message)
}

func (s *server) throttled(w http.ResponseWriter) bool {
	if s.throttle > 0 && rand.Float64() < s.throttle {
		s.throttledCalls.Add(1)
		awsError(w, http.StatusBadRequest,
			"ProvisionedThroughputExceededException", "Rate exceeded")
		return true
	}
	return false
}

func writeJSON(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	io.WriteString(w, body)
}

func (s *server) handleListShards(w http.ResponseWriter, _ []byte) {
	var b strings.Builder
	b.WriteString(`{"Shards":[`)
	for i := 0; i < s.shards; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"ShardId":%q,"HashKeyRange":{"StartingHashKey":"0",`+
			`"EndingHashKey":"1"},"SequenceNumberRange":{"StartingSequenceNumber":%q}}`,
			shardID(i), sequenceNumber(0))
	}
	b.WriteString(`]}`)
	writeJSON(w, b.String())
}

func (s *server) handleDescribeStreamSummary(w http.ResponseWriter, _ []byte) {
	writeJSON(w, fmt.Sprintf(`{"StreamDescriptionSummary":{"StreamName":%q,`+
		`"StreamARN":"arn:aws:kinesis:us-east-1:000000000000:stream/%s",`+
		`"StreamStatus":"ACTIVE","RetentionPeriodHours":24,`+
		`"StreamCreationTimestamp":%s,"EnhancedMonitoring":[],`+
		`"OpenShardCount":%d,"MaxRecordSizeInKiB":%d}}`,
		s.stream, s.stream, s.arrival, s.shards, s.maxRecordKiB))
}

func (s *server) handleGetShardIterator(w http.ResponseWriter, body []byte) {
	var req struct {
		ShardId                string
		ShardIteratorType      string
		StartingSequenceNumber string
	}
	if err := json.Unmarshal(body, &req); err != nil {
		awsError(w, http.StatusBadRequest, "SerializationException", err.Error())
		return
	}
	shard, err := parseShardID(req.ShardId)
	if err != nil || shard < 0 || shard >= s.shards {
		awsError(w, http.StatusBadRequest, "ResourceNotFoundException",
			fmt.Sprintf("unknown shard %q", req.ShardId))
		return
	}
	offset := 0
	if req.ShardIteratorType == "AFTER_SEQUENCE_NUMBER" {
		seq, err := strconv.Atoi(req.StartingSequenceNumber)
		if err != nil {
			awsError(w, http.StatusBadRequest, "InvalidArgumentException",
				fmt.Sprintf("bad sequence number %q", req.StartingSequenceNumber))
			return
		}
		offset = seq + 1
	}
	writeJSON(w, fmt.Sprintf(`{"ShardIterator":%q}`, iterator(shard, offset)))
}

func (s *server) handleGetRecords(w http.ResponseWriter, body []byte) {
	if s.throttled(w) {
		return
	}
	var req struct {
		ShardIterator string
		Limit         int
	}
	if err := json.Unmarshal(body, &req); err != nil {
		awsError(w, http.StatusBadRequest, "SerializationException", err.Error())
		return
	}
	shard, offset, err := parseIterator(req.ShardIterator)
	if err != nil || shard < 0 || shard >= s.shards {
		awsError(w, http.StatusBadRequest, "InvalidArgumentException",
			fmt.Sprintf("bad iterator %q", req.ShardIterator))
		return
	}
	limit := s.maxRecords
	if req.Limit > 0 && req.Limit < limit {
		limit = req.Limit
	}
	count := s.shardCount(shard)
	n := count - offset
	if n < 0 {
		n = 0
	}
	if n > limit {
		n = limit
	}
	var b strings.Builder
	b.WriteString(`{"Records":[`)
	for j := 0; j < n; j++ {
		if j > 0 {
			b.WriteByte(',')
		}
		global := (offset+j)*s.shards + shard
		fmt.Fprintf(&b, `{"Data":%q,"SequenceNumber":%q,"PartitionKey":"bench-%d",`+
			`"ApproximateArrivalTimestamp":%s}`,
			s.encoded[global], sequenceNumber(offset+j), global, s.arrival)
	}
	b.WriteString(`],"MillisBehindLatest":0`)
	if offset+n < count {
		fmt.Fprintf(&b, `,"NextShardIterator":%q`, iterator(shard, offset+n))
	}
	b.WriteString(`}`)
	s.getRecordsCalls.Add(1)
	s.getRecordsTotal.Add(int64(n))
	writeJSON(w, b.String())
}

// base64DecodedLen returns the exact decoded size of a padded base64 string.
func base64DecodedLen(data string) int {
	padding := 0
	for i := len(data) - 1; i >= 0 && data[i] == '='; i-- {
		padding++
	}
	return len(data)/4*3 - padding
}

func (s *server) handlePutRecords(w http.ResponseWriter, body []byte) {
	if s.throttled(w) {
		return
	}
	var req struct {
		Records []struct {
			Data         string
			PartitionKey string
		}
	}
	if err := json.Unmarshal(body, &req); err != nil {
		awsError(w, http.StatusBadRequest, "SerializationException", err.Error())
		return
	}
	failed := 0
	var b strings.Builder
	b.WriteString(`[`)
	for i, record := range req.Records {
		if i > 0 {
			b.WriteByte(',')
		}
		if s.throttle > 0 && rand.Float64() < s.throttle {
			failed++
			s.throttledRecords.Add(1)
			b.WriteString(`{"ErrorCode":"ProvisionedThroughputExceededException",` +
				`"ErrorMessage":"Rate exceeded"}`)
			continue
		}
		s.putRecordsTotal.Add(1)
		s.putBytes.Add(int64(base64DecodedLen(record.Data)))
		fmt.Fprintf(&b, `{"SequenceNumber":%q,"ShardId":%q}`,
			sequenceNumber(i), shardID(0))
	}
	b.WriteString(`]`)
	s.putRecordsCalls.Add(1)
	writeJSON(w, fmt.Sprintf(`{"FailedRecordCount":%d,"Records":%s}`,
		failed, b.String()))
}

func (s *server) handleAPI(w http.ResponseWriter, r *http.Request) {
	if s.latency > 0 {
		time.Sleep(s.latency)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		awsError(w, http.StatusBadRequest, "SerializationException", err.Error())
		return
	}
	target := r.Header.Get("X-Amz-Target")
	action := target[strings.IndexByte(target, '.')+1:]
	switch action {
	case "ListShards":
		s.handleListShards(w, body)
	case "DescribeStreamSummary":
		s.handleDescribeStreamSummary(w, body)
	case "GetShardIterator":
		s.handleGetShardIterator(w, body)
	case "GetRecords":
		s.handleGetRecords(w, body)
	case "PutRecords":
		s.handlePutRecords(w, body)
	default:
		awsError(w, http.StatusBadRequest, "UnknownOperationException", target)
	}
}

func (s *server) handleStats(w http.ResponseWriter, _ *http.Request) {
	stats := map[string]int64{
		"get_records_calls": s.getRecordsCalls.Load(),
		"get_records":       s.getRecordsTotal.Load(),
		"put_records_calls": s.putRecordsCalls.Load(),
		"put_records":       s.putRecordsTotal.Load(),
		"put_bytes":         s.putBytes.Load(),
		"throttled_calls":   s.throttledCalls.Load(),
		"throttled_records": s.throttledRecords.Load(),
	}
	body, _ := json.Marshal(stats)
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

func loadDataset(path string) ([]string, error) {
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var encoded []string
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for scanner.Scan() {
		encoded = append(encoded,
			base64.StdEncoding.EncodeToString(scanner.Bytes()))
	}
	return encoded, scanner.Err()
}

func main() {
	port := flag.Int("port", 0, "listen port (0 = auto-assign)")
	shards := flag.Int("shards", 1, "number of shards")
	latency := flag.Duration("latency", 0, "artificial per-request latency")
	throttleRate := flag.Float64("throttle-rate", 0,
		"probability of throttling a call or record")
	maxRecords := flag.Int("max-records", 10000,
		"maximum records per GetRecords response")
	maxRecordKiB := flag.Int("max-record-size-kib", 1024,
		"stream record size limit reported by DescribeStreamSummary")
	dataset := flag.String("dataset", "", "NDJSON file to replay (one record per line)")
	stream := flag.String("stream", "tenzir-bench", "stream name")
	flag.Parse()
	encoded, err := loadDataset(*dataset)
	if err != nil {
		log.Fatalf("failed to load dataset: %v", err)
	}
	s := &server{
		stream:       *stream,
		shards:       *shards,
		latency:      *latency,
		throttle:     *throttleRate,
		maxRecords:   *maxRecords,
		maxRecordKiB: *maxRecordKiB,
		encoded:      encoded,
		startTime:    time.Now(),
	}
	s.arrival = fmt.Sprintf("%.3f", float64(s.startTime.UnixMilli())/1000.0)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /", s.handleAPI)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /stats", s.handleStats)
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	fmt.Printf("LISTENING %s\n", listener.Addr())
	os.Stdout.Sync()
	log.Fatal(http.Serve(listener, mux))
}
