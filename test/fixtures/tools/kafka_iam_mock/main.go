package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const oauthBearerMechanism = "OAUTHBEARER"

type config struct {
	brokerPort      int
	httpPort        int
	topic           string
	partitions      int
	awsRegion       string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
}

type authEvent struct {
	Timestamp time.Time `json:"timestamp"`
	Stage     string    `json:"stage"`
	Mechanism string    `json:"mechanism,omitempty"`
	Success   bool      `json:"success"`
	Reason    string    `json:"reason,omitempty"`
	Token     string    `json:"token,omitempty"`
	Decoded   string    `json:"decoded,omitempty"`
}

type seedRequest struct {
	Topic    string   `json:"topic"`
	Messages []string `json:"messages"`
}

type authEventsResponse struct {
	HadSuccess   bool        `json:"had_success"`
	SuccessCount int         `json:"success_count"`
	FailureCount int         `json:"failure_count"`
	Events       []authEvent `json:"events"`
}

type mockServer struct {
	cfg     config
	cluster *kfake.Cluster
	broker  string

	mu          sync.Mutex
	hadSuccess  bool
	authHistory []authEvent
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := parseFlags()
	if err := validateConfig(cfg); err != nil {
		return err
	}
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.Ports(cfg.brokerPort),
		kfake.SeedTopics(int32(cfg.partitions), cfg.topic),
		kfake.WithLogger(kfake.BasicLogger(os.Stderr, kfake.LogLevelNone)),
	)
	if err != nil {
		return fmt.Errorf("failed to create kfake cluster: %w", err)
	}
	defer cluster.Close()
	addrs := cluster.ListenAddrs()
	if len(addrs) != 1 {
		return fmt.Errorf("expected one broker address, got %d", len(addrs))
	}
	server := &mockServer{
		cfg:     cfg,
		cluster: cluster,
		broker:  addrs[0],
	}
	server.installControls()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.handleHealth)
	mux.HandleFunc("/seed", server.handleSeed)
	mux.HandleFunc("/auth_events", server.handleAuthEvents)
	httpServer := &http.Server{
		Addr:              fmt.Sprintf("127.0.0.1:%d", cfg.httpPort),
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}
	log.Printf("broker listening at %s", server.broker)
	log.Printf("control plane listening at %s", httpServer.Addr)
	errCh := make(chan error, 1)
	go func() {
		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errCh:
		return fmt.Errorf("helper http server failed: %w", err)
	case sig := <-sigCh:
		log.Printf("received signal %s, shutting down", sig)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return httpServer.Shutdown(ctx)
}

func parseFlags() config {
	cfg := config{}
	flag.IntVar(&cfg.brokerPort, "broker-port", 0, "broker listen port")
	flag.IntVar(&cfg.httpPort, "http-port", 0, "control-plane HTTP listen port")
	flag.StringVar(&cfg.topic, "topic", "tenzir_iam_test", "default topic name")
	flag.IntVar(&cfg.partitions, "partitions", 1, "number of topic partitions")
	flag.StringVar(&cfg.awsRegion, "aws-region", "us-east-1", "expected AWS region")
	flag.StringVar(&cfg.accessKeyID, "access-key-id", "AKIA_TEST_ACCESS_KEY", "expected access key id")
	flag.StringVar(&cfg.secretAccessKey, "secret-access-key", "test-secret-key", "expected secret access key (unused, for traceability)")
	flag.StringVar(&cfg.sessionToken, "session-token", "", "expected session token (unused, for traceability)")
	flag.Parse()
	return cfg
}

func validateConfig(cfg config) error {
	switch {
	case cfg.brokerPort <= 0:
		return errors.New("`--broker-port` must be > 0")
	case cfg.httpPort <= 0:
		return errors.New("`--http-port` must be > 0")
	case cfg.partitions <= 0:
		return errors.New("`--partitions` must be > 0")
	case cfg.topic == "":
		return errors.New("`--topic` must not be empty")
	case cfg.awsRegion == "":
		return errors.New("`--aws-region` must not be empty")
	case cfg.accessKeyID == "":
		return errors.New("`--access-key-id` must not be empty")
	}
	return nil
}

func (s *mockServer) installControls() {
	s.cluster.ControlKey(int16(kmsg.SASLHandshake), func(req kmsg.Request) (kmsg.Response, error, bool) {
		s.cluster.KeepControl()
		handshakeReq, ok := req.(*kmsg.SASLHandshakeRequest)
		if !ok {
			return nil, fmt.Errorf("unexpected request type for SASL handshake: %T", req), true
		}
		resp := handshakeReq.ResponseKind().(*kmsg.SASLHandshakeResponse)
		if handshakeReq.Mechanism != oauthBearerMechanism {
			reason := fmt.Sprintf("unsupported SASL mechanism `%s`", handshakeReq.Mechanism)
			resp.ErrorCode = kerr.UnsupportedSaslMechanism.Code
			resp.SupportedMechanisms = []string{oauthBearerMechanism}
			msg := reason
			s.recordAuthEvent(authEvent{
				Timestamp: time.Now().UTC(),
				Stage:     "handshake",
				Mechanism: handshakeReq.Mechanism,
				Success:   false,
				Reason:    msg,
			})
			return resp, nil, true
		}
		s.recordAuthEvent(authEvent{
			Timestamp: time.Now().UTC(),
			Stage:     "handshake",
			Mechanism: handshakeReq.Mechanism,
			Success:   true,
		})
		return resp, nil, true
	})
	s.cluster.ControlKey(int16(kmsg.SASLAuthenticate), func(req kmsg.Request) (kmsg.Response, error, bool) {
		s.cluster.KeepControl()
		authReq, ok := req.(*kmsg.SASLAuthenticateRequest)
		if !ok {
			return nil, fmt.Errorf("unexpected request type for SASL authenticate: %T", req), true
		}
		resp := authReq.ResponseKind().(*kmsg.SASLAuthenticateResponse)
		token, err := extractBearerToken(authReq.SASLAuthBytes)
		if err != nil {
			return s.rejectAuthentication(resp, "", "", fmt.Sprintf("invalid SASL payload: %s", err))
		}
		decodedURI, err := s.validateToken(token)
		if err != nil {
			return s.rejectAuthentication(resp, token, "", err.Error())
		}
		s.recordAuthEvent(authEvent{
			Timestamp: time.Now().UTC(),
			Stage:     "authenticate",
			Mechanism: oauthBearerMechanism,
			Success:   true,
			Token:     token,
			Decoded:   decodedURI,
		})
		return resp, nil, true
	})
}

func (s *mockServer) rejectAuthentication(resp *kmsg.SASLAuthenticateResponse, token string, decoded string, reason string) (kmsg.Response, error, bool) {
	resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
	msg := reason
	resp.ErrorMessage = &msg
	s.recordAuthEvent(authEvent{
		Timestamp: time.Now().UTC(),
		Stage:     "authenticate",
		Mechanism: oauthBearerMechanism,
		Success:   false,
		Reason:    msg,
		Token:     token,
		Decoded:   decoded,
	})
	return resp, nil, true
}

func (s *mockServer) validateToken(token string) (string, error) {
	decoded, err := decodeURLSafeToken(token)
	if err != nil {
		return "", fmt.Errorf("failed to decode token: %w", err)
	}
	parsed, err := url.Parse(decoded)
	if err != nil {
		return decoded, fmt.Errorf("failed to parse decoded token URI: %w", err)
	}
	expectedHost := fmt.Sprintf("kafka.%s.amazonaws.com", s.cfg.awsRegion)
	if parsed.Host != expectedHost {
		return decoded, fmt.Errorf("unexpected host `%s`, expected `%s`", parsed.Host, expectedHost)
	}
	query := parsed.Query()
	if query.Get("Action") != "kafka-cluster:Connect" {
		return decoded, errors.New("missing or invalid `Action=kafka-cluster:Connect`")
	}
	if query.Get("User-Agent") != "Tenzir" {
		return decoded, errors.New("missing or invalid `User-Agent=Tenzir`")
	}
	if query.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
		return decoded, errors.New("missing or invalid `X-Amz-Algorithm=AWS4-HMAC-SHA256`")
	}
	credential := query.Get("X-Amz-Credential")
	if credential == "" {
		return decoded, errors.New("missing `X-Amz-Credential`")
	}
	if !strings.Contains(credential, s.cfg.accessKeyID) {
		return decoded, fmt.Errorf("credential `%s` does not contain expected access key id", credential)
	}
	return decoded, nil
}

func extractBearerToken(payload []byte) (string, error) {
	fields := strings.Split(string(payload), "\x01")
	for _, field := range fields {
		if !strings.HasPrefix(field, "auth=Bearer ") {
			continue
		}
		token := strings.TrimSpace(strings.TrimPrefix(field, "auth=Bearer "))
		if token == "" {
			return "", errors.New("empty bearer token")
		}
		return token, nil
	}
	return "", errors.New("missing `auth=Bearer ...` field")
}

func decodeURLSafeToken(token string) (string, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err == nil {
		return string(decoded), nil
	}
	decoded, stdErr := base64.URLEncoding.DecodeString(token)
	if stdErr == nil {
		return string(decoded), nil
	}
	return "", err
}

func (s *mockServer) recordAuthEvent(event authEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if event.Success && event.Stage == "authenticate" {
		s.hadSuccess = true
	}
	s.authHistory = append(s.authHistory, event)
}

func (s *mockServer) snapshotAuthEvents() authEventsResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]authEvent, len(s.authHistory))
	copy(events, s.authHistory)
	successCount := 0
	failureCount := 0
	for _, event := range events {
		if event.Success {
			successCount++
		} else {
			failureCount++
		}
	}
	return authEventsResponse{
		HadSuccess:   s.hadSuccess,
		SuccessCount: successCount,
		FailureCount: failureCount,
		Events:       events,
	}
}

func (s *mockServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"broker": s.broker,
		"topic":  s.cfg.topic,
	})
}

func (s *mockServer) handleSeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
			"error": "method not allowed",
		})
		return
	}
	req := seedRequest{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("failed to decode request body: %s", err),
		})
		return
	}
	topic := req.Topic
	if topic == "" {
		topic = s.cfg.topic
	}
	if len(req.Messages) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "`messages` must not be empty",
		})
		return
	}
	if err := s.produceMessages(topic, req.Messages); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("failed to seed topic: %s", err),
		})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *mockServer) handleAuthEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
			"error": "method not allowed",
		})
		return
	}
	writeJSON(w, http.StatusOK, s.snapshotAuthEvents())
}

func (s *mockServer) produceMessages(topic string, messages []string) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(s.broker),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		return fmt.Errorf("failed to create producer client: %w", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, message := range messages {
		record := &kgo.Record{
			Value: []byte(message),
		}
		if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
			return err
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
