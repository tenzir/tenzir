package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestOffsets(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "events"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	server := &mockServer{broker: cluster.ListenAddrs()[0]}
	check := func(want string) {
		t.Helper()
		recorder := httptest.NewRecorder()
		server.handleOffsets(recorder, httptest.NewRequest(http.MethodGet, "/offsets?group=test&topic=events", nil))
		if recorder.Code != http.StatusOK || recorder.Body.String() != want {
			t.Fatalf("got status %d, body %q; want %q", recorder.Code, recorder.Body.String(), want)
		}
	}
	check("{\"offset\":null}\n")
	client, err := kgo.NewClient(kgo.SeedBrokers(server.broker))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request := kmsg.NewPtrOffsetCommitRequest()
	request.Group = "test"
	partition := kmsg.NewOffsetCommitRequestTopicPartition()
	partition.Partition = 0
	partition.Offset = 11
	request.Topics = []kmsg.OffsetCommitRequestTopic{{
		Topic: "events", Partitions: []kmsg.OffsetCommitRequestTopicPartition{partition},
	}}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Topics) != 1 || len(response.Topics[0].Partitions) != 1 || response.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("commit failed: %+v", response)
	}
	check("{\"offset\":11}\n")
}

func TestOffsetsInvalidRequest(t *testing.T) {
	server := &mockServer{}
	for _, test := range []struct {
		method string
		url    string
		status int
	}{
		{http.MethodPost, "/offsets?group=test&topic=events", http.StatusMethodNotAllowed},
		{http.MethodGet, "/offsets?group=test", http.StatusBadRequest},
		{http.MethodGet, "/offsets?topic=events", http.StatusBadRequest},
	} {
		recorder := httptest.NewRecorder()
		server.handleOffsets(recorder, httptest.NewRequest(test.method, test.url, nil))
		if recorder.Code != test.status {
			t.Errorf("%s %s: got status %d, want %d", test.method, test.url, recorder.Code, test.status)
		}
	}
}
