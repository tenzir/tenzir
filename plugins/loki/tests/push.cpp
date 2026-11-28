//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "loki/push.pb.h"

#include <tenzir/detail/narrow.hpp>
#include <tenzir/test/test.hpp>

#include <arrow/util/compression.h>
#include <loki/push.hpp>

#include <chrono>
#include <cstdint>
#include <limits>
#include <simdjson.h>
#include <string>
#include <vector>

using namespace tenzir;
using namespace tenzir::plugins::loki;
using namespace std::chrono_literals;

namespace {

auto snappy_decompress(std::string_view compressed, size_t expected_size)
  -> std::string {
  auto codec_result = arrow::util::Codec::Create(
    arrow::Compression::type::SNAPPY,
    arrow::util::Codec::UseDefaultCompressionLevel());
  REQUIRE(codec_result.ok());
  auto codec = codec_result.MoveValueUnsafe();
  auto output = std::string{};
  output.resize(expected_size);
  auto decompressed
    = codec->Decompress(detail::narrow<int64_t>(compressed.size()),
                        reinterpret_cast<uint8_t const*>(compressed.data()),
                        detail::narrow<int64_t>(output.size()),
                        reinterpret_cast<uint8_t*>(output.data()));
  REQUIRE(decompressed.ok());
  output.resize(detail::narrow<size_t>(*decompressed));
  return output;
}

} // namespace

TEST("parse_encoding accepts protobuf and json only") {
  CHECK_EQUAL(parse_encoding("protobuf"), Option<Encoding>{Encoding::protobuf});
  CHECK_EQUAL(parse_encoding("json"), Option<Encoding>{Encoding::json});
  CHECK(not parse_encoding(""));
  CHECK(not parse_encoding("PROTOBUF"));
  CHECK(not parse_encoding("msgpack"));
}

TEST("normalize_push_url handles paths, queries, fragments, and invalid URLs") {
  auto bare = normalize_push_url("http://loki:3100");
  REQUIRE(bare.is_ok());
  CHECK_EQUAL(bare.unwrap().url, "http://loki:3100/loki/api/v1/push");
  CHECK_EQUAL(bare.unwrap().target, "/loki/api/v1/push");
  auto root = normalize_push_url("https://loki.example.com/");
  REQUIRE(root.is_ok());
  CHECK_EQUAL(root.unwrap().url, "https://loki.example.com/loki/api/v1/push");
  CHECK_EQUAL(root.unwrap().target, "/loki/api/v1/push");
  auto with_query_and_fragment
    = normalize_push_url("https://loki.example.com/?k=v#client-only");
  REQUIRE(with_query_and_fragment.is_ok());
  CHECK_EQUAL(with_query_and_fragment.unwrap().url,
              "https://loki.example.com/loki/api/v1/push?k=v");
  CHECK_EQUAL(with_query_and_fragment.unwrap().target, "/loki/api/v1/push?k=v");
  auto bare_with_fragment = normalize_push_url("http://loki:3100#frag");
  REQUIRE(bare_with_fragment.is_ok());
  CHECK_EQUAL(bare_with_fragment.unwrap().url,
              "http://loki:3100/loki/api/v1/push");
  CHECK_EQUAL(bare_with_fragment.unwrap().target, "/loki/api/v1/push");
  auto explicit_push
    = normalize_push_url("https://loki.example.com/loki/api/v1/push");
  REQUIRE(explicit_push.is_ok());
  CHECK_EQUAL(explicit_push.unwrap().url,
              "https://loki.example.com/loki/api/v1/push");
  CHECK_EQUAL(explicit_push.unwrap().target, "/loki/api/v1/push");
  auto custom_gateway = normalize_push_url(
    "https://gw.example.com/tenant/custom/push?x=1#secret-fragment");
  REQUIRE(custom_gateway.is_ok());
  CHECK_EQUAL(custom_gateway.unwrap().url,
              "https://gw.example.com/tenant/custom/push?x=1");
  CHECK_EQUAL(custom_gateway.unwrap().target, "/tenant/custom/push?x=1");
  // Reject non-HTTP schemes, missing hosts, and malformed URIs without leaking
  // credentials into the error string.
  auto bad_scheme = normalize_push_url("ftp://user:secret-token@loki:3100/");
  REQUIRE(bad_scheme.is_err());
  CHECK_EQUAL(bad_scheme.unwrap_err().find("secret-token"), std::string::npos);
  CHECK(normalize_push_url("http:///loki/api/v1/push").is_err());
  CHECK(normalize_push_url("not a url").is_err());
}

TEST("header name, reserved header, and header value validation") {
  CHECK(is_valid_header_name("Authorization"));
  CHECK(is_valid_header_name("X-Custom_Header.1"));
  CHECK(not is_valid_header_name(""));
  CHECK(not is_valid_header_name("Bad Header"));
  CHECK(not is_valid_header_name("Bad:Header"));
  CHECK(not is_valid_header_name("Bad\r\nHeader"));
  CHECK(is_reserved_header("Content-Type", false));
  CHECK(is_reserved_header("content-encoding", false));
  CHECK(is_reserved_header("CONTENT-LENGTH", false));
  CHECK(is_reserved_header("Transfer-Encoding", false));
  CHECK(is_reserved_header("Host", false));
  CHECK(not is_reserved_header("Authorization", false));
  CHECK(not is_reserved_header("X-Scope-OrgID", false));
  CHECK(is_reserved_header("X-Scope-OrgID", true));
  CHECK(is_reserved_header("x-scope-orgid", true));
  CHECK(is_valid_header_value("tenant-1"));
  CHECK(is_valid_header_value("Bearer\ttoken value"));
  CHECK(not is_valid_header_value("tenant\r\nX-Injected: 1"));
  CHECK(not is_valid_header_value("tenant\nline"));
  CHECK(not is_valid_header_value(std::string_view{"a\0b", 3}));
  CHECK(not is_valid_header_value("a\x7f"
                                  "b"));
}

TEST("is_valid_tenant_id enforces Loki multi-tenancy restrictions") {
  CHECK(is_valid_tenant_id("a"));
  CHECK(is_valid_tenant_id("tenant-1_prod.eu!*'(x)"));
  CHECK(is_valid_tenant_id("__mimir_cluster"));
  CHECK(is_valid_tenant_id(std::string(max_tenant_id_bytes, 'a')));
  CHECK(not is_valid_tenant_id(""));
  CHECK(not is_valid_tenant_id(std::string(max_tenant_id_bytes + 1, 'a')));
  CHECK(not is_valid_tenant_id("."));
  CHECK(not is_valid_tenant_id(".."));
  CHECK(not is_valid_tenant_id("tenant a"));
  CHECK(not is_valid_tenant_id("tenant/a"));
  CHECK(not is_valid_tenant_id("tenant|b"));
  CHECK(not is_valid_tenant_id("tenant\nb"));
  CHECK(not is_valid_tenant_id("caf\xC3\xA9"));
}

TEST("status classification, transport error redaction, and timeout rounding") {
  CHECK(is_push_success_status(204));
  CHECK(not is_push_success_status(200));
  CHECK(not is_push_success_status(201));
  CHECK(not is_push_success_status(202));
  CHECK(not is_push_success_status(260));
  CHECK(not is_push_success_status(400));
  CHECK(not is_push_success_status(429));
  CHECK(not is_push_success_status(500));
  auto const tls_msg = classify_transport_error(
    "TLS certificate verification failed for https://secret-token@loki");
  CHECK_EQUAL(tls_msg, "TLS handshake or certificate verification failed");
  CHECK_EQUAL(tls_msg.find("secret-token"), std::string_view::npos);
  CHECK_EQUAL(classify_transport_error("read timed out"),
              "request or connection timed out");
  CHECK_EQUAL(classify_transport_error("failed to resolve host"),
              "failed to resolve server hostname");
  CHECK_EQUAL(classify_transport_error("connection refused"),
              "connection refused");
  CHECK_EQUAL(classify_transport_error("unexpected peer reset"),
              "connection error");
  CHECK_EQUAL(classify_transport_error("opaque internal secret=xyz"),
              "network transport error");
  CHECK_EQUAL(ceil_milliseconds(0ns), 0ms);
  CHECK_EQUAL(ceil_milliseconds(-5ms), 0ms);
  CHECK_EQUAL(ceil_milliseconds(1ns), 1ms);
  CHECK_EQUAL(ceil_milliseconds(500us), 1ms);
  CHECK_EQUAL(ceil_milliseconds(1ms), 1ms);
  CHECK_EQUAL(ceil_milliseconds(1500us), 2ms);
}

TEST("quote_label_string, is_valid_label_name, and format_labels") {
  CHECK(is_valid_label_name("app"));
  CHECK(is_valid_label_name("_job_0"));
  CHECK(not is_valid_label_name(""));
  CHECK(not is_valid_label_name("0app"));
  CHECK(not is_valid_label_name("service.name"));
  CHECK(not is_valid_label_name("service name"));
  CHECK(not is_valid_label_name("k8s-pod"));
  CHECK_EQUAL(quote_label_string(R"(a"b\c)"), R"("a\"b\\c")");
  CHECK_EQUAL(quote_label_string("line1\nline2\r\t"), R"("line1\nline2\r\t")");
  CHECK_EQUAL(
    quote_label_string(std::string_view{"\x00\x07\x08\x0b\x0c\x1f\x7f", 7}),
    R"("\x00\a\b\v\f\x1f\x7f")");
  // Printable UTF-8 stays literal rather than being JSON-surrogate escaped.
  CHECK_EQUAL(quote_label_string("caf\xC3\xA9/ok"), "\"caf\xC3\xA9/ok\"");
  auto labels = Labels{
    {"app", "api"},
    {"env", "prod"},
    {"msg", "hi \"there\"\n\\ok"},
  };
  CHECK_EQUAL(format_labels(labels),
              R"({app="api", env="prod", msg="hi \"there\"\n\\ok"})");
}

TEST("split_timestamp_ns uses floor division across full int64 range") {
  CHECK_EQUAL(split_timestamp_ns(0), (SplitTimestamp{0, 0}));
  CHECK_EQUAL(split_timestamp_ns(1), (SplitTimestamp{0, 1}));
  CHECK_EQUAL(split_timestamp_ns(1'700'000'000'123'456'789LL),
              (SplitTimestamp{1'700'000'000LL, 123'456'789}));
  CHECK_EQUAL(split_timestamp_ns(-1), (SplitTimestamp{-1, 999'999'999}));
  CHECK_EQUAL(split_timestamp_ns(-1'000'000'000LL), (SplitTimestamp{-1, 0}));
  CHECK_EQUAL(split_timestamp_ns(-1'000'000'001LL),
              (SplitTimestamp{-2, 999'999'999}));
  CHECK_EQUAL(split_timestamp_ns(std::numeric_limits<int64_t>::max()),
              (SplitTimestamp{9'223'372'036LL, 854'775'807}));
  CHECK_EQUAL(split_timestamp_ns(std::numeric_limits<int64_t>::min()),
              (SplitTimestamp{-9'223'372'037LL, 145'224'192}));
}

TEST("serialize_protobuf checks max_bytes and round-trips with Snappy") {
  auto streams = std::vector<Stream>{
    {
      .labels = {{"app", "web"}, {"env", "prod"}},
      .entries = {
        {
          .timestamp_ns = 1'700'000'000'123'456'789LL,
          .line = "hello loki",
          .structured_metadata = {{"trace_id", "abc"}, {"user", "alice"}},
        },
        {
          .timestamp_ns = -500,
          .line = "pre-epoch",
          .structured_metadata = {},
        },
      },
    },
  };
  auto wire_res = serialize_protobuf(streams);
  REQUIRE(wire_res.is_ok());
  auto const wire = std::move(wire_res).unwrap();
  // Exact boundary succeeds; one byte smaller fails with exceeds_max_bytes.
  auto exact = serialize_protobuf(streams, wire.size());
  REQUIRE(exact.is_ok());
  CHECK_EQUAL(exact.unwrap(), wire);
  auto too_small = serialize_protobuf(streams, wire.size() - 1);
  REQUIRE(too_small.is_err());
  CHECK_EQUAL(too_small.unwrap_err(), SerializeError::exceeds_max_bytes);
  auto compressed = snappy_compress(wire);
  REQUIRE(compressed.is_ok());
  auto restored = snappy_decompress(compressed.unwrap(), wire.size());
  CHECK_EQUAL(restored, wire);
  auto decoded = ::tenzir::loki::proto::PushRequest{};
  REQUIRE(decoded.ParseFromString(restored));
  REQUIRE_EQUAL(decoded.streams_size(), 1);
  auto const& stream = decoded.streams(0);
  CHECK_EQUAL(stream.labels(), R"({app="web", env="prod"})");
  REQUIRE_EQUAL(stream.entries_size(), 2);
  CHECK_EQUAL(stream.entries(0).timestamp().seconds(), 1'700'000'000LL);
  CHECK_EQUAL(stream.entries(0).timestamp().nanos(), 123'456'789);
  CHECK_EQUAL(stream.entries(0).line(), "hello loki");
  REQUIRE_EQUAL(stream.entries(0).structured_metadata_size(), 2);
  CHECK_EQUAL(stream.entries(0).structured_metadata(0).name(), "trace_id");
  CHECK_EQUAL(stream.entries(0).structured_metadata(0).value(), "abc");
  CHECK_EQUAL(stream.entries(0).structured_metadata(1).name(), "user");
  CHECK_EQUAL(stream.entries(0).structured_metadata(1).value(), "alice");
  CHECK_EQUAL(stream.entries(1).timestamp().seconds(), -1);
  CHECK_EQUAL(stream.entries(1).timestamp().nanos(), 999'999'500);
  CHECK_EQUAL(stream.entries(1).line(), "pre-epoch");
  CHECK_EQUAL(stream.entries(1).structured_metadata_size(), 0);
}

TEST("serialize_json encodes nanosecond strings and optional structured "
     "metadata") {
  auto streams = std::vector<Stream>{
    {
      .labels = {{"app", "api"}, {"env", "prod"}},
      .entries = {
        {
          .timestamp_ns = 1'700'000'000'000'000'001LL,
          .line = "plain line",
          .structured_metadata = {},
        },
        {
          .timestamp_ns = 1'700'000'000'000'000'002LL,
          .line = "line with \"quotes\"\nand \\slash",
          .structured_metadata = {{"level", "error"}, {"trace_id", "t-42"}},
        },
      },
    },
  };
  auto json_str = serialize_json(streams);
  auto parser = simdjson::dom::parser{};
  auto doc = parser.parse(json_str);
  REQUIRE(not doc.error());
  auto json_streams = doc["streams"].get_array();
  REQUIRE(not json_streams.error());
  REQUIRE_EQUAL(json_streams.size(), size_t{1});
  auto first_stream = json_streams.at(0);
  CHECK_EQUAL(first_stream["stream"]["app"].get_string().value(), "api");
  CHECK_EQUAL(first_stream["stream"]["env"].get_string().value(), "prod");
  auto values = first_stream["values"].get_array();
  REQUIRE(not values.error());
  REQUIRE_EQUAL(values.size(), size_t{2});
  // Entry without structured metadata has 2 elements.
  auto v0 = values.at(0).get_array();
  REQUIRE_EQUAL(v0.size(), size_t{2});
  CHECK_EQUAL(v0.at(0).get_string().value(), "1700000000000000001");
  CHECK_EQUAL(v0.at(1).get_string().value(), "plain line");
  // Entry with structured metadata has 3 elements.
  auto v1 = values.at(1).get_array();
  REQUIRE_EQUAL(v1.size(), size_t{3});
  CHECK_EQUAL(v1.at(0).get_string().value(), "1700000000000000002");
  CHECK_EQUAL(v1.at(1).get_string().value(),
              "line with \"quotes\"\nand \\slash");
  CHECK_EQUAL(v1.at(2)["level"].get_string().value(), "error");
  CHECK_EQUAL(v1.at(2)["trace_id"].get_string().value(), "t-42");
}

TEST("sort_entries and split_streams preserve ascending timestamp order") {
  auto streams = std::vector<Stream>{
    {
      .labels = {{"app", "a"}},
      .entries = {
        {.timestamp_ns = 300, .line = "c", .structured_metadata = {}},
        {.timestamp_ns = 100, .line = "a1", .structured_metadata = {}},
        {.timestamp_ns = 200, .line = "b", .structured_metadata = {}},
        {.timestamp_ns = 100, .line = "a2", .structured_metadata = {}},
      },
    },
  };
  sort_entries(streams);
  REQUIRE_EQUAL(streams.front().entries.size(), size_t{4});
  CHECK_EQUAL(streams.front().entries[0].line, "a1");
  CHECK_EQUAL(streams.front().entries[1].line, "a2");
  CHECK_EQUAL(streams.front().entries[2].line, "b");
  CHECK_EQUAL(streams.front().entries[3].line, "c");
  auto second = split_streams(streams);
  REQUIRE_EQUAL(streams.size(), size_t{1});
  REQUIRE_EQUAL(second.size(), size_t{1});
  REQUIRE_EQUAL(streams.front().entries.size(), size_t{2});
  REQUIRE_EQUAL(second.front().entries.size(), size_t{2});
  CHECK_EQUAL(streams.front().entries[0].line, "a1");
  CHECK_EQUAL(streams.front().entries[1].line, "a2");
  CHECK_EQUAL(second.front().entries[0].line, "b");
  CHECK_EQUAL(second.front().entries[1].line, "c");
}

TEST("BatchBuffer groups, counts tenant key bytes, and evicts oldest first") {
  auto const t0 = BatchBuffer::TimePoint{100s};
  auto buf = BatchBuffer{
    /*max_batch_size=*/2,
    /*max_batch_bytes=*/500,
    /*flush_interval=*/5s,
    /*max_total_entries=*/4,
    /*max_total_bytes=*/10'000,
  };
  auto r1
    = buf.add(std::string{"tenant-a"}, {{"app", "web"}},
              {.timestamp_ns = 20, .line = "a1", .structured_metadata = {}},
              t0);
  CHECK(r1.empty());
  auto r2
    = buf.add(std::string{"tenant-b"}, {{"app", "db"}},
              {.timestamp_ns = 10, .line = "b1", .structured_metadata = {}},
              t0 + 1s);
  CHECK(r2.empty());
  CHECK_EQUAL(buf.next_deadline(), Option<BatchBuffer::TimePoint>{t0 + 5s});
  // Second entry for tenant-a triggers per-tenant max_batch_size flush.
  auto r3
    = buf.add(std::string{"tenant-a"}, {{"app", "web"}},
              {.timestamp_ns = 15, .line = "a2", .structured_metadata = {}},
              t0 + 2s);
  REQUIRE_EQUAL(r3.size(), size_t{1});
  CHECK_EQUAL(r3[0].tenant, Option<std::string>{"tenant-a"});
  CHECK_EQUAL(r3[0].entry_count, uint64_t{2});
  REQUIRE_EQUAL(r3[0].streams.size(), size_t{1});
  REQUIRE_EQUAL(r3[0].streams[0].entries.size(), size_t{2});
  // Next deadline now belongs to tenant-b (t0 + 1s + 5s = t0 + 6s).
  CHECK_EQUAL(buf.next_deadline(), Option<BatchBuffer::TimePoint>{t0 + 6s});
  CHECK(buf.take_expired(t0 + 5s).empty());
  auto expired = buf.take_expired(t0 + 6s);
  REQUIRE_EQUAL(expired.size(), size_t{1});
  CHECK_EQUAL(expired[0].tenant, Option<std::string>{"tenant-b"});
  CHECK(buf.empty());
  // Proactive flush before overflowing max_batch_bytes.
  auto big1 = buf.add(None{}, {{"app", "big"}},
                      {.timestamp_ns = 1,
                       .line = std::string(300, 'x'),
                       .structured_metadata = {}},
                      t0);
  CHECK(big1.empty());
  auto big2 = buf.add(None{}, {{"app", "big"}},
                      {.timestamp_ns = 2,
                       .line = std::string(300, 'y'),
                       .structured_metadata = {}},
                      t0);
  REQUIRE_EQUAL(big2.size(), size_t{1});
  CHECK_EQUAL(big2[0].entry_count, uint64_t{1});
  CHECK_EQUAL(big2[0].streams[0].entries[0].line, std::string(300, 'x'));
  auto rest = buf.take_all();
  REQUIRE_EQUAL(rest.size(), size_t{1});
  CHECK_EQUAL(rest[0].streams[0].entries[0].line, std::string(300, 'y'));
  // Global entry cap evicts the earliest-deadline open tenant in O(1).
  auto cap_buf = BatchBuffer{2, 100'000, duration::max(), 3, 100'000};
  CHECK(cap_buf
          .add(std::string{"t1"}, {{"app", "a"}},
               {.timestamp_ns = 1, .line = "x", .structured_metadata = {}}, t0)
          .empty());
  CHECK(cap_buf
          .add(std::string{"t2"}, {{"app", "a"}},
               {.timestamp_ns = 2, .line = "xx", .structured_metadata = {}},
               t0 + 1s)
          .empty());
  auto evicted
    = cap_buf.add(std::string{"t3"}, {{"app", "a"}},
                  {.timestamp_ns = 3, .line = "xxx", .structured_metadata = {}},
                  t0 + 2s);
  REQUIRE_EQUAL(evicted.size(), size_t{1});
  CHECK_EQUAL(evicted[0].tenant, Option<std::string>{"t1"});
  CHECK_EQUAL(cap_buf.total_entries(), uint64_t{2});
  // duration::max() flush interval saturates cleanly without overflow.
  CHECK_EQUAL(cap_buf.next_deadline(),
              Option<BatchBuffer::TimePoint>{BatchBuffer::TimePoint::max()});
  // Tenant key bytes are counted against total_bytes so long tenant IDs
  // trigger global byte eviction cleanly.
  auto tenant_byte_buf = BatchBuffer{10, 400, 10s, 100, 400};
  auto const long_t1 = std::string(149, 'a') + "1";
  auto const long_t2 = std::string(149, 'a') + "2";
  auto tb1 = tenant_byte_buf.add(
    long_t1, {{"app", "a"}},
    {.timestamp_ns = 1, .line = "x", .structured_metadata = {}}, t0);
  CHECK(tb1.empty());
  CHECK(tenant_byte_buf.total_bytes() > 150);
  auto tb2 = tenant_byte_buf.add(
    long_t2, {{"app", "a"}},
    {.timestamp_ns = 2, .line = "y", .structured_metadata = {}}, t0 + 1s);
  REQUIRE_EQUAL(tb2.size(), size_t{1});
  CHECK_EQUAL(tb2[0].tenant, Option<std::string>{long_t1});
  // Moving a populated BatchBuffer preserves deadline index invariants.
  auto moved = std::move(tenant_byte_buf);
  CHECK_EQUAL(moved.next_deadline(), Option<BatchBuffer::TimePoint>{t0 + 11s});
  auto moved_rest = moved.take_all();
  REQUIRE_EQUAL(moved_rest.size(), size_t{1});
  CHECK_EQUAL(moved_rest[0].tenant, Option<std::string>{long_t2});
  CHECK(moved.empty());
}
