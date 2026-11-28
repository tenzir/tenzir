//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "loki/push.pb.h"

#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/saturating_arithmetic.hpp>
#include <tenzir/detail/string.hpp>

#include <arrow/util/compression.h>
#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>
#include <fmt/format.h>
#include <loki/push.hpp>

#include <algorithm>
#include <array>
#include <charconv>
#include <iterator>
#include <limits>
#include <ranges>
#include <simdjson.h>
#include <utility>

namespace tenzir::plugins::loki {

namespace {

using namespace std::string_view_literals;

constexpr auto default_push_path = "/loki/api/v1/push"sv;
constexpr auto nanos_per_second = int64_t{1'000'000'000};
constexpr auto hex_digits = std::string_view{"0123456789abcdef"};

auto is_http_token_char(unsigned char ch) -> bool {
  if (detail::ascii_isalnum(ch)) {
    return true;
  }
  constexpr auto specials = "!#$%&'*+-.^_`|~"sv;
  return specials.find(static_cast<char>(ch)) != std::string_view::npos;
}

auto is_tenant_id_char(unsigned char ch) -> bool {
  if (detail::ascii_isalnum(ch)) {
    return true;
  }
  constexpr auto specials = "!-_.*'()"sv;
  return specials.find(static_cast<char>(ch)) != std::string_view::npos;
}

auto contains_icase(std::string_view haystack, std::string_view needle)
  -> bool {
  auto const lower = detail::ascii_tolower(haystack);
  return lower.find(needle) != std::string::npos;
}

auto append_escaped_label_chars(std::string& out, std::string_view value)
  -> void {
  for (auto ch : value) {
    auto const byte = static_cast<unsigned char>(ch);
    switch (byte) {
      case '\\':
        out.append("\\\\");
        break;
      case '"':
        out.append("\\\"");
        break;
      case '\a':
        out.append("\\a");
        break;
      case '\b':
        out.append("\\b");
        break;
      case '\f':
        out.append("\\f");
        break;
      case '\n':
        out.append("\\n");
        break;
      case '\r':
        out.append("\\r");
        break;
      case '\t':
        out.append("\\t");
        break;
      case '\v':
        out.append("\\v");
        break;
      default:
        if (byte < 0x20 or byte == 0x7f) {
          out.append("\\x");
          out.push_back(hex_digits[byte >> 4]);
          out.push_back(hex_digits[byte & 0x0f]);
        } else {
          out.push_back(ch);
        }
        break;
    }
  }
}

auto append_json_int64_string(simdjson::builder::string_builder& builder,
                              int64_t value) -> void {
  auto buf = std::array<char, 24>{};
  auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), value);
  TENZIR_ASSERT(ec == std::errc{});
  builder.append('"');
  builder.append_raw(
    std::string_view{buf.data(), static_cast<size_t>(ptr - buf.data())});
  builder.append('"');
}

} // namespace

auto parse_encoding(std::string_view value) -> Option<Encoding> {
  if (value == "protobuf") {
    return Encoding::protobuf;
  }
  if (value == "json") {
    return Encoding::json;
  }
  return None{};
}

auto normalize_push_url(std::string_view url)
  -> Result<NormalizedPushUrl, std::string> {
  auto parsed = boost::urls::parse_uri(url);
  if (not parsed) {
    return Err{std::string{"expected a valid HTTP(S) URL"}};
  }
  auto const scheme = parsed->scheme();
  if (scheme != "http" and scheme != "https") {
    return Err{std::string{"URL scheme must be `http` or `https`"}};
  }
  if (not parsed->has_authority() or parsed->encoded_host().empty()) {
    return Err{std::string{"URL must contain a host"}};
  }
  auto normalized = boost::urls::url{*parsed};
  // Fragments are client-only per RFC 3986 / RFC 9110 and must never be sent
  // on the HTTP request target.
  normalized.remove_fragment();
  auto const path = normalized.encoded_path();
  if (path.empty() or path == "/") {
    normalized.set_path(default_push_path);
  }
  auto const target = normalized.encoded_target();
  return NormalizedPushUrl{
    .url = std::string{normalized.buffer()},
    .target = std::string{target.data(), target.size()},
  };
}

auto is_valid_header_name(std::string_view name) -> bool {
  if (name.empty()) {
    return false;
  }
  return std::ranges::all_of(name, [](char ch) {
    return is_http_token_char(static_cast<unsigned char>(ch));
  });
}

auto is_reserved_header(std::string_view name, bool has_tenant_expr) -> bool {
  constexpr auto always_reserved = std::array{
    "content-encoding"sv,  "content-length"sv, "content-type"sv, "host"sv,
    "transfer-encoding"sv,
  };
  if (std::ranges::any_of(always_reserved, [name](std::string_view header) {
        return detail::ascii_icase_equal(name, header);
      })) {
    return true;
  }
  return has_tenant_expr and detail::ascii_icase_equal(name, "x-scope-orgid"sv);
}

auto is_valid_header_value(std::string_view value) -> bool {
  return std::ranges::all_of(value, [](char ch) {
    auto const byte = static_cast<unsigned char>(ch);
    return byte == '\t' or (byte >= 0x20 and byte != 0x7f);
  });
}

auto is_valid_tenant_id(std::string_view tenant) -> bool {
  if (tenant.empty() or tenant.size() > max_tenant_id_bytes) {
    return false;
  }
  if (tenant == "." or tenant == "..") {
    return false;
  }
  return std::ranges::all_of(tenant, [](char ch) {
    return is_tenant_id_char(static_cast<unsigned char>(ch));
  });
}

auto is_push_success_status(uint16_t status_code) -> bool {
  return status_code == 204;
}

auto classify_transport_error(std::string_view error) -> std::string_view {
  if (contains_icase(error, "tls") or contains_icase(error, "ssl")
      or contains_icase(error, "certificate")
      or contains_icase(error, "x509")) {
    return "TLS handshake or certificate verification failed"sv;
  }
  if (contains_icase(error, "timeout") or contains_icase(error, "timed out")) {
    return "request or connection timed out"sv;
  }
  if (contains_icase(error, "resolve") or contains_icase(error, "dns")
      or contains_icase(error, "getaddrinfo")
      or contains_icase(error, "unknown host")) {
    return "failed to resolve server hostname"sv;
  }
  if (contains_icase(error, "refused")) {
    return "connection refused"sv;
  }
  if (contains_icase(error, "reset") or contains_icase(error, "eof")
      or contains_icase(error, "closed") or contains_icase(error, "broken pipe")
      or contains_icase(error, "connect")) {
    return "connection error"sv;
  }
  return "network transport error"sv;
}

auto ceil_milliseconds(duration value) -> std::chrono::milliseconds {
  if (value <= duration::zero()) {
    return std::chrono::milliseconds::zero();
  }
  return std::chrono::ceil<std::chrono::milliseconds>(value);
}

auto is_valid_label_name(std::string_view name) -> bool {
  if (name.empty()) {
    return false;
  }
  auto const first = static_cast<unsigned char>(name.front());
  if (not(detail::ascii_isalpha(first) or first == '_')) {
    return false;
  }
  return std::ranges::all_of(name | std::views::drop(1), [](unsigned char c) {
    return detail::ascii_isalnum(c) or c == '_';
  });
}

auto quote_label_string(std::string_view value) -> std::string {
  auto result = std::string{};
  result.reserve(value.size() + 2);
  result.push_back('"');
  append_escaped_label_chars(result, value);
  result.push_back('"');
  return result;
}

auto format_labels(Labels const& labels) -> std::string {
  auto result = std::string{"{"};
  auto first = true;
  for (auto const& [key, value] : labels) {
    TENZIR_ASSERT(is_valid_label_name(key));
    if (not first) {
      result.append(", ");
    }
    first = false;
    result.append(key);
    result.push_back('=');
    result.push_back('"');
    append_escaped_label_chars(result, value);
    result.push_back('"');
  }
  result.push_back('}');
  return result;
}

auto split_timestamp_ns(int64_t timestamp_ns) -> SplitTimestamp {
  // Protobuf timestamps always carry non-negative nanoseconds in [0, 1e9 - 1],
  // so negative epoch timestamps use floor division toward -infinity.
  auto seconds = timestamp_ns / nanos_per_second;
  auto nanos = timestamp_ns % nanos_per_second;
  if (nanos < 0) {
    --seconds;
    nanos += nanos_per_second;
  }
  return {
    .seconds = seconds,
    .nanos = detail::narrow<int32_t>(nanos),
  };
}

auto sort_entries(std::vector<Stream>& streams) -> void {
  for (auto& stream : streams) {
    std::ranges::stable_sort(stream.entries, {}, &Entry::timestamp_ns);
  }
}

auto serialize_protobuf(std::vector<Stream> const& streams, size_t max_bytes)
  -> Result<std::string, SerializeError> {
  auto request = ::tenzir::loki::proto::PushRequest{};
  for (auto const& stream : streams) {
    auto* out_stream = request.add_streams();
    out_stream->set_labels(format_labels(stream.labels));
    for (auto const& entry : stream.entries) {
      auto* out_entry = out_stream->add_entries();
      auto const ts = split_timestamp_ns(entry.timestamp_ns);
      auto* out_ts = out_entry->mutable_timestamp();
      out_ts->set_seconds(ts.seconds);
      out_ts->set_nanos(ts.nanos);
      out_entry->set_line(entry.line);
      for (auto const& [name, value] : entry.structured_metadata) {
        auto* out_pair = out_entry->add_structured_metadata();
        out_pair->set_name(name);
        out_pair->set_value(value);
      }
    }
  }
  auto const wire_size = request.ByteSizeLong();
  if (wire_size > max_protobuf_message_bytes or wire_size > max_bytes) {
    return Err{SerializeError::exceeds_max_bytes};
  }
  auto output = std::string{};
  output.reserve(wire_size);
  if (not request.SerializeToString(&output)) {
    return Err{SerializeError::serialization_failed};
  }
  return output;
}

auto snappy_compress(std::string_view input)
  -> Result<std::string, std::string> {
  auto codec_result = arrow::util::Codec::Create(
    arrow::Compression::type::SNAPPY,
    arrow::util::Codec::UseDefaultCompressionLevel());
  if (not codec_result.ok()) {
    return Err{std::string{"failed to create Snappy codec"}};
  }
  auto codec = codec_result.MoveValueUnsafe();
  auto const input_size = detail::narrow<int64_t>(input.size());
  auto const* input_ptr = reinterpret_cast<uint8_t const*>(input.data());
  auto const max_len = codec->MaxCompressedLen(input_size, input_ptr);
  auto output = std::string{};
  output.resize(detail::narrow<size_t>(max_len));
  auto compressed_size
    = codec->Compress(input_size, input_ptr,
                      detail::narrow<int64_t>(output.size()),
                      reinterpret_cast<uint8_t*>(output.data()));
  if (not compressed_size.ok()) {
    return Err{std::string{"Snappy compression failed"}};
  }
  output.resize(detail::narrow<size_t>(*compressed_size));
  return output;
}

auto serialize_json(std::vector<Stream> const& streams) -> std::string {
  auto builder = simdjson::builder::string_builder{};
  builder.start_object();
  builder.escape_and_append_with_quotes("streams");
  builder.append_colon();
  builder.start_array();
  for (auto s_idx = size_t{0}; s_idx < streams.size(); ++s_idx) {
    if (s_idx > 0) {
      builder.append_comma();
    }
    auto const& stream = streams[s_idx];
    builder.start_object();
    builder.escape_and_append_with_quotes("stream");
    builder.append_colon();
    builder.start_object();
    for (auto l_idx = size_t{0}; l_idx < stream.labels.size(); ++l_idx) {
      if (l_idx > 0) {
        builder.append_comma();
      }
      auto const& [key, value] = stream.labels[l_idx];
      TENZIR_ASSERT(is_valid_label_name(key));
      builder.escape_and_append_with_quotes(key);
      builder.append_colon();
      builder.escape_and_append_with_quotes(value);
    }
    builder.end_object();
    builder.append_comma();
    builder.escape_and_append_with_quotes("values");
    builder.append_colon();
    builder.start_array();
    for (auto e_idx = size_t{0}; e_idx < stream.entries.size(); ++e_idx) {
      if (e_idx > 0) {
        builder.append_comma();
      }
      auto const& entry = stream.entries[e_idx];
      builder.start_array();
      append_json_int64_string(builder, entry.timestamp_ns);
      builder.append_comma();
      builder.escape_and_append_with_quotes(entry.line);
      if (not entry.structured_metadata.empty()) {
        builder.append_comma();
        builder.start_object();
        for (auto m_idx = size_t{0}; m_idx < entry.structured_metadata.size();
             ++m_idx) {
          if (m_idx > 0) {
            builder.append_comma();
          }
          auto const& [key, value] = entry.structured_metadata[m_idx];
          builder.escape_and_append_with_quotes(key);
          builder.append_colon();
          builder.escape_and_append_with_quotes(value);
        }
        builder.end_object();
      }
      builder.end_array();
    }
    builder.end_array();
    builder.end_object();
  }
  builder.end_array();
  builder.end_object();
  auto const view = builder.view();
  TENZIR_ASSERT(not view.error());
  return std::string{view.value_unsafe()};
}

auto estimate_entry_bytes(Entry const& entry, bool is_new_stream,
                          Labels const& labels) -> uint64_t {
  // Account for timestamp, line, and framing overhead conservatively so
  // batches flush near `max_batch_bytes` without frequent wire splitting.
  auto result = detail::saturating_add<uint64_t>(32, entry.line.size());
  for (auto const& [key, value] : entry.structured_metadata) {
    result = detail::saturating_add<uint64_t>(result, 8);
    result = detail::saturating_add<uint64_t>(result, key.size());
    result = detail::saturating_add<uint64_t>(result, value.size());
  }
  if (is_new_stream) {
    result = detail::saturating_add<uint64_t>(result, 16);
    for (auto const& [key, value] : labels) {
      result = detail::saturating_add<uint64_t>(result, 6);
      result = detail::saturating_add<uint64_t>(result, key.size());
      result = detail::saturating_add<uint64_t>(result, value.size());
    }
  }
  return result;
}

auto split_streams(std::vector<Stream>& streams) -> std::vector<Stream> {
  TENZIR_ASSERT(
    streams.size() > 1
    or (streams.size() == 1 and streams.front().entries.size() > 1));
  if (streams.size() > 1) {
    auto result = std::vector<Stream>{};
    auto const half = streams.size() / 2;
    result.reserve(streams.size() - half);
    std::move(streams.begin() + detail::narrow<int64_t>(half), streams.end(),
              std::back_inserter(result));
    streams.erase(streams.begin() + detail::narrow<int64_t>(half),
                  streams.end());
    return result;
  }
  auto& stream = streams.front();
  auto const half = stream.entries.size() / 2;
  auto second = Stream{};
  second.labels = stream.labels;
  second.entries.reserve(stream.entries.size() - half);
  std::move(stream.entries.begin() + detail::narrow<int64_t>(half),
            stream.entries.end(), std::back_inserter(second.entries));
  stream.entries.erase(stream.entries.begin() + detail::narrow<int64_t>(half),
                       stream.entries.end());
  auto result = std::vector<Stream>{};
  result.push_back(std::move(second));
  return result;
}

BatchBuffer::BatchBuffer(uint64_t max_batch_size, uint64_t max_batch_bytes,
                         duration flush_interval, uint64_t max_total_entries,
                         uint64_t max_total_bytes)
  : max_batch_size_{std::max<uint64_t>(max_batch_size, 1)},
    max_batch_bytes_{std::max<uint64_t>(max_batch_bytes, 1)},
    flush_interval_{flush_interval},
    max_total_entries_{std::max(max_total_entries, max_batch_size_)},
    max_total_bytes_{std::max(max_total_bytes, max_batch_bytes_)} {
}

auto BatchBuffer::add(Option<std::string> tenant, Labels labels, Entry entry,
                      TimePoint now) -> std::vector<TenantBatch> {
  auto ready = std::vector<TenantBatch>{};
  auto it = pending_.find(tenant);
  auto is_new_stream
    = it == pending_.end() or not it->second.streams.contains(labels);
  auto entry_bytes = estimate_entry_bytes(entry, is_new_stream, labels);
  // Flush the tenant's open batch before appending if adding this entry would
  // push the batch over `max_batch_bytes`.
  if (it != pending_.end() and it->second.entry_count > 0
      and detail::saturating_add(it->second.estimated_bytes, entry_bytes)
            > max_batch_bytes_) {
    ready.push_back(extract(it));
    it = pending_.end();
    is_new_stream = true;
    entry_bytes = estimate_entry_bytes(entry, is_new_stream, labels);
  }
  auto added_bytes = entry_bytes;
  if (it == pending_.end()) {
    // Account for the tenant key's own byte footprint so high-cardinality
    // tenants cannot exceed the buffer's byte budget.
    auto const tenant_bytes = tenant ? uint64_t{tenant->size()} : uint64_t{0};
    added_bytes = detail::saturating_add(added_bytes, tenant_bytes);
    auto [inserted, ok] = pending_.emplace(std::move(tenant), PendingTenant{});
    TENZIR_ASSERT(ok);
    inserted->second.deadline = compute_deadline(now);
    inserted->second.deadline_it
      = by_deadline_.emplace(inserted->second.deadline, inserted);
    it = inserted;
  }
  auto& bucket = it->second;
  bucket.streams[std::move(labels)].push_back(std::move(entry));
  ++bucket.entry_count;
  bucket.estimated_bytes
    = detail::saturating_add(bucket.estimated_bytes, added_bytes);
  ++total_entries_;
  total_bytes_ = detail::saturating_add(total_bytes_, added_bytes);
  if (bucket.entry_count >= max_batch_size_
      or bucket.estimated_bytes >= max_batch_bytes_) {
    ready.push_back(extract(it));
  }
  // Enforce the global buffer cap across all open tenants in O(1) per eviction
  // by flushing the earliest-deadline tenant first.
  while (not pending_.empty()
         and (total_entries_ >= max_total_entries_
              or total_bytes_ >= max_total_bytes_)) {
    TENZIR_ASSERT(not by_deadline_.empty());
    ready.push_back(extract(by_deadline_.begin()->second));
  }
  return ready;
}

auto BatchBuffer::next_deadline() const -> Option<TimePoint> {
  if (by_deadline_.empty()) {
    return None{};
  }
  return by_deadline_.begin()->first;
}

auto BatchBuffer::take_expired(TimePoint now) -> std::vector<TenantBatch> {
  auto ready = std::vector<TenantBatch>{};
  while (not by_deadline_.empty() and by_deadline_.begin()->first <= now) {
    ready.push_back(extract(by_deadline_.begin()->second));
  }
  return ready;
}

auto BatchBuffer::take_all() -> std::vector<TenantBatch> {
  auto ready = std::vector<TenantBatch>{};
  ready.reserve(pending_.size());
  while (not pending_.empty()) {
    ready.push_back(extract(pending_.begin()));
  }
  return ready;
}

auto BatchBuffer::empty() const -> bool {
  return pending_.empty();
}

auto BatchBuffer::total_entries() const -> uint64_t {
  return total_entries_;
}

auto BatchBuffer::total_bytes() const -> uint64_t {
  return total_bytes_;
}

auto BatchBuffer::extract(TenantMap::iterator it) -> TenantBatch {
  auto batch = TenantBatch{};
  batch.tenant = it->first;
  batch.entry_count = it->second.entry_count;
  batch.streams.reserve(it->second.streams.size());
  for (auto& [labels, entries] : it->second.streams) {
    batch.streams.push_back(Stream{
      .labels = labels,
      .entries = std::move(entries),
    });
  }
  total_entries_
    = detail::saturating_sub(total_entries_, it->second.entry_count);
  total_bytes_
    = detail::saturating_sub(total_bytes_, it->second.estimated_bytes);
  by_deadline_.erase(it->second.deadline_it);
  pending_.erase(it);
  if (pending_.empty()) {
    total_entries_ = 0;
    total_bytes_ = 0;
  }
  return batch;
}

auto BatchBuffer::compute_deadline(TimePoint now) const -> TimePoint {
  auto const max_remaining = TimePoint::max() - now;
  auto const step
    = std::chrono::duration_cast<Clock::duration>(flush_interval_);
  if (step >= max_remaining) {
    return TimePoint::max();
  }
  return now + step;
}

} // namespace tenzir::plugins::loki
