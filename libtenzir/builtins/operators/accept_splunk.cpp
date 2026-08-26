//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arc.hpp"
#include "tenzir/async/bounded_queue.hpp"
#include "tenzir/async/oneshot.hpp"
#include "tenzir/async/semaphore.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/blob.hpp"
#include "tenzir/checked_math.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/concept/parseable/tenzir/uuid.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http.hpp"
#include "tenzir/http_server.hpp"
#include "tenzir/json_parser.hpp"
#include "tenzir/multi_series_builder.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/pipeline_metrics.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/variant.hpp"

#include <folly/io/IOBuf.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/coro/HTTPCoroSession.h>
#include <proxygen/lib/http/coro/HTTPSourceReader.h>
#include <proxygen/lib/http/coro/server/HTTPServer.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <limits>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tenzir::plugins::accept_splunk {

namespace {

using namespace tenzir::si_literals;

constexpr auto json_content_type = std::string_view{"application/json"};
constexpr auto json_whitespace = std::string_view{" \t\r\n"};
constexpr auto default_max_request_size = size_t{10_Mi};
constexpr auto compressed_size_allowance = size_t{64_Ki};
constexpr auto success_body
  = std::string_view{R"({"text":"Success","code":0})"};
constexpr auto health_body
  = std::string_view{R"({"text":"HEC is healthy","code":17})"};

struct AcceptSplunkArgs {
  located<std::string> endpoint{"0.0.0.0:8088", location::unknown};
  located<secret> hec_token;
  Option<located<uint64_t>> max_request_size;
  Option<located<uint64_t>> max_connections;
  Option<located<data>> tls;

  auto get_max_request_size() const -> size_t {
    return max_request_size ? detail::narrow<size_t>(max_request_size->inner)
                            : default_max_request_size;
  }

  auto get_max_concurrent_requests() const -> uint64_t {
    return max_connections ? max_connections->inner : uint64_t{10};
  }
};

struct Response {
  uint16_t status;
  std::string body;
};

using ResponseSignal = Oneshot<Response>;

enum class EndpointKind {
  event,
  raw,
};

struct RequestMetadata {
  EndpointKind kind;
  std::string peer_ip;
  Option<std::string> channel;
  record query;
};

struct RequestStarted {
  uint64_t request_id;
  RequestMetadata metadata;
  std::string content_encoding;
  Arc<ResponseSignal> response_signal;
};

struct RequestBody {
  uint64_t request_id;
  SimdjsonPaddedBuffer data;
};

struct RequestFinished {
  uint64_t request_id;
};

struct Noop {};

using Message = variant<Noop, RequestStarted, RequestBody, RequestFinished>;
using MessageQueue = BoundedQueue<Message>;

auto hec_response(uint16_t status, int code, std::string_view text)
  -> Response {
  return Response{
    .status = status,
    .body = fmt::format(R"({{"text":"{}","code":{}}})", text, code),
  };
}

auto classify_path(proxygen::HTTPMessage const& msg, std::string peer_ip)
  -> variant<Response, RequestMetadata> {
  auto const method = msg.getMethod();
  auto const path = std::string_view{msg.getPathAsStringPiece()};
  if (method == proxygen::HTTPMethod::GET
      and (path == "/services/collector/health"
           or path == "/services/collector/health/1.0")) {
    return Response{.status = 200, .body = std::string{health_body}};
  }
  if (method == proxygen::HTTPMethod::POST
      and path == "/services/collector/ack") {
    return hec_response(400, 14, "ACK is disabled");
  }
  auto kind = Option<EndpointKind>{None{}};
  if (method == proxygen::HTTPMethod::POST
      and (path == "/services/collector" or path == "/services/collector/event"
           or path == "/services/collector/event/1.0")) {
    kind = EndpointKind::event;
  } else if (method == proxygen::HTTPMethod::POST
             and (path == "/services/collector/raw"
                  or path == "/services/collector/raw/1.0")) {
    kind = EndpointKind::raw;
  }
  if (not kind) {
    return hec_response(404, 6, "Invalid data format");
  }
  auto query = record{};
  for (auto const& [key, value] : msg.getQueryParams()) {
    query[key] = value;
  }
  auto channel = Option<std::string>{None{}};
  auto header_channel = std::string{
    msg.getHeaders().getSingleOrEmpty("X-Splunk-Request-Channel")};
  if (not header_channel.empty()) {
    channel = std::move(header_channel);
  } else if (auto it = query.find("channel"); it != query.end()) {
    if (auto value = try_as<std::string>(&it->second);
        value and not value->empty()) {
      channel = *value;
    }
  }
  return RequestMetadata{
    .kind = *kind,
    .peer_ip = std::move(peer_ip),
    .channel = std::move(channel),
    .query = std::move(query),
  };
}

class RequestHandler final : public proxygen::coro::HTTPHandler {
public:
  RequestHandler(size_t max_request_size, std::string token,
                 Arc<MessageQueue> queue,
                 Arc<Atomic<uint64_t>> request_id_generator,
                 Arc<Semaphore> request_slots)
    : max_request_size_{max_request_size},
      token_{std::move(token)},
      queue_{std::move(queue)},
      request_id_generator_{std::move(request_id_generator)},
      request_slots_{std::move(request_slots)} {
  }

  auto handleRequest(folly::EventBase*,
                     proxygen::coro::HTTPSessionContextPtr session,
                     proxygen::coro::HTTPSourceHolder request_source)
    -> folly::coro::Task<proxygen::coro::HTTPSourceHolder> override {
    TENZIR_ASSERT(session);
    auto permit = request_slots_->try_acquire();
    if (not permit) {
      auto response = hec_response(503, 9, "Server is busy");
      co_return http_server::make_response(response.status,
                                           std::string{json_content_type},
                                           std::move(response.body));
    }
    auto request_id
      = request_id_generator_->fetch_add(1, std::memory_order_relaxed);
    auto response_signal = Arc<ResponseSignal>{std::in_place};
    auto reader = proxygen::coro::HTTPSourceReader{std::move(request_source)};
    auto started = false;
    auto compressed_bytes = size_t{0};
    auto max_compressed_bytes
      = checked_add(max_request_size_, compressed_size_allowance);
    TENZIR_ASSERT(max_compressed_bytes);
    reader
      .onHeadersAsync([&](std::unique_ptr<proxygen::HTTPMessage> msg,
                          bool is_final, bool) -> folly::coro::Task<bool> {
        if (not is_final) {
          co_return proxygen::coro::HTTPSourceReader::Continue;
        }
        auto classified
          = classify_path(*msg, session->getPeerAddress().getAddressStr());
        if (auto response = try_as<Response>(&classified)) {
          response_signal->send(std::move(*response));
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }
        auto authorization = std::string_view{
          msg->getHeaders().getSingleOrEmpty("Authorization")};
        if (authorization.empty()) {
          response_signal->send(hec_response(401, 2, "Token is required"));
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }
        constexpr auto scheme = std::string_view{"Splunk"};
        if (authorization.size() <= scheme.size()
            or not detail::ascii_icase_equal(
              authorization.substr(0, scheme.size()), scheme)
            or authorization[scheme.size()] != ' ') {
          response_signal->send(hec_response(401, 3, "Invalid authorization"));
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }
        if (authorization.substr(scheme.size() + 1) != token_) {
          response_signal->send(hec_response(403, 4, "Invalid token"));
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }
        auto metadata = std::move(as<RequestMetadata>(classified));
        if (metadata.kind == EndpointKind::raw and not metadata.channel) {
          response_signal->send(
            hec_response(400, 10, "Data channel is missing"));
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }
        if (metadata.channel) {
          auto channel = uuid{};
          if (not parsers::uuid(*metadata.channel, channel)) {
            response_signal->send(
              hec_response(400, 11, "Invalid data channel"));
            co_return proxygen::coro::HTTPSourceReader::Cancel;
          }
        }
        auto content_encoding
          = std::string{msg->getHeaders().getSingleOrEmpty("Content-Encoding")};
        if (not content_encoding.empty()) {
          content_encoding
            = detail::ascii_tolower(detail::trim(content_encoding));
          if (content_encoding != "gzip") {
            response_signal->send(hec_response(415, 6, "Invalid data format"));
            co_return proxygen::coro::HTTPSourceReader::Cancel;
          }
        }
        if (content_encoding.empty()) {
          auto content_length = http_server::parse_number<size_t>(
            msg->getHeaders().getSingleOrEmpty("Content-Length"));
          if (content_length and *content_length > max_request_size_) {
            response_signal->send(
              hec_response(413, 6, "Data payload too large"));
            co_return proxygen::coro::HTTPSourceReader::Cancel;
          }
        }
        co_await queue_->enqueue(RequestStarted{
          .request_id = request_id,
          .metadata = std::move(metadata),
          .content_encoding = std::move(content_encoding),
          .response_signal = response_signal,
        });
        started = true;
        co_return proxygen::coro::HTTPSourceReader::Continue;
      })
      .onBodyAsync([&](quic::BufQueue body, bool) -> folly::coro::Task<bool> {
        if (response_signal->has_sent()) {
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }
        if (body.empty()) {
          co_return proxygen::coro::HTTPSourceReader::Continue;
        }
        auto buffer = body.move();
        buffer->coalesce();
        auto new_compressed_bytes
          = checked_add(compressed_bytes, buffer->length());
        if (not new_compressed_bytes
            or *new_compressed_bytes > *max_compressed_bytes) {
          response_signal->send(hec_response(413, 6, "Data payload too large"));
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }
        compressed_bytes = *new_compressed_bytes;
        auto bytes = std::span{
          reinterpret_cast<std::byte const*>(buffer->data()), buffer->length()};
        auto data = SimdjsonPaddedBuffer{bytes};
        co_await queue_->enqueue(
          RequestBody{.request_id = request_id, .data = std::move(data)});
        co_return proxygen::coro::HTTPSourceReader::Continue;
      })
      .onError([&](proxygen::coro::HTTPSourceReader::ErrorContext,
                   proxygen::coro::HTTPError const&) {
        if (not response_signal->has_sent()) {
          response_signal->send(hec_response(400, 6, "Invalid data format"));
        }
      });
    co_await reader.read(detail::narrow<uint32_t>(*max_compressed_bytes));
    if (started) {
      co_await queue_->enqueue(RequestFinished{.request_id = request_id});
    }
    auto response = co_await response_signal->recv();
    co_await folly::coro::co_reschedule_on_current_executor;
    auto response_source
      = http_server::make_response(response.status,
                                   std::string{json_content_type},
                                   std::move(response.body));
    co_return http_server::track_response_delivery(
      std::move(response_source),
      [queue = queue_, request_slots = request_slots_,
       permit = std::move(*permit)]() mutable {
        TENZIR_UNUSED(request_slots);
        permit.release();
        queue->force_enqueue(Noop{});
      });
  }

private:
  size_t max_request_size_;
  std::string token_;
  Arc<MessageQueue> queue_;
  Arc<Atomic<uint64_t>> request_id_generator_;
  Arc<Semaphore> request_slots_;
};

struct InFlightRequest {
  RequestMetadata metadata;
  Arc<ResponseSignal> response_signal;
  Option<std::shared_ptr<arrow::util::Decompressor>> decompressor;
  SimdjsonPaddedBuffer body;
  size_t output_bytes = 0;
  bool decompression_finished = false;
};

auto normalize_time(data& value) -> bool {
  auto seconds = Option<double>{None{}};
  if (auto x = try_as<int64_t>(&value); x and *x >= 0) {
    seconds = static_cast<double>(*x);
  } else if (auto x = try_as<uint64_t>(&value)) {
    seconds = static_cast<double>(*x);
  } else if (auto x = try_as<double>(&value); x and *x >= 0) {
    seconds = *x;
  } else if (auto x = try_as<std::string>(&value)) {
    auto parsed = time{};
    if (not parsers::unix_ts(*x, parsed) or parsed < time{}) {
      return false;
    }
    value = parsed;
    return true;
  } else if (is<time>(value)) {
    return true;
  } else {
    return false;
  }
  auto parsed = from_unix_timestamp(*seconds);
  if (not parsed) {
    return false;
  }
  value = *parsed;
  return true;
}

auto validate_event(record& event) -> Option<Response> {
  auto it = event.find("event");
  if (it == event.end()) {
    return hec_response(400, 12, "Event field is required");
  }
  if (is<caf::none_t>(it->second)
      or (is<std::string>(it->second)
          and as<std::string>(it->second).empty())) {
    return hec_response(400, 13, "Event field cannot be blank");
  }
  if (not is<std::string>(it->second) and not is<record>(it->second)) {
    return hec_response(400, 6, "Invalid data format");
  }
  if (auto time_it = event.find("time");
      time_it != event.end() and not normalize_time(time_it->second)) {
    return hec_response(400, 6, "Invalid data format");
  }
  if (auto fields = event.find("fields"); fields != event.end()) {
    auto const* indexed_fields = try_as<record>(&fields->second);
    if (not indexed_fields
        or not std::ranges::all_of(*indexed_fields, [](auto const& field) {
             if (is<std::string>(field.second)) {
               return true;
             }
             auto const* values = try_as<list>(&field.second);
             return values
                    and std::ranges::all_of(*values, [](data const& item) {
                          return is<std::string>(item);
                        });
           })) {
      return hec_response(400, 15, "Error in handling indexed fields");
    }
  }
  return None{};
}

auto make_receiver(std::string const& peer, Option<std::string> const& channel)
  -> record {
  auto receiver = record{};
  auto peer_ip = ip{};
  if (parsers::ip(peer, peer_ip)) {
    receiver["peer_ip"] = peer_ip;
  } else {
    receiver["peer_ip"] = peer;
  }
  if (channel) {
    receiver["channel"] = *channel;
  }
  return receiver;
}

auto parse_concatenated_events(SimdjsonPaddedBuffer const& body)
  -> Result<std::vector<record>, Response> {
  auto parser = simdjson::ondemand::parser{};
  auto stream = simdjson::ondemand::document_stream{};
  auto const* data = reinterpret_cast<char const*>(body.data());
  if (parser.iterate_many(data, body.size(), body.size()).get(stream)) {
    return Err{hec_response(400, 6, "Invalid data format")};
  }
  auto events = std::vector<record>{};
  for (auto it = stream.begin(); it != stream.end(); ++it) {
    if (it.error()) {
      return Err{hec_response(400, 6, "Invalid data format")};
    }
    auto document = *it;
    if (document.error()) {
      return Err{hec_response(400, 6, "Invalid data format")};
    }
    auto raw = document->raw_json();
    if (raw.error()) {
      return Err{hec_response(400, 6, "Invalid data format")};
    }
    auto parsed = from_json(raw.value_unsafe());
    if (not parsed) {
      return Err{hec_response(400, 6, "Invalid data format")};
    }
    auto* envelope = try_as<record>(&*parsed);
    if (not envelope) {
      return Err{hec_response(400, 6, "Invalid data format")};
    }
    events.push_back(std::move(*envelope));
  }
  if (stream.truncated_bytes() != 0) {
    return Err{hec_response(400, 6, "Invalid data format")};
  }
  return events;
}

auto parse_event_body(SimdjsonPaddedBuffer const& body,
                      RequestMetadata const& metadata)
  -> Result<std::vector<record>, Response> {
  auto text
    = std::string_view{reinterpret_cast<char const*>(body.data()), body.size()};
  text = detail::trim_front(text, json_whitespace);
  if (text.empty()) {
    return Err{hec_response(400, 5, "No data")};
  }
  auto events = std::vector<record>{};
  if (text.front() == '[') {
    auto parsed = from_json(text);
    if (not parsed) {
      return Err{hec_response(400, 6, "Invalid data format")};
    }
    auto* list = try_as<tenzir::list>(&*parsed);
    if (not list) {
      return Err{hec_response(400, 6, "Invalid data format")};
    }
    for (auto& item : *list) {
      auto* envelope = try_as<record>(&item);
      if (not envelope) {
        return Err{hec_response(400, 6, "Invalid data format")};
      }
      events.push_back(std::move(*envelope));
    }
  } else {
    auto parsed = parse_concatenated_events(body);
    if (parsed.is_err()) {
      return Err{std::move(parsed).unwrap_err()};
    }
    events = std::move(parsed).unwrap();
  }
  if (events.empty()) {
    return Err{hec_response(400, 5, "No data")};
  }
  auto receiver = make_receiver(metadata.peer_ip, metadata.channel);
  for (auto& event : events) {
    if (auto error = validate_event(event)) {
      return Err{std::move(*error)};
    }
    event.try_emplace("receiver", receiver);
  }
  return events;
}

auto make_raw_event(std::span<std::byte const> body,
                    RequestMetadata const& metadata)
  -> Result<record, Response> {
  if (body.empty()) {
    return Err{hec_response(400, 5, "No data")};
  }
  auto event = record{};
  event["raw"] = blob{body};
  for (auto const* key : {"host", "source", "sourcetype", "index"}) {
    if (auto it = metadata.query.find(key); it != metadata.query.end()) {
      event[key] = it->second;
    }
  }
  if (auto it = metadata.query.find("time"); it != metadata.query.end()) {
    auto const* value = try_as<std::string>(&it->second);
    if (not value) {
      return Err{hec_response(400, 6, "Invalid data format")};
    }
    auto parsed = data{*value};
    if (not normalize_time(parsed)) {
      return Err{hec_response(400, 6, "Invalid data format")};
    }
    event["time"] = std::move(parsed);
  }
  event["receiver"] = make_receiver(metadata.peer_ip, metadata.channel);
  return event;
}

class AcceptSplunk final : public Operator<void, table_slice> {
public:
  explicit AcceptSplunk(AcceptSplunkArgs args)
    : args_{std::move(args)},
      request_slots_{std::in_place, args_.get_max_concurrent_requests()} {
  }

  ~AcceptSplunk() noexcept override {
    force_stop();
  }
  AcceptSplunk(AcceptSplunk const&) = delete;
  AcceptSplunk(AcceptSplunk&&) noexcept = default;
  auto operator=(AcceptSplunk const&) -> AcceptSplunk& = delete;
  auto operator=(AcceptSplunk&&) -> AcceptSplunk& = delete;

  auto start(OpCtx& ctx) -> Task<void> override {
    auto token = std::string{};
    auto requests = std::vector<secret_request>{
      make_secret_request("hec_token", args_.hec_token, token, ctx.dh()),
    };
    if (auto result = co_await ctx.resolve_secrets(std::move(requests));
        result.is_error()) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    if (token.empty()) {
      diagnostic::error("`hec_token` must not be empty")
        .primary(args_.hec_token)
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto config = make_config(ctx);
    if (not config) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto request_id_generator
      = Arc<Atomic<uint64_t>>{std::in_place, uint64_t{0}};
    auto handler
      = std::make_shared<RequestHandler>(args_.get_max_request_size(),
                                         std::move(token), message_queue_,
                                         request_id_generator, request_slots_);
    auto server = co_await http_server::Server::start(std::move(*config),
                                                      std::move(handler));
    if (server.is_err()) {
      diagnostic::error("failed to start HTTP server: {}",
                        std::move(server).unwrap_err())
        .primary(args_.endpoint)
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    server_ = std::move(server).unwrap();
    bytes_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "accept_splunk"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    events_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "accept_splunk"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::events);
    lifecycle_ = Lifecycle::running;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](RequestStarted message) -> Task<void> {
        auto decompressor
          = Option<std::shared_ptr<arrow::util::Decompressor>>{None{}};
        if (not message.content_encoding.empty()) {
          decompressor
            = http::make_decompressor(message.content_encoding, ctx.dh());
          if (not decompressor) {
            message.response_signal->send(
              hec_response(415, 6, "Invalid data format"));
            co_return;
          }
        }
        active_requests_.emplace(message.request_id,
                                 InFlightRequest{
                                   .metadata = std::move(message.metadata),
                                   .response_signal
                                   = std::move(message.response_signal),
                                   .decompressor = std::move(decompressor),
                                   .body = {},
                                   .output_bytes = 0,
                                   .decompression_finished = false,
                                 });
      },
      [&](RequestBody message) -> Task<void> {
        auto it = active_requests_.find(message.request_id);
        if (it == active_requests_.end()
            or it->second.response_signal->has_sent()) {
          co_return;
        }
        auto& request = it->second;
        bytes_read_counter_.add(message.data.size());
        auto data = std::move(message.data);
        if (request.decompressor) {
          auto remaining
            = checked_sub(args_.get_max_request_size(), request.output_bytes);
          TENZIR_ASSERT(remaining);
          auto decompressed = http::decompress_chunk_with_status(
            **request.decompressor, data.view(), ctx.dh(), *remaining);
          if (decompressed.is_err()) {
            auto status = std::move(decompressed).unwrap_err();
            request.response_signal->send(
              status == 413 ? hec_response(413, 6, "Data payload too large")
                            : hec_response(status, 6, "Invalid data format"));
            co_return;
          }
          auto decoded = std::move(decompressed).unwrap();
          request.decompression_finished = decoded.finished;
          request.output_bytes += decoded.bytes.size();
          request.body.append(decoded.bytes);
          co_return;
        }
        auto output_bytes = checked_add(request.output_bytes, data.size());
        if (not output_bytes or *output_bytes > args_.get_max_request_size()) {
          request.response_signal->send(
            hec_response(413, 6, "Data payload too large"));
          co_return;
        }
        request.output_bytes = *output_bytes;
        request.body.append(data.view());
      },
      [&](RequestFinished message) -> Task<void> {
        auto it = active_requests_.find(message.request_id);
        if (it == active_requests_.end()) {
          co_return;
        }
        auto request = std::move(it->second);
        active_requests_.erase(it);
        if (request.response_signal->has_sent()) {
          co_return;
        }
        if (request.decompressor and not request.decompression_finished) {
          diagnostic::warning("rejected incomplete gzip request")
            .primary(args_.endpoint)
            .emit(ctx);
          request.response_signal->send(
            hec_response(400, 6, "Invalid data format"));
          co_return;
        }
        auto events = std::vector<record>{};
        auto schema_name = std::string_view{};
        if (request.metadata.kind == EndpointKind::event) {
          auto parsed = parse_event_body(request.body, request.metadata);
          if (parsed.is_err()) {
            request.response_signal->send(std::move(parsed).unwrap_err());
            co_return;
          }
          events = std::move(parsed).unwrap();
          schema_name = "splunk.hec.event";
        } else {
          auto parsed = make_raw_event(request.body.view(), request.metadata);
          if (parsed.is_err()) {
            request.response_signal->send(std::move(parsed).unwrap_err());
            co_return;
          }
          events.push_back(std::move(parsed).unwrap());
          schema_name = "splunk.hec.raw";
        }
        auto builder = multi_series_builder{
          multi_series_builder::policy_default{},
          {.default_schema_name = std::string{schema_name}},
          ctx.dh()};
        for (auto& event : events) {
          builder.data(data{std::move(event)});
        }
        auto slices = builder.finalize_as_table_slice();
        auto rows = uint64_t{0};
        for (auto& slice : slices) {
          rows += slice.rows();
          co_await push(std::move(slice));
        }
        events_read_counter_.add(rows);
        request.response_signal->send(
          Response{.status = 200, .body = std::string{success_body}});
      },
      [&](Noop) -> Task<void> {
        maybe_finish_draining();
        co_return;
      });
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push);
    if (lifecycle_ == Lifecycle::done) {
      co_return FinalizeBehavior::done;
    }
    begin_draining(ctx);
    maybe_finish_draining();
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    begin_draining(ctx);
    maybe_finish_draining();
    co_return;
  }

  auto state() -> OperatorState override {
    maybe_finish_draining();
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::normal;
  }

private:
  enum class Lifecycle {
    starting,
    running,
    draining,
    done,
  };

  static constexpr auto drain_timeout = std::chrono::seconds{5};

  auto make_config(OpCtx& ctx) const
    -> Option<proxygen::coro::HTTPServer::Config> {
    auto config
      = http_server::make_config(args_.endpoint.inner, args_.endpoint.source,
                                 args_.tls, ctx.actor_system().config(),
                                 ctx.dh());
    if (not config) {
      return None{};
    }
    return std::move(*config);
  }

  auto force_stop() -> void {
    if (lifecycle_ == Lifecycle::done) {
      return;
    }
    lifecycle_ = Lifecycle::done;
    drain_deadline_ = None{};
    if (server_) {
      (*server_)->force_stop();
      server_ = None{};
    }
  }

  auto begin_draining(OpCtx& ctx) -> void {
    if (lifecycle_ != Lifecycle::running) {
      return;
    }
    lifecycle_ = Lifecycle::draining;
    drain_deadline_ = std::chrono::steady_clock::now() + drain_timeout;
    ctx.spawn_task([queue = message_queue_,
                    deadline = *drain_deadline_]() mutable -> Task<void> {
      co_await sleep_until(deadline);
      co_await queue->enqueue(Noop{});
    });
    if (server_) {
      (*server_)->drain();
    }
  }

  auto maybe_finish_draining() -> void {
    if (lifecycle_ != Lifecycle::draining) {
      return;
    }
    if (drain_deadline_
        and std::chrono::steady_clock::now() >= *drain_deadline_) {
      force_stop();
      return;
    }
    if (not active_requests_.empty()
        or request_slots_->available_permits()
             != detail::narrow<size_t>(args_.get_max_concurrent_requests())
        or not message_queue_->empty()) {
      return;
    }
    drain_deadline_ = None{};
    if (server_) {
      (*server_)->finish();
      server_ = None{};
    }
    lifecycle_ = Lifecycle::done;
  }

  AcceptSplunkArgs args_;
  Arc<Semaphore> request_slots_;
  Option<Box<http_server::Server>> server_;
  std::unordered_map<uint64_t, InFlightRequest> active_requests_;
  MetricsCounter bytes_read_counter_;
  MetricsCounter events_read_counter_;
  mutable Arc<MessageQueue> message_queue_{std::in_place, uint32_t{64}};
  Lifecycle lifecycle_ = Lifecycle::starting;
  Option<std::chrono::steady_clock::time_point> drain_deadline_ = None{};
};

class AcceptSplunkPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "accept_splunk";
  }

  auto describe() const -> Description override {
    auto describer = Describer<AcceptSplunkArgs, AcceptSplunk>{};
    auto endpoint
      = describer.optional_positional("endpoint", &AcceptSplunkArgs::endpoint);
    describer.named("hec_token", &AcceptSplunkArgs::hec_token);
    auto max_request_size = describer.named(
      "max_request_size", &AcceptSplunkArgs::max_request_size);
    auto max_connections
      = describer.named("max_connections", &AcceptSplunkArgs::max_connections);
    auto tls_validator
      = tls_options{{.tls_default = false, .is_server = true}}.add_to_describer(
        describer, &AcceptSplunkArgs::tls);
    describer.validate([=](DescribeCtx& ctx) -> Empty {
      tls_validator(ctx);
      if (auto value = ctx.get(endpoint)) {
        http_server::parse_endpoint(value->inner, value->source, ctx);
      }
      if (auto value = ctx.get(max_request_size)) {
        if (value->inner == 0) {
          diagnostic::error("`max_request_size` must be greater than 0")
            .primary(value->source)
            .emit(ctx);
        } else if (value->inner > std::numeric_limits<uint32_t>::max()
                                    - compressed_size_allowance) {
          diagnostic::error("`max_request_size` is too large")
            .primary(value->source)
            .emit(ctx);
        }
      }
      if (auto value = ctx.get(max_connections)) {
        if (value->inner == 0) {
          diagnostic::error("`max_connections` must be greater than 0")
            .primary(value->source)
            .emit(ctx);
        } else if (value->inner > std::numeric_limits<uint32_t>::max()) {
          diagnostic::error("`max_connections` is too large")
            .primary(value->source)
            .emit(ctx);
        }
      }
      return {};
    });
    return describer.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::accept_splunk

TENZIR_REGISTER_PLUGIN(tenzir::plugins::accept_splunk::AcceptSplunkPlugin)
