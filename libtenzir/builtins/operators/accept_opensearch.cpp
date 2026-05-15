//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/async/oneshot.hpp>
#include <tenzir/async/semaphore.hpp>
#include <tenzir/atomic.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/inspect_enum_str.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_server.hpp>
#include <tenzir/json_parser.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/variant.hpp>

#include <folly/coro/BoundedQueue.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/SSLContext.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/coro/HTTPCoroSession.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/HTTPSourceReader.h>
#include <proxygen/lib/http/coro/server/HTTPServer.h>
#include <proxygen/lib/http/coro/server/ScopedHTTPServer.h>
#include <proxygen/lib/utils/URL.h>

#include <cstddef>
#include <cstring>
#include <limits>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tenzir::plugins::accept_opensearch {

namespace {

constexpr auto default_max_request_size = size_t{10 * 1024 * 1024};
constexpr auto bulk_content_type = std::string_view{"application/x-ndjson"};
constexpr auto info_response = std::string_view{
  R"({"name":"hostname","cluster_name":"opensearch","cluster_uuid":"rTLctDY8SoqcaEkfmuyGFA","version":{"distribution":"opensearch","number":"8.17.0","build_flavor":"default","build_type":"tar","build_hash":"unknown","build_date":"2025-02-21T09:34:11Z","build_snapshot":false,"lucene_version":"9.12.1","minimum_wire_compatibility_version":"7.10.0","minimum_index_compatibility_version":"7.0.0"},"tagline":"Tenzir accept_opensearch"})"};
constexpr auto bulk_response = std::string_view{
  R"({"errors":false,"items":[{"create":{"status":201,"result":"created"}}]})"};

struct AcceptOpenSearchArgs {
  located<std::string> url{"0.0.0.0:9200", location::unknown};
  bool keep_actions = false;
  Option<located<uint64_t>> max_request_size;
  Option<located<data>> tls;

  auto get_max_request_size() const -> size_t {
    if (not max_request_size) {
      return default_max_request_size;
    }
    return detail::narrow<size_t>(max_request_size->inner);
  }
};

struct Response {
  uint16_t status;
  std::string content_type;
  std::string body;
};

using ResponseSignal = Oneshot<Response>;

// needed to make Message default-constructable
struct Noop {};

struct RequestStarted {
  uint64_t request_id;
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

using Message = variant<Noop, RequestStarted, RequestBody, RequestFinished>;
using MessageQueue = folly::coro::BoundedQueue<Message>;

class failing_diagnostic_handler final : public diagnostic_handler {
public:
  failing_diagnostic_handler(diagnostic_handler& inner, bool& failed)
    : inner_{inner}, failed_{failed} {
  }

  void emit(diagnostic d) override {
    if (d.severity == severity::error) {
      failed_ = true;
    }
    inner_.emit(std::move(d));
  }

private:
  diagnostic_handler& inner_;
  bool& failed_;
};

auto is_delete_action(view3<record> row) -> bool {
  auto found_delete = false;
  for (auto const& [key, value] : row) {
    if (is<caf::none_t>(value)) {
      continue;
    }
    if (key != "delete") {
      return false;
    }
    if (not try_as<view3<record>>(value)) {
      return false;
    }
    found_delete = true;
  }
  return found_delete;
}

auto handle_slice(bool& is_action, table_slice const& slice) -> table_slice {
  if (slice.rows() == 0) {
    return {};
  }
  auto filtered = std::vector<table_slice>{};
  auto row = size_t{0};
  for (auto record : slice.values()) {
    if (is_action) {
      is_action = is_delete_action(record);
    } else {
      filtered.push_back(subslice(slice, row, row + 1));
      is_action = true;
    }
    ++row;
  }
  return concatenate(filtered);
}

auto is_bulk_ingest_path(std::string_view path) -> bool {
  constexpr auto bulk_path = std::string_view{"/_bulk"};
  if (path == bulk_path) {
    return true;
  }
  if (not path.ends_with(bulk_path)) {
    return false;
  }
  auto suffix_offset = path.size() - bulk_path.size();
  auto second_slash = path.find('/', 1);
  return second_slash != std::string_view::npos
         and second_slash == suffix_offset;
}

auto get_static_opensearch_response(proxygen::HTTPMessage const& msg)
  -> Option<Response> {
  auto path = std::string_view{msg.getPathAsStringPiece()};
  auto method = msg.getMethod();
  if (method == proxygen::HTTPMethod::GET and path == "/") {
    return Response{
      .status = 200,
      .content_type = std::string{bulk_content_type},
      .body = std::string{info_response},
    };
  }
  if (is_bulk_ingest_path(path)) {
    // These requests carry data we want to ingest.
    // Respond only after the bulk payload was processed.
    return None{};
  }
  if (method == proxygen::HTTPMethod::HEAD) {
    return Response{
      .status = 200,
      .content_type = "",
      .body = "",
    };
  }
  return Response{
    .status = 200,
    .content_type = std::string{bulk_content_type},
    .body = "{}",
  };
}

class RequestHandler final : public proxygen::coro::HTTPHandler {
public:
  RequestHandler(AcceptOpenSearchArgs args, Arc<MessageQueue> queue,
                 Arc<Atomic<uint64_t>> request_id_gen,
                 Arc<Semaphore> active_connections)
    : args_{std::move(args)},
      queue_{std::move(queue)},
      request_id_gen_{std::move(request_id_gen)},
      active_connections_{std::move(active_connections)} {
  }

  auto handleRequest(folly::EventBase*, proxygen::coro::HTTPSessionContextPtr,
                     proxygen::coro::HTTPSourceHolder request_source)
    -> folly::coro::Task<proxygen::coro::HTTPSourceHolder> override {
    auto permit = co_await active_connections_->acquire();
    auto reader = proxygen::coro::HTTPSourceReader{std::move(request_source)};
    auto request_id = request_id_gen_->fetch_add(1, std::memory_order_relaxed);
    auto bytes_received = size_t{};
    auto response_signal = Arc<ResponseSignal>{std::in_place};
    auto started = false;
    reader
      .onHeadersAsync([&](std::unique_ptr<proxygen::HTTPMessage> msg,
                          bool is_final, bool) -> folly::coro::Task<bool> {
        if (not is_final) {
          co_return proxygen::coro::HTTPSourceReader::Continue;
        }
        if (auto response = get_static_opensearch_response(*msg)) {
          response_signal->send(std::move(*response));
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }
        auto content_length_header = std::string_view{
          msg->getHeaders().getSingleOrEmpty("Content-Length")};
        if (auto content_length
            = http_server::parse_number<size_t>(content_length_header);
            content_length and *content_length > args_.get_max_request_size()) {
          response_signal->send(
            Response{.status = 413,
                     .content_type = std::string{bulk_content_type},
                     .body = "{}"});
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }
        auto content_encoding
          = std::string{msg->getHeaders().getSingleOrEmpty("Content-Encoding")};
        co_await queue_->enqueue(
          RequestStarted{.request_id = request_id,
                         .content_encoding = std::move(content_encoding),
                         .response_signal = response_signal});
        started = true;
        co_return proxygen::coro::HTTPSourceReader::Continue;
      })
      .onBodyAsync([&](quic::BufQueue queue, bool) -> folly::coro::Task<bool> {
        if (response_signal->has_sent()) {
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }
        if (not queue.empty()) {
          auto iobuf = queue.move();
          iobuf->coalesce();
          bytes_received += iobuf->length();
          if (bytes_received > args_.get_max_request_size()) {
            response_signal->send(
              Response{.status = 413,
                       .content_type = std::string{bulk_content_type},
                       .body = "{}"});
            co_return proxygen::coro::HTTPSourceReader::Cancel;
          }
          auto data = SimdjsonPaddedBuffer{iobuf->length()};
          std::memcpy(data.data(), iobuf->data(), iobuf->length());
          co_await queue_->enqueue(
            RequestBody{.request_id = request_id, .data = std::move(data)});
        }
        co_return proxygen::coro::HTTPSourceReader::Continue;
      })
      .onError([&](proxygen::coro::HTTPSourceReader::ErrorContext,
                   proxygen::coro::HTTPError const&) {
        if (not response_signal->has_sent()) {
          response_signal->send(
            Response{.status = 400,
                     .content_type = std::string{bulk_content_type},
                     .body = "{}"});
        }
      });
    co_await reader.read(
      detail::narrow<uint32_t>(args_.get_max_request_size()));
    if (started) {
      co_await queue_->enqueue(RequestFinished{.request_id = request_id});
    }
    auto response = co_await response_signal->recv();
    co_await folly::coro::co_reschedule_on_current_executor;
    co_return http_server::make_response(response.status, response.content_type,
                                         std::move(response.body));
  }

private:
  AcceptOpenSearchArgs args_;
  Arc<MessageQueue> queue_;
  Arc<Atomic<uint64_t>> request_id_gen_;
  Arc<Semaphore> active_connections_;
};

struct InFlightRequest {
  Arc<ResponseSignal> response_signal;
  Option<std::shared_ptr<arrow::util::Decompressor>> decompressor;
  size_t output_bytes = 0;
  json::streaming_ndjson_parser parser;
  bool is_action = true;
  bool failed = false;
};

class AcceptOpenSearch final : public Operator<void, table_slice> {
public:
  explicit AcceptOpenSearch(AcceptOpenSearchArgs args)
    : args_{std::move(args)},
      active_connections_{std::in_place, get_max_connections()} {
  }

  ~AcceptOpenSearch() noexcept override {
    force_stop();
  }
  AcceptOpenSearch(const AcceptOpenSearch&) = default;
  AcceptOpenSearch(AcceptOpenSearch&&) = default;
  AcceptOpenSearch& operator=(const AcceptOpenSearch&) = default;
  AcceptOpenSearch& operator=(AcceptOpenSearch&&) = delete;

  auto start(OpCtx& ctx) -> Task<void> override {
    auto config = co_await make_config(ctx);
    if (not config) {
      co_return;
    }
    auto request_id_gen = Arc<Atomic<uint64_t>>{std::in_place, uint64_t{0}};
    auto request_handler = std::make_shared<RequestHandler>(
      args_, message_queue_, request_id_gen, active_connections_);
    try {
      auto server = proxygen::coro::ScopedHTTPServer::start(
        std::move(config.unwrap()), std::move(request_handler));
      server_ = Arc<proxygen::coro::ScopedHTTPServer>::from_non_null(
        std::move(server));
    } catch (std::exception const& ex) {
      diagnostic::error("failed to start HTTP server: {}", ex.what())
        .primary(args_.url)
        .emit(ctx);
      co_return;
    }
    // When the operator is forcefully stopped (e.g., by `head 1` finishing),
    // the executor skips `finish_sub` and cancels the operator scope. Catch
    // that cancellation, reply to already-accepted requests ourselves, then
    // drain and destroy the server while the proxygen EventBase is still
    // running.
    ctx.spawn_task([this]() -> Task<void> {
      co_await catch_cancellation(wait_forever());
      force_stop();
    });
    bytes_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "accept_opensearch"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    events_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "accept_opensearch"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::events);
    co_return;
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
      [&](RequestStarted msg) -> Task<void> {
        auto decompressor
          = Option<std::shared_ptr<arrow::util::Decompressor>>{None{}};
        auto failed = false;
        if (not msg.content_encoding.empty()) {
          auto failing_dh = failing_diagnostic_handler{ctx.dh(), failed};
          decompressor
            = http::make_decompressor(msg.content_encoding, failing_dh);
          if (not decompressor) {
            msg.response_signal->send(
              Response{.status = 415,
                       .content_type = std::string{bulk_content_type},
                       .body = "{}"});
            co_return;
          }
        }
        active_requests_.emplace(
          msg.request_id, InFlightRequest{
                            .response_signal = std::move(msg.response_signal),
                            .decompressor = std::move(decompressor),
                            .parser = {},
                            .is_action = true,
                            .failed = failed,
                          });
      },
      [&](RequestBody msg) -> Task<void> {
        auto response_signal = Option<Arc<ResponseSignal>>{None{}};
        auto decompressor
          = Option<std::shared_ptr<arrow::util::Decompressor>>{None{}};
        auto is_action = true;
        auto failed = false;
        auto slices = std::vector<table_slice>{};
        auto it = active_requests_.find(msg.request_id);
        if (it == active_requests_.end()
            or it->second.response_signal->has_sent()) {
          co_return;
        }
        auto& req = it->second;
        response_signal = req.response_signal;
        decompressor = req.decompressor;
        is_action = req.is_action;
        failed = req.failed;
        auto data = std::move(msg.data);
        auto failing_dh = failing_diagnostic_handler{ctx.dh(), failed};
        if (decompressor) {
          auto remaining = args_.get_max_request_size() - req.output_bytes;
          auto decompressed = http::decompress_chunk_simdjson(
            **decompressor, data.view(), failing_dh, remaining);
          if (decompressed.is_err()) {
            (*response_signal)
              ->send(Response{.status = std::move(decompressed).unwrap_err(),
                              .content_type = std::string{bulk_content_type},
                              .body = "{}"});
            co_return;
          }
          data = std::move(decompressed).unwrap();
        }
        req.output_bytes += data.size();
        if (req.output_bytes > args_.get_max_request_size()) {
          diagnostic::warning("request body exceeds `max_request_size`")
            .primary(args_.url)
            .note("rejecting request body")
            .emit(ctx);
          (*response_signal)
            ->send(Response{.status = 413,
                            .content_type = std::string{bulk_content_type},
                            .body = "{}"});
          co_return;
        }
        bytes_read_counter_.add(data.size());
        slices = req.parser.parse_chunk(data, "accept_opensearch", failing_dh);
        req.failed = failed;
        if (failed) {
          (*response_signal)
            ->send(Response{.status = 400,
                            .content_type = std::string{bulk_content_type},
                            .body = "{}"});
          co_return;
        }
        for (auto& slice : slices) {
          auto filtered = handle_slice(is_action, slice);
          if (args_.keep_actions) {
            if (auto rows = slice.rows(); rows > 0) {
              events_read_counter_.add(rows);
              co_await push(std::move(slice));
            }
          } else {
            if (auto rows = filtered.rows(); rows > 0) {
              events_read_counter_.add(rows);
              co_await push(std::move(filtered));
            }
          }
        }
        req.is_action = is_action;
      },
      [&](RequestFinished msg) -> Task<void> {
        auto req = Option<InFlightRequest>{None{}};
        {
          auto it = active_requests_.find(msg.request_id);
          if (it == active_requests_.end()) {
            co_return;
          }
          req = std::move(it->second);
          active_requests_.erase(it);
        }
        if (req->response_signal->has_sent()) {
          co_return;
        }
        auto failing_dh = failing_diagnostic_handler{ctx.dh(), req->failed};
        auto slices = req->parser.finish("accept_opensearch", failing_dh);
        if (req->failed) {
          req->response_signal->send(
            Response{.status = 400,
                     .content_type = std::string{bulk_content_type},
                     .body = "{}"});
          co_return;
        }
        auto is_action = req->is_action;
        for (auto& slice : slices) {
          auto filtered = handle_slice(is_action, slice);
          if (args_.keep_actions) {
            if (auto rows = slice.rows(); rows > 0) {
              events_read_counter_.add(rows);
              co_await push(std::move(slice));
            }
          } else {
            if (auto rows = filtered.rows(); rows > 0) {
              events_read_counter_.add(rows);
              co_await push(std::move(filtered));
            }
          }
        }
        if (not is_action) {
          req->response_signal->send(
            Response{.status = 400,
                     .content_type = std::string{bulk_content_type},
                     .body = "{}"});
          co_return;
        }
        req->response_signal->send(Response{
          .status = 200,
          .content_type = std::string{bulk_content_type},
          .body = std::string{bulk_response},
        });
      },
      [&](Noop) -> Task<void> {
        co_return;
      });
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push, ctx);
    co_return FinalizeBehavior::done;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    force_stop();
    co_return;
  }

  auto state() -> OperatorState override {
    return not server_ ? OperatorState::done : OperatorState::normal;
  }

private:
  static auto get_max_connections() -> uint64_t {
    return 10;
  }

  void force_stop() {
    if (server_) {
      (*server_)->getServer().forceStop();
      // move server to a new thread, where it can call thread join
      std::thread([srv = std::exchange(server_, None{})] {}).detach();
    }
  }

  auto make_config(OpCtx& ctx) const
    -> Task<Option<proxygen::coro::HTTPServer::Config>> {
    auto const* cfg = std::addressof(ctx.actor_system().config());
    auto parsed = http_server::parse_endpoint(args_.url.inner, args_.url.source,
                                              ctx.dh(), "url");
    if (not parsed) {
      co_return None{};
    }
    auto tls_enabled = http_server::is_tls_enabled(args_.tls, cfg);
    if (parsed->scheme_tls) {
      if (*parsed->scheme_tls and not tls_enabled) {
        diagnostic::error("`https://` endpoint requires `tls=true`")
          .primary(args_.url)
          .emit(ctx);
        co_return None{};
      }
      if (not *parsed->scheme_tls and tls_enabled) {
        diagnostic::error("`http://` endpoint requires `tls=false`")
          .primary(args_.url)
          .emit(ctx);
        co_return None{};
      }
    }
    auto config = proxygen::coro::HTTPServer::Config{};
    try {
      config.socketConfig.bindAddress.setFromHostPort(parsed->host,
                                                      parsed->port);
    } catch (std::exception const& ex) {
      diagnostic::error("failed to configure HTTP endpoint: {}", ex.what())
        .primary(args_.url)
        .emit(ctx);
      co_return None{};
    }
    config.numIOThreads = 1;
    if (tls_enabled) {
      auto tls_opts = tls_options::from_optional(
        args_.tls, {.tls_default = false, .is_server = true});
      auto tls_config = http_server::make_ssl_context_config(
        tls_opts, args_.url.source, ctx, cfg);
      if (not tls_config) {
        co_return None{};
      }
      config.socketConfig.sslContextConfigs.emplace_back(
        std::move(*tls_config));
    }
    config.shutdownOnSignals = {};
    co_return config;
  }

  // --- args ---
  AcceptOpenSearchArgs args_;
  // --- transient ---
  Arc<Semaphore> active_connections_;
  Option<Arc<proxygen::coro::ScopedHTTPServer>> server_;
  std::unordered_map<uint64_t, InFlightRequest> active_requests_{{}};
  MetricsCounter bytes_read_counter_;
  MetricsCounter events_read_counter_;
  mutable Arc<MessageQueue> message_queue_{std::in_place, uint32_t{64}};
};

class AcceptOpenSearchPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "accept_opensearch";
  }

  auto describe() const -> Description override {
    auto d = Describer<AcceptOpenSearchArgs, AcceptOpenSearch>{};
    auto url_arg = d.positional("url", &AcceptOpenSearchArgs::url);
    d.named("keep_actions", &AcceptOpenSearchArgs::keep_actions);
    auto max_request_size_arg
      = d.named("max_request_size", &AcceptOpenSearchArgs::max_request_size);
    auto tls_validator
      = tls_options{{.tls_default = false, .is_server = true}}.add_to_describer(
        d, &AcceptOpenSearchArgs::tls);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      tls_validator(ctx);
      auto parsed = Option<http_server::server_endpoint>{None{}};
      if (auto url = ctx.get(url_arg)) {
        http_server::parse_endpoint(url->inner, url->source, ctx, "url");
      }
      if (auto max_request_size = ctx.get(max_request_size_arg)) {
        if (max_request_size->inner == 0) {
          diagnostic::error("`max_request_size` must be greater than 0")
            .primary(max_request_size->source)
            .emit(ctx);
        } else if (max_request_size->inner > static_cast<uint64_t>(
                     std::numeric_limits<uint32_t>::max())) {
          diagnostic::error("`max_request_size` is too large")
            .primary(max_request_size->source)
            .note("maximum supported value: {}",
                  std::numeric_limits<uint32_t>::max())
            .emit(ctx);
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::accept_opensearch

TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::accept_opensearch::AcceptOpenSearchPlugin)
