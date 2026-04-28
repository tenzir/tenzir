//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/async/mutex.hpp>
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
#include <span>
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

struct RequestStarted {
  uint64_t request_id;
  std::string content_encoding;
  Arc<ResponseSignal> response_signal;
};

struct RequestBody {
  uint64_t request_id;
  std::vector<std::byte> data;
};

struct RequestFinished {
  uint64_t request_id;
};

struct Noop {};

using Message = variant<Noop, RequestStarted, RequestBody, RequestFinished>;
using MessageQueue = folly::coro::BoundedQueue<Message>;

class failing_diagnostic_handler final : public diagnostic_handler {
public:
  failing_diagnostic_handler(diagnostic_handler& inner, bool& failed)
    : inner_{inner}, failed_{failed} {
  }

  void emit(diagnostic d) override {
    if (d.severity == severity::warning or d.severity == severity::error) {
      failed_ = true;
    }
    inner_.emit(std::move(d));
  }

private:
  diagnostic_handler& inner_;
  bool& failed_;
};

auto handle_slice(bool& is_action, table_slice const& slice) -> table_slice {
  if (slice.rows() == 0) {
    return {};
  }
  auto ty = as<record_type>(slice.schema());
  auto fields = std::vector<record_type::field_view>{};
  for (auto const& field : ty.fields()) {
    fields.push_back(field);
  }
  auto delete_ = std::ranges::any_of(
    fields,
    [](auto&& x) {
      return x == "delete";
    },
    &record_type::field_view::name);
  auto other_actions = std::ranges::any_of(
    fields,
    [](auto&& x) {
      return x == "create" or x == "index" or x == "update";
    },
    &record_type::field_view::name);
  if (delete_) {
    return is_action ? table_slice{} : subslice(slice, 0, 1);
  }
  if (other_actions) {
    auto filtered = std::vector<table_slice>{};
    if (is_action) {
      is_action = slice.rows() % 2 == 0;
      for (auto i = size_t{1}; i < slice.rows(); i += 2) {
        filtered.push_back(subslice(slice, i, i + 1));
      }
      return concatenate(filtered);
    }
    is_action = slice.rows() % 2 != 0;
    for (auto i = size_t{}; i < slice.rows(); i += 2) {
      filtered.push_back(subslice(slice, i, i + 1));
    }
    return concatenate(filtered);
  }
  if (is_action) {
    is_action = slice.rows() % 2 == 0;
    return {};
  }
  is_action = slice.rows() % 2 != 0;
  return subslice(slice, 0, 1);
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
  if (path != "/_bulk") {
    return Response{
      .status = 200,
      .content_type = method == proxygen::HTTPMethod::HEAD
                        ? ""
                        : std::string{bulk_content_type},
      .body = method == proxygen::HTTPMethod::HEAD ? "" : "{}",
    };
  }
  return None{};
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
          auto data = std::vector<std::byte>(iobuf->length());
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
    permit.release();
    co_await folly::coro::co_reschedule_on_current_executor;
    co_await queue_->enqueue(Noop{});
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

  auto start(OpCtx& ctx) -> Task<void> override {
    auto config = co_await make_config(ctx);
    if (not config) {
      lifecycle_ = Lifecycle::done;
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
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    // When the operator is forcefully stopped (e.g., by `head 1` finishing),
    // the executor cancels the operator scope. This task catches that
    // cancellation, unblocks any in-flight handlers, then drains the server
    // while the main EventBase is still running.
    ctx.spawn_task([this]() -> Task<void> {
      co_await catch_cancellation(wait_forever());
      {
        auto active_requests = co_await active_requests_.lock();
        for (auto& [_, req] : *active_requests) {
          req.response_signal->send(
            Response{.status = 503,
                     .content_type = std::string{bulk_content_type},
                     .body = "{}"});
        }
      }
      if (server_) {
        (*server_)->getServer().drain();
        // The ScopedHTTPServer destructor blocks on thread_.join(), so
        // run it in a detached thread to avoid blocking the executor fiber.
        auto srv = std::move(*server_);
        server_ = None{};
        std::thread([srv = std::move(srv)]() {
          // srv destructor calls drain()+forceStop()+thread_.join()
        }).detach();
      }
    });
    bytes_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "accept_opensearch"},
                         MetricsDirection::read, MetricsVisibility::external_);
    lifecycle_ = Lifecycle::running;
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
        if (lifecycle_ != Lifecycle::running) {
          msg.response_signal->send(
            Response{.status = 503,
                     .content_type = std::string{bulk_content_type},
                     .body = "{}"});
          co_return;
        }
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
        auto requests = co_await active_requests_.lock();
        requests->emplace(msg.request_id,
                          InFlightRequest{
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
        {
          auto requests = co_await active_requests_.lock();
          auto it = requests->find(msg.request_id);
          if (it == requests->end() or it->second.response_signal->has_sent()) {
            co_return;
          }
          auto& req = it->second;
          response_signal = req.response_signal;
          decompressor = req.decompressor;
          is_action = req.is_action;
          failed = req.failed;
          auto data = blob{std::span{msg.data}};
          auto failing_dh = failing_diagnostic_handler{ctx.dh(), failed};
          if (decompressor) {
            auto remaining = args_.get_max_request_size() - req.output_bytes;
            auto decompressed = http::decompress_chunk(
              **decompressor,
              std::span<std::byte const>{msg.data.data(), msg.data.size()},
              failing_dh, remaining);
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
          slices
            = req.parser.parse_chunk(data, "accept_opensearch", failing_dh);
          req.failed = failed;
        }
        if (failed) {
          (*response_signal)
            ->send(Response{.status = 400,
                            .content_type = std::string{bulk_content_type},
                            .body = "{}"});
          co_return;
        }
        if (args_.keep_actions) {
          for (auto& slice : slices) {
            if (slice.rows() > 0) {
              co_await push(std::move(slice));
            }
          }
        } else {
          for (auto& slice : slices) {
            auto filtered = handle_slice(is_action, slice);
            if (filtered.rows() > 0) {
              co_await push(std::move(filtered));
            }
          }
        }
        auto requests = co_await active_requests_.lock();
        auto it = requests->find(msg.request_id);
        if (it != requests->end()) {
          it->second.is_action = is_action;
        }
      },
      [&](RequestFinished msg) -> Task<void> {
        auto req = Option<InFlightRequest>{None{}};
        {
          auto requests = co_await active_requests_.lock();
          auto it = requests->find(msg.request_id);
          if (it == requests->end()) {
            co_return;
          }
          req = std::move(it->second);
          requests->erase(it);
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
        if (args_.keep_actions) {
          for (auto& slice : slices) {
            if (slice.rows() > 0) {
              co_await push(std::move(slice));
            }
          }
        } else {
          auto is_action = req->is_action;
          for (auto& slice : slices) {
            auto filtered = handle_slice(is_action, slice);
            if (filtered.rows() > 0) {
              co_await push(std::move(filtered));
            }
          }
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
    co_await maybe_finish_draining();
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push, ctx);
    TENZIR_ASSERT(lifecycle_ == Lifecycle::done);
    co_return FinalizeBehavior::done;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await request_stop();
  }

  auto state() -> OperatorState override {
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("lifecycle", lifecycle_);
  }

private:
  enum class Lifecycle {
    running,
    draining,
    done,
  };

  friend auto inspect(auto& f, Lifecycle& x) {
    return tenzir::detail::inspect_enum_str(f, x,
                                            {"running", "draining", "done"});
  }

  static auto get_max_connections() -> uint64_t {
    return 10;
  }

  auto request_stop() -> Task<void> {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    lifecycle_ = Lifecycle::draining;
    if (server_) {
      (*server_)->getServer().drain();
    }
    co_await maybe_finish_draining();
  }

  auto maybe_finish_draining() -> Task<void> {
    if (lifecycle_ != Lifecycle::draining) {
      co_return;
    }
    if (active_connections_->available_permits() != get_max_connections()) {
      co_return;
    }
    auto active_requests = co_await active_requests_.lock();
    if (active_requests->empty() and message_queue_->empty()) {
      server_ = None{};
      lifecycle_ = Lifecycle::done;
    }
  }

  auto make_config(OpCtx& ctx) const
    -> Task<Option<proxygen::coro::HTTPServer::Config>> {
    auto parsed = http_server::parse_endpoint(args_.url.inner, args_.url.source,
                                              ctx.dh(), "url");
    if (not parsed) {
      co_return None{};
    }
    auto tls_enabled = http_server::is_tls_enabled(args_.tls);
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
    config.sessionConfig.connIdleTimeout = std::chrono::milliseconds{200};
    if (tls_enabled) {
      auto tls_opts = tls_options::from_optional(
        args_.tls, {.tls_default = false, .is_server = true});
      auto tls_config = http_server::make_ssl_context_config(
        tls_opts, args_.url.source, ctx.dh());
      if (not tls_config) {
        co_return None{};
      }
      config.socketConfig.sslContextConfigs.emplace_back(
        std::move(*tls_config));
    }
    co_return config;
  }

  // --- args ---
  AcceptOpenSearchArgs args_;
  // --- transient ---
  Arc<Semaphore> active_connections_;
  Option<Arc<proxygen::coro::ScopedHTTPServer>> server_;
  Mutex<std::unordered_map<uint64_t, InFlightRequest>> active_requests_{{}};
  MetricsCounter bytes_read_counter_;
  mutable Arc<MessageQueue> message_queue_{std::in_place, uint32_t{64}};
  // --- state ---
  Lifecycle lifecycle_ = Lifecycle::running;
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
