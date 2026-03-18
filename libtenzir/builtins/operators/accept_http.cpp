//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/arc.hpp"
#include "tenzir/async.hpp"
#include "tenzir/box.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/http_server_thread.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/option.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/set.hpp"

#include <boost/url/parse.hpp>
#include <folly/ScopeGuard.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/fibers/Semaphore.h>
#include <proxygen/lib/http/coro/HTTPCoroSession.h>
#include <proxygen/lib/http/coro/HTTPSourceReader.h>
#include <proxygen/lib/http/coro/server/HTTPServer.h>
#include <proxygen/lib/services/AcceptorConfiguration.h>

#include <limits>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace tenzir::plugins {
namespace {

using namespace tenzir::si_literals;

constexpr auto default_max_request_size = 10_Mi;
constexpr auto default_listen_backlog = uint32_t{128};
constexpr auto message_queue_capacity = uint32_t{1_Ki};
constexpr auto http_1_1 = std::string_view{"http/1.1"};

template <typename T>
constexpr auto inner(std::optional<located<T>> const& x) -> std::optional<T> {
  return x.transform([](auto&& x) {
    return x.inner;
  });
};

struct AcceptHttpArgs {
  location op = location::unknown;
  located<secret> url;
  std::optional<located<record>> responses;
  std::optional<ast::field_path> metadata_field;
  std::optional<located<uint64_t>> max_request_size;
  std::optional<located<uint64_t>> max_connections;
  std::optional<located<data>> tls;
  std::optional<located<ir::pipeline>> parse;
  let_id request_let;

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    if (max_request_size and max_request_size->inner == 0) {
      diagnostic::error("`max_request_size` must not be zero")
        .primary(max_request_size->source)
        .emit(dh);
      return failure::promise();
    }
    if (max_connections and max_connections->inner == 0) {
      diagnostic::error("`max_connections` must not be zero")
        .primary(max_connections->source)
        .emit(dh);
      return failure::promise();
    }
    if (max_connections
        and max_connections->inner > std::numeric_limits<uint32_t>::max()) {
      diagnostic::error("`max_connections` must be at most {}",
                        std::numeric_limits<uint32_t>::max())
        .primary(max_connections->source)
        .emit(dh);
      return failure::promise();
    }
    if (responses) {
      TRY(http::validate_response_map(responses->inner, dh, responses->source));
    }
    if (parse) {
      auto output = parse->inner.infer_type(tag_v<chunk_ptr>, dh);
      if (not output) {
        return failure::promise();
      }
      if (*output and not(*output)->is_any<void, table_slice>()) {
        diagnostic::error("pipeline must return events or be a sink")
          .primary(*parse)
          .emit(dh);
        return failure::promise();
      }
    }
    auto tls_opts
      = tls ? tls_options{*tls, {.tls_default = false, .is_server = true}}
            : tls_options{{.tls_default = false, .is_server = true}};
    TRY(tls_opts.validate(dh));
    return {};
  }
};

struct RequestReceived {
  http::RequestData request;
};

struct Shutdown {};

using Message = variant<RequestReceived, Shutdown>;
using MessageQueue = folly::coro::BoundedQueue<Message>;

enum class Lifecycle {
  running,
  draining,
  done,
};

struct AcceptHttpServerState {
  AcceptHttpServerState(Arc<MessageQueue> message_queue,
                        Option<record> responses, size_t max_request_size,
                        Option<uint64_t> max_connections)
    : message_queue{std::move(message_queue)},
      responses_{std::move(responses)},
      max_request_size_{max_request_size} {
    if (max_connections) {
      connection_slots_ = Box<folly::fibers::Semaphore>{
        std::in_place, detail::narrow<size_t>(*max_connections)};
    }
  }

  auto responses() const -> Option<record> const& {
    return responses_;
  }

  auto max_request_size() const -> size_t {
    return max_request_size_;
  }

  auto try_acquire_connection_slot() -> bool {
    return not connection_slots_ or (*connection_slots_)->try_wait();
  }

  auto release_connection_slot() -> void {
    if (connection_slots_) {
      (*connection_slots_)->signal();
    }
  }

  Arc<MessageQueue> message_queue;

private:
  Option<record> responses_;
  size_t max_request_size_ = default_max_request_size;

  // The HTTP handler must decide synchronously whether it can hand out a new
  // request slot before the operator thread processes the corresponding
  // `RequestReceived` message, so a small semaphore-backed token pool is the
  // minimal shared surface that preserves queue-based coordination.
  Option<Box<folly::fibers::Semaphore>> connection_slots_;
};

class ConnectionLimitedSource final : public proxygen::coro::HTTPSourceFilter {
public:
  ConnectionLimitedSource(Arc<AcceptHttpServerState> state,
                          proxygen::coro::HTTPSource* source)
    : HTTPSourceFilter{source}, state_{std::move(state)} {
    setHeapAllocated();
  }

  auto readHeaderEvent()
    -> folly::coro::Task<proxygen::coro::HTTPHeaderEvent> override {
    auto event = co_await readHeaderEventImpl(/*deleteOnDone=*/false);
    if (event.eom) {
      release_connection_slot();
    }
    auto guard = folly::makeGuard(lifetime(event));
    co_return event;
  }

  auto readBodyEvent(uint32_t max = std::numeric_limits<uint32_t>::max())
    -> folly::coro::Task<proxygen::coro::HTTPBodyEvent> override {
    auto event = co_await readBodyEventImpl(max, /*deleteOnDone=*/false);
    if (event.eom) {
      release_connection_slot();
    }
    auto guard = folly::makeGuard(lifetime(event));
    co_return event;
  }

  void stopReading(
    folly::Optional<const proxygen::coro::HTTPErrorCode> error) override {
    release_connection_slot();
    HTTPSourceFilter::stopReading(error);
  }

private:
  auto release_connection_slot() -> void {
    if (released_) {
      return;
    }
    released_ = true;
    state_->release_connection_slot();
  }

  Arc<AcceptHttpServerState> state_;
  bool released_ = false;
};

class AcceptHttpHandler final : public proxygen::coro::HTTPHandler {
public:
  explicit AcceptHttpHandler(Arc<AcceptHttpServerState> state)
    : state_{std::move(state)} {
  }

  auto handleRequest(folly::EventBase*, proxygen::coro::HTTPSessionContextPtr,
                     proxygen::coro::HTTPSourceHolder request_source)
    -> folly::coro::Task<proxygen::coro::HTTPSourceHolder> override {
    if (not state_->try_acquire_connection_slot()) {
      co_return http::make_fixed_response_source(503, std::string{});
    }
    auto request = http::RequestData{};
    auto body_too_large = false;
    auto reader = proxygen::coro::HTTPSourceReader{std::move(request_source)};
    reader.onHeaders([&](std::unique_ptr<proxygen::HTTPMessage> msg,
                         bool is_final, bool /*eom*/) -> bool {
      if (not is_final) {
        return proxygen::coro::HTTPSourceReader::Continue;
      }
      request.method = msg->getMethodString();
      request.path = msg->getPath();
      request.query = http::decode_query_string(msg->getQueryString());
      auto [major, minor] = msg->getHTTPVersion();
      request.version = fmt::format("{}.{}", major, minor);
      auto parsed = boost::urls::parse_uri_reference(msg->getURL());
      if (parsed) {
        request.fragment = std::string{parsed->fragment()};
      }
      msg->getHeaders().forEach(
        [&](std::string const& name, std::string const& value) {
          request.headers.emplace_back(name, value);
        });
      return proxygen::coro::HTTPSourceReader::Continue;
    });
    reader.onBody([&](proxygen::coro::BufQueue body, bool /*eom*/) -> bool {
      auto bytes = body.chainLength();
      if (request.body.size() + bytes > state_->max_request_size()) {
        body_too_large = true;
        return proxygen::coro::HTTPSourceReader::Cancel;
      }
      auto iobuf = body.move();
      if (iobuf) {
        for (auto const& range : *iobuf) {
          auto* begin = reinterpret_cast<std::byte const*>(range.data());
          request.body.insert(request.body.end(), begin, begin + range.size());
        }
      }
      return proxygen::coro::HTTPSourceReader::Continue;
    });
    co_await reader.read();
    if (body_too_large) {
      auto response = http::make_fixed_response_source(413, std::string{});
      co_return proxygen::coro::HTTPSourceHolder{
        new ConnectionLimitedSource{state_, response.release()}};
    }
    auto response_code = uint16_t{200};
    auto content_type = std::string{};
    auto body = std::string{};
    if (auto const& responses = state_->responses(); responses) {
      if (auto response = http::lookup_response(*responses, request.path)) {
        response_code = response->code;
        content_type = response->content_type;
        body = response->body;
      }
    }
    co_await state_->message_queue->enqueue(
      RequestReceived{std::move(request)});
    auto response = http::make_fixed_response_source(
      response_code, std::move(body), content_type);
    co_return proxygen::coro::HTTPSourceHolder{
      new ConnectionLimitedSource{state_, response.release()}};
  }

private:
  Arc<AcceptHttpServerState> state_;
};

class AcceptHttp final : public Operator<void, table_slice> {
public:
  explicit AcceptHttp(AcceptHttpArgs args)
    : args_{std::move(args)}, request_let_id_{args_.request_let} {
  }

  AcceptHttp(AcceptHttp const&) = delete;
  auto operator=(AcceptHttp const&) -> AcceptHttp& = delete;
  AcceptHttp(AcceptHttp&&) noexcept = default;
  auto operator=(AcceptHttp&&) noexcept -> AcceptHttp& = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    auto& dh = ctx.dh();
    auto request = make_secret_request("url", args_.url, resolved_url_, dh);
    auto resolved = co_await ctx.resolve_secrets({std::move(request)});
    if (not resolved) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    if (resolved_url_.empty()) {
      diagnostic::error("`url` must not be empty").primary(args_.url).emit(dh);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto endpoint = http::parse_host_port_endpoint(resolved_url_);
    if (endpoint.is_err()) {
      diagnostic::error("`url` must have the form `<host>:<port>`")
        .primary(args_.url)
        .emit(dh);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    tls_options_
      = args_.tls
          ? tls_options{*args_.tls, {.tls_default = false, .is_server = true}}
          : tls_options{{.tls_default = false, .is_server = true}};
    server_state_ = Arc<AcceptHttpServerState>{
      std::in_place,
      message_queue_,
      args_.responses.transform([](auto const& x) {
        return x.inner;
      }),
      inner(args_.max_request_size).value_or(default_max_request_size),
      inner(args_.max_connections),
    };
    handler_ = std::make_shared<AcceptHttpHandler>(*server_state_);
    auto [host, port] = std::move(endpoint).unwrap();
    auto config = make_server_config(host, port, dh);
    if (not config) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    server_ = Box<::tenzir::detail::HttpServerThread>{
      std::in_place, folly::SocketAddress{host, port, true}, std::move(*config),
      handler_};
    if (auto error = (*server_)->start()) {
      diagnostic::error("failed to start http server")
        .primary(args_.url)
        .note("reason: {}", *error)
        .emit(dh);
      server_ = None{};
      lifecycle_ = Lifecycle::done;
    }
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](RequestReceived event) -> Task<void> {
        if (lifecycle_ == Lifecycle::done) {
          co_return;
        }
        auto request = std::move(event.request);
        auto request_record = http::make_request_record(request);
        if (not args_.parse) {
          auto slice = http::make_request_event(request);
          if (args_.metadata_field) {
            auto metadata = series_builder{};
            for (auto i = size_t{}; i < slice.rows(); ++i) {
              metadata.data(request_record);
            }
            slice = assign(*args_.metadata_field,
                           metadata.finish_assert_one_array(), std::move(slice),
                           ctx.dh());
          }
          co_await push(std::move(slice));
          co_return;
        }
        if (request.body.empty()) {
          co_return;
        }
        auto body = std::move(request.body);
        auto encoding
          = http::find_header_value(request.headers, "content-encoding");
        auto payload = chunk_ptr{};
        auto body_span = std::span<std::byte const>{body.data(), body.size()};
        if (auto decompressed
            = http::try_decompress_body(encoding, body_span, ctx.dh())) {
          payload = chunk::make(std::move(*decompressed));
        } else {
          payload = chunk::make(std::move(body));
        }
        auto pipeline = args_.parse->inner;
        auto env = substitute_ctx::env_t{};
        env[request_let_id_] = request_record;
        auto reg = global_registry();
        auto b_ctx = base_ctx{ctx, *reg};
        auto sub_result
          = pipeline.substitute(substitute_ctx{b_ctx, &env}, true);
        if (not sub_result) {
          co_return;
        }
        auto sub_key = data{next_sub_key_++};
        active_sub_keys_.insert(sub_key);
        if (args_.metadata_field) {
          request_metadata_by_sub_key_.emplace(sub_key, request_record);
        }
        auto sub = co_await ctx.spawn_sub(sub_key, std::move(pipeline),
                                          tag_v<chunk_ptr>);
        auto open_pipeline = as<OpenPipeline<chunk_ptr>>(sub);
        auto push_result = co_await open_pipeline.push(std::move(payload));
        if (push_result.is_err()) {
          active_sub_keys_.erase(sub_key);
          request_metadata_by_sub_key_.erase(sub_key);
          co_await request_shutdown();
          co_return;
        }
        co_await open_pipeline.close();
      },
      [&](Shutdown) -> Task<void> {
        maybe_finish_draining();
        co_return;
      });
  }

  auto process_sub(SubKeyView key, table_slice slice, Push<table_slice>& push,
                   OpCtx& ctx) -> Task<void> override {
    auto sub_key = materialize(key);
    if (active_sub_keys_.find(sub_key) == active_sub_keys_.end()) {
      co_return;
    }
    if (slice.rows() == 0) {
      co_return;
    }
    if (args_.metadata_field) {
      auto metadata_it = request_metadata_by_sub_key_.find(sub_key);
      TENZIR_ASSERT(metadata_it != request_metadata_by_sub_key_.end());
      auto metadata = series_builder{};
      for (auto i = size_t{}; i < slice.rows(); ++i) {
        metadata.data(metadata_it->second);
      }
      slice = assign(*args_.metadata_field, metadata.finish_assert_one_array(),
                     std::move(slice), ctx.dh());
    }
    co_await push(std::move(slice));
  }

  auto finish_sub(SubKeyView key, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    auto sub_key = materialize(key);
    active_sub_keys_.erase(sub_key);
    request_metadata_by_sub_key_.erase(sub_key);
    maybe_finish_draining();
    co_return;
  }

  auto finalize(Push<table_slice>&, OpCtx&) -> Task<FinalizeBehavior> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return FinalizeBehavior::done;
    }
    co_await request_shutdown();
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto state() -> OperatorState override {
    maybe_finish_draining();
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::unspecified;
  }

private:
  auto make_server_config(std::string const& host, uint16_t port,
                          diagnostic_handler& dh)
    -> failure_or<std::shared_ptr<const proxygen::AcceptorConfiguration>> {
    auto config = std::make_shared<proxygen::AcceptorConfiguration>();
    config->plaintextProtocol = std::string{http_1_1};
    config->forceHTTP1_0_to_1_1 = true;
    config->bindAddress = folly::SocketAddress{host, port, true};
    config->acceptBacklog = inner(args_.max_connections)
                              .transform([](auto value) {
                                return detail::narrow<uint32_t>(value);
                              })
                              .value_or(default_listen_backlog);
    config->maxNumPendingConnectionsPerWorker = config->acceptBacklog;
    if (not tls_options_.get_tls(nullptr).inner) {
      return config;
    }
    auto certfile = tls_options_.get_certfile(nullptr);
    auto keyfile = tls_options_.get_keyfile(nullptr);
    if (not certfile or not keyfile) {
      diagnostic::error(
        "TLS server mode requires `tls.certfile` and `tls.keyfile`")
        .primary(args_.url)
        .emit(dh);
      return failure::promise();
    }
    auto tls_config = proxygen::coro::HTTPServer::getDefaultTLSConfig();
    try {
      tls_config.setCertificate(certfile->inner, keyfile->inner, "");
    } catch (std::exception const& ex) {
      diagnostic::error("failed to configure TLS server certificate")
        .primary(args_.url)
        .note("reason: {}", ex.what())
        .emit(dh);
      return failure::promise();
    }
    config->sslContextConfigs.emplace_back(std::move(tls_config));
    return config;
  }

  auto stop_server() -> void {
    server_ = None{};
    handler_.reset();
    server_state_ = None{};
  }

  auto begin_draining() -> bool {
    if (lifecycle_ != Lifecycle::running) {
      return false;
    }
    lifecycle_ = Lifecycle::draining;
    stop_server();
    return true;
  }

  auto request_shutdown() -> Task<void> {
    if (begin_draining()) {
      co_await message_queue_->enqueue(Shutdown{});
    }
    maybe_finish_draining();
    co_return;
  }

  auto maybe_finish_draining() -> void {
    if (lifecycle_ == Lifecycle::draining and active_sub_keys_.empty()) {
      lifecycle_ = Lifecycle::done;
    }
  }

  AcceptHttpArgs args_;
  std::string resolved_url_;
  mutable Arc<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Option<Arc<AcceptHttpServerState>> server_state_;
  Option<Box<::tenzir::detail::HttpServerThread>> server_;
  std::shared_ptr<AcceptHttpHandler> handler_;
  std::unordered_set<data> active_sub_keys_;
  std::unordered_map<data, record> request_metadata_by_sub_key_;
  let_id request_let_id_;
  uint64_t next_sub_key_ = 0;
  tls_options tls_options_{{.tls_default = false, .is_server = true}};
  Lifecycle lifecycle_ = Lifecycle::running;
};

} // namespace

struct AcceptHttpPlugin final : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return "tql2.accept_http";
  }

  auto describe() const -> Description override {
    auto d = Describer<AcceptHttpArgs, AcceptHttp>{};
    d.operator_location(&AcceptHttpArgs::op);
    auto url = d.positional("url", &AcceptHttpArgs::url);
    auto responses = d.named("responses", &AcceptHttpArgs::responses);
    auto metadata_field
      = d.named("metadata_field", &AcceptHttpArgs::metadata_field);
    auto max_request_size
      = d.named("max_request_size", &AcceptHttpArgs::max_request_size);
    auto max_connections
      = d.named("max_connections", &AcceptHttpArgs::max_connections);
    auto tls = d.named("tls", &AcceptHttpArgs::tls);
    auto parse = d.pipeline(&AcceptHttpArgs::parse,
                            {{"request", &AcceptHttpArgs::request_let}});
    d.validate([=](DescribeCtx& ctx) -> Empty {
      auto args = AcceptHttpArgs{};
      args.op = ctx.get_location(url).value_or(location::unknown);
      if (auto x = ctx.get(url)) {
        args.url = *x;
      }
      if (auto x = ctx.get(responses)) {
        args.responses = *x;
      }
      if (auto x = ctx.get(metadata_field)) {
        args.metadata_field = *x;
      }
      if (auto x = ctx.get(max_request_size)) {
        args.max_request_size = *x;
      }
      if (auto x = ctx.get(max_connections)) {
        args.max_connections = *x;
      }
      if (auto x = ctx.get(tls)) {
        args.tls = *x;
      }
      if (auto x = ctx.get(parse)) {
        args.parse = *x;
      }
      std::ignore = args.validate(ctx);
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace tenzir::plugins

TENZIR_REGISTER_PLUGIN(tenzir::plugins::AcceptHttpPlugin)
