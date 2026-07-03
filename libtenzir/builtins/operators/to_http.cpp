//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/async/channel.hpp>
#include <tenzir/async/oneshot.hpp>
#include <tenzir/async/result.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/box.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/http_proxy_connect.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/result.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try.hpp>

#include <folly/SocketAddress.h>
#include <folly/coro/Sleep.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/AsyncSocketException.h>
#include <proxygen/lib/http/HTTPMethod.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/HTTPSource.h>
#include <proxygen/lib/http/coro/client/HTTPClient.h>
#include <proxygen/lib/utils/URL.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::http {
auto to_http_response(proxygen::coro::HTTPClient::Response& resp) -> Response;
} // namespace tenzir::http

namespace tenzir::plugins::to_http {

namespace {

using Headers = std::vector<http::Header>;

/// Strip platform-specific errno details from exception messages so that
/// diagnostic output is stable across operating systems.
auto scrub_errno(std::string msg) -> std::string {
  // Matches ", errno = 111 (Connection refused)" and similar.
  static constexpr auto prefix = std::string_view{", errno = "};
  if (auto pos = msg.find(prefix); pos != std::string::npos) {
    msg.erase(pos);
  }
  return msg;
}

struct ToHttpArgs {
  located<secret> url;
  Option<located<std::string>> method;
  Option<located<data>> headers;
  Option<located<data>> tls;
  Option<located<duration>> timeout;
  Option<located<duration>> connection_timeout;
  Option<located<uint64_t>> max_retry_count;
  Option<located<duration>> retry_delay;
  Option<located<bool>> buffer_all;
  located<ir::pipeline> printer;
  location operator_location;
};

auto resolve_secrets(OpCtx& ctx, ToHttpArgs& args, std::string& resolved_url,
                     Headers& resolved_headers) -> Task<failure_or<void>> {
  auto requests = std::vector<secret_request>{};
  requests.emplace_back(
    make_secret_request("url", args.url, resolved_url, ctx.dh()));
  auto header_requests = http::make_header_secret_requests(
    args.headers, resolved_headers, ctx.dh());
  requests.insert(requests.end(),
                  std::make_move_iterator(header_requests.begin()),
                  std::make_move_iterator(header_requests.end()));
  CO_TRY(co_await ctx.resolve_secrets(std::move(requests)));
  if (resolved_url.empty()) {
    diagnostic::error("`url` must not be empty").primary(args.url).emit(ctx);
    co_return failure::promise();
  }
  co_return {};
}

// -- streaming HTTP source ---------------------------------------------------

/// Custom HTTPSource that streams body chunks from a Channel.
///
/// Proxygen pulls body data from this source via readBodyEvent(). The channel
/// is fed by process_sub() on the executor thread; Proxygen reads from it on
/// the event base thread.
class StreamingHTTPSource final : public proxygen::coro::HTTPSource {
public:
  StreamingHTTPSource(std::unique_ptr<proxygen::HTTPMessage> msg,
                      tenzir::Receiver<chunk_ptr> body_rx)
    : msg_{std::move(msg)}, body_rx_{std::move(body_rx)} {
  }

  ~StreamingHTTPSource() override = default;

  auto readHeaderEvent()
    -> folly::coro::Task<proxygen::coro::HTTPHeaderEvent> override {
    // Do not set Content-Length — the body size is unknown upfront, so
    // proxygen will use chunked transfer encoding.
    proxygen::coro::HTTPHeaderEvent event(std::move(msg_), /*eom=*/false);
    auto guard = folly::makeGuard(lifetime(event));
    co_return event;
  }

  auto readBodyEvent(uint32_t)
    -> folly::coro::Task<proxygen::coro::HTTPBodyEvent> override {
    TENZIR_ASSERT(body_rx_);
    auto item = co_await body_rx_->recv();
    body_read_started_ = true;
    if (not item or not *item) {
      // Channel closed or null sentinel: end of body.
      proxygen::coro::HTTPBodyEvent event(nullptr, /*eom=*/true);
      auto guard = folly::makeGuard(lifetime(event));
      co_return event;
    }
    body_started_ = true;
    auto buf = folly::IOBuf::copyBuffer((*item)->data(), (*item)->size());
    proxygen::coro::HTTPBodyEvent event(std::move(buf), /*eom=*/false);
    auto guard = folly::makeGuard(lifetime(event));
    co_return event;
  }

  void stopReading(
    folly::Optional<const proxygen::coro::HTTPErrorCode>) noexcept override {
    if (heapAllocated_) {
      delete this;
    }
  }

  void setReadTimeout(std::chrono::milliseconds) noexcept override {
  }

  auto body_started() const -> bool {
    return body_started_;
  }

  auto take_receiver_for_retry() -> Option<Receiver<chunk_ptr>> {
    if (body_read_started_) {
      return None{};
    }
    return std::exchange(body_rx_, None{});
  }

private:
  std::unique_ptr<proxygen::HTTPMessage> msg_;
  Option<Receiver<chunk_ptr>> body_rx_;
  bool body_read_started_ = false;
  bool body_started_ = false;
};

struct retryable_http_response : std::runtime_error {
  retryable_http_response(uint16_t status_code,
                          Option<std::chrono::seconds> retry_after)
    : std::runtime_error{fmt::format("retryable HTTP status {}", status_code)},
      status_code{status_code},
      retry_after{retry_after} {
  }

  uint16_t status_code = 0;
  Option<std::chrono::seconds> retry_after;
};

struct retry_info {
  std::string reason;
  Option<std::chrono::seconds> retry_after;
};

auto get_retry_info(folly::exception_wrapper const& error)
  -> Option<retry_info> {
  if (error.is_compatible_with<folly::AsyncSocketException>()) {
    return retry_info{.reason = "connection error", .retry_after = None{}};
  }
  auto result = Option<retry_info>{};
  error.with_exception([&](proxygen::coro::HTTPError const& http_error) {
    if (http::is_retryable_http_error(http_error.code)) {
      result = retry_info{
        .reason = "connection error",
        .retry_after = None{},
      };
    }
  });
  error.with_exception([&](retryable_http_response const& response) {
    result = retry_info{
      .reason = fmt::format("HTTP error {}", response.status_code),
      .retry_after = response.retry_after,
    };
  });
  return result;
}

// -- operator ----------------------------------------------------------------

constexpr auto body_channel_capacity = size_t{8};

class ToHttp final : public Operator<table_slice, void> {
public:
  explicit ToHttp(ToHttpArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    bytes_write_counter_ = ctx.make_counter(
      MetricsLabel{
        "operator",
        "to_http",
      },
      MetricsDirection::write, MetricsVisibility::external_,
      MetricsUnit::bytes);
    events_write_counter_ = ctx.make_counter(
      MetricsLabel{
        "operator",
        "to_http",
      },
      MetricsDirection::write, MetricsVisibility::external_,
      MetricsUnit::events);
    // setup url, headers & tls
    if (auto result = co_await resolve_secrets(ctx, args_, url_, headers_);
        result.is_error()) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto tls_result = http::normalize_url_and_tls(
      args_.tls, url_, args_.url.source, ctx.dh(), ctx.actor_system().config());
    if (tls_result.is_error()) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto tls_enabled = *tls_result;
    if (tls_enabled) {
      http::ensure_default_ca_paths();
    }
    auto ssl_context = std::shared_ptr<folly::SSLContext>{};
    if (tls_enabled) {
      auto tls_opts
        = tls_options::from_optional(args_.tls, {.is_server = false});
      auto tls = tls_opts.resolve(ctx.actor_system().config(), ctx.dh());
      if (tls.is_error()) {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
      auto ssl_result = tls->make_folly_ssl_context(ctx.dh(), true);
      if (ssl_result.is_error()) {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
      ssl_context = std::move(*ssl_result);
    }
    parsed_url_ = proxygen::URL{url_};
    if (not parsed_url_.isValid() or not parsed_url_.hasHost()) {
      diagnostic::error("invalid URL: `{}`", url_).primary(args_.url).emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    evb_ = ctx.io_executor()->getEventBase();
    auto secure = tls_enabled
                    ? proxygen::coro::HTTPClient::SecureTransportImpl::TLS
                    : proxygen::coro::HTTPClient::SecureTransportImpl::NONE;
    conn_params_ = proxygen::coro::HTTPClient::getConnParams(
      secure, parsed_url_.getHost());
    if (ssl_context) {
      conn_params_.sslContext = std::move(ssl_context);
    }
    // Create the body channel. The request task starts lazily on the first
    // emitted body chunk so empty invocations do not send empty HTTP requests.
    // In buffer_all mode, chunks are accumulated in memory instead.
    if (not is_buffer_all()) {
      auto [tx, rx] = channel<chunk_ptr>(body_channel_capacity);
      body_tx_.emplace(std::move(tx));
      body_rx_.emplace(std::move(rx));
    }
    // Spawn writer sub-pipeline.
    auto pipeline = args_.printer.inner;
    co_await ctx.spawn_sub<table_slice>(sub_key(), std::move(pipeline));
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    auto sub = ctx.get_sub(sub_key());
    if (not sub) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto const rows = input.rows();
    auto& pipeline = as<SubHandle<table_slice>>(*sub);
    if (auto result = co_await pipeline.push(std::move(input));
        result.is_err()) {
      co_await begin_draining(ctx);
      co_return;
    }
    events_write_counter_.add(rows);
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx& ctx)
    -> Task<void> override {
    if (not chunk or chunk->size() == 0 or lifecycle_ == Lifecycle::done) {
      co_return;
    }
    if (is_buffer_all()) {
      bytes_write_counter_.add(chunk->size());
      buffered_body_.append(
        folly::IOBuf::copyBuffer(chunk->data(), chunk->size()));
      co_return;
    }
    if (not body_tx_) {
      co_return;
    }
    if (not request_started_) {
      start_request_task(ctx);
    }
    bytes_write_counter_.add(chunk->size());
    co_await body_tx_->send(std::move(chunk));
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await response_->recv();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    auto response = std::move(result).as<http::Response>();
    if (response.status_code != 0 and not response.is_status_success()) {
      diagnostic::error("HTTP request returned status {}", response.status_code)
        .primary(args_.operator_location)
        .emit(ctx);
    }
    // Close the sub-pipeline to stop process_sub() from blocking on
    // the body queue. Then mark the operator as done.
    auto sub = ctx.get_sub(sub_key());
    if (sub) {
      auto& pipeline = as<SubHandle<table_slice>>(*sub);
      co_await pipeline.close();
    }
    lifecycle_ = Lifecycle::done;
  }

  auto finish_sub(SubKeyView, OpCtx& ctx) -> Task<void> override {
    if (is_buffer_all()) {
      if (buffered_body_.empty()) {
        std::ignore = response_->send(http::Response{});
      } else {
        start_buffered_request_task(ctx);
      }
      co_return;
    }
    TENZIR_UNUSED(ctx);
    if (request_started_) {
      // Send a null chunk as end-of-body sentinel.
      if (body_tx_) {
        co_await body_tx_->send(chunk_ptr{});
      }
    } else {
      std::ignore = response_->send(http::Response{});
    }
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return FinalizeBehavior::done;
    }
    if (lifecycle_ == Lifecycle::running) {
      co_await begin_draining(ctx);
    }
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto state() -> OperatorState override {
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::normal;
  }

private:
  enum class Lifecycle {
    running,
    draining,
    done,
  };

  auto is_buffer_all() const -> bool {
    return args_.buffer_all and args_.buffer_all->inner;
  }

  auto start_request_task(OpCtx& ctx) -> void {
    TENZIR_ASSERT(body_rx_);
    request_started_ = true;
    ctx.spawn_task([this, dh = &ctx.dh(),
                    body_rx = std::move(*body_rx_)]() mutable -> Task<void> {
      co_await run_request(*dh, std::move(body_rx));
    });
    body_rx_.reset();
  }

  auto start_buffered_request_task(OpCtx& ctx) -> void {
    request_started_ = true;
    auto body = buffered_body_.move();
    ctx.spawn_task(
      [this, dh = &ctx.dh(), body = std::move(body)]() mutable -> Task<void> {
        co_await run_buffered_request(*dh, std::move(body));
      });
  }

  auto begin_draining(OpCtx& ctx) -> Task<void> {
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    lifecycle_ = Lifecycle::draining;
    auto sub = ctx.get_sub(sub_key());
    if (not sub) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto& pipeline = as<SubHandle<table_slice>>(*sub);
    co_await pipeline.close();
  }

  auto run_request(diagnostic_handler& dh, Receiver<chunk_ptr> body_rx_val)
    -> Task<void> {
    auto max_retries = get_max_retry_count();
    auto attempt = uint32_t{0};
    auto response = http::Response{};
    auto body_rx = Option<Receiver<chunk_ptr>>{std::move(body_rx_val)};
    while (true) {
      auto attempt_result = co_await try_single_request(body_rx);
      if (attempt_result.is_ok()) {
        response = std::move(attempt_result).unwrap();
        break;
      }
      auto error = std::move(attempt_result).unwrap_err();
      auto error_message = scrub_errno(error.what().toStdString());
      auto retry = get_retry_info(error);
      // Once the receiver has been moved into the streaming source,
      // the body cannot be replayed and retries are impossible.
      if (not body_rx or not retry or attempt >= max_retries) {
        diagnostic::error("HTTP request to `{}` failed: {}", url_,
                          error_message)
          .primary(args_.operator_location)
          .emit(dh);
        break;
      }
      // Compute delay and emit a warning.
      auto delay = http::retry_delay_for_attempt(get_retry_delay(), attempt,
                                                 retry->retry_after);
      ++attempt;
      auto delay_secs = std::chrono::duration_cast<std::chrono::seconds>(delay);
      diagnostic::warning("{} ({}), attempt {}/{}, retrying after {}s",
                          retry->reason, error_message, attempt,
                          max_retries + 1u, delay_secs.count())
        .primary(args_.operator_location)
        .emit(dh);
      co_await folly::coro::sleep(delay);
    }
    std::ignore = response_->send(std::move(response));
  }

  auto make_request_message(HttpRequestTargetForm target_form,
                            Option<proxy_url> const& proxy) const
    -> std::unique_ptr<proxygen::HTTPMessage> {
    auto msg = std::make_unique<proxygen::HTTPMessage>();
    msg->setURL(make_proxy_request_target(parsed_url_, target_form));
    msg->setMethod(get_method());
    msg->setSecure(parsed_url_.isSecure());
    for (auto const& [name, value] : headers_) {
      msg->getHeaders().add(name, value);
    }
    add_forward_proxy_authorization(msg->getHeaders(), target_form, proxy);
    if (not msg->getHeaders().exists(proxygen::HTTP_HEADER_HOST)) {
      msg->getHeaders().add(proxygen::HTTP_HEADER_HOST,
                            parsed_url_.getHostAndPortOmitDefault());
    }
    return msg;
  }

  /// Attempts a single HTTP request. On success, moves `body_rx` into the
  /// streaming source (leaving it empty). On connection failure before
  /// the source is created, `body_rx` remains valid for a retry.
  auto try_single_request(Option<Receiver<chunk_ptr>>& body_rx)
    -> Task<Result<http::Response, folly::exception_wrapper>> {
    auto attempt_res = co_await async_try(folly::coro::co_withExecutor(
      folly::Executor::KeepAlive<>{evb_},
      folly::coro::co_invoke([this, &body_rx]() -> Task<http::Response> {
        // Create a one-shot request handle, routing through the configured
        // HTTP proxy when one applies.
        auto proxy_request = co_await make_http_proxy_request(
          *evb_, parsed_url_.getHost(), parsed_url_.getPort(), conn_params_,
          proxygen::coro::HTTPCoroConnector::defaultSessionParams(),
          get_connection_timeout(), parsed_url_.isSecure());
        auto msg = make_request_message(proxy_request.target_form(),
                                        proxy_request.proxy());
        // Create the streaming source. If the request fails before the
        // source starts reading body events, the receiver can still be reused
        // for a retry.
        auto source = StreamingHTTPSource{std::move(msg), std::move(*body_rx)};
        body_rx.reset();
        auto resp = proxygen::coro::HTTPClient::Response{};
        auto request_result = co_await async_try(proxy_request.send(
          &source, proxygen::coro::HTTPClient::makeDefaultReader(resp),
          get_timeout()));
        if (request_result.is_err()) {
          if (auto retry_rx = source.take_receiver_for_retry()) {
            body_rx = std::move(retry_rx);
          }
          co_yield folly::coro::co_error(
            std::move(request_result).unwrap_err());
        }
        auto http_response = http::to_http_response(resp);
        if (not source.body_started()) {
          if (http::is_retryable_http_status(http_response.status_code)) {
            if (auto retry_rx = source.take_receiver_for_retry()) {
              body_rx = std::move(retry_rx);
            }
            throw retryable_http_response{
              http_response.status_code,
              http::parse_retry_after(
                resp.headers->getHeaders().getSingleOrEmpty("Retry-After")),
            };
          }
          // The server responded before reading the request body.
          // Treat this as an error to prevent silent data loss.
          co_yield folly::coro::co_error(
            folly::make_exception_wrapper<std::runtime_error>(fmt::format(
              "server responded with HTTP {} before reading the request "
              "body",
              http_response.status_code)));
        }
        co_return http_response;
      })));
    if (attempt_res.is_err()) {
      co_return Err{std::move(attempt_res).unwrap_err()};
    }
    co_return std::move(attempt_res).unwrap();
  }

  auto run_buffered_request(diagnostic_handler& dh,
                            std::unique_ptr<folly::IOBuf> body) -> Task<void> {
    auto max_retries = get_max_retry_count();
    auto attempt = uint32_t{0};
    auto response = http::Response{};
    while (true) {
      auto attempt_result = co_await try_single_buffered_request(body->clone());
      if (attempt_result.is_ok()) {
        response = std::move(attempt_result).unwrap();
        break;
      }
      auto error = std::move(attempt_result).unwrap_err();
      auto error_message = scrub_errno(error.what().toStdString());
      auto retry = get_retry_info(error);
      if (not retry or attempt >= max_retries) {
        diagnostic::error("HTTP request to `{}` failed: {}", url_,
                          error_message)
          .primary(args_.operator_location)
          .emit(dh);
        break;
      }
      auto delay = http::retry_delay_for_attempt(get_retry_delay(), attempt,
                                                 retry->retry_after);
      ++attempt;
      auto delay_secs = std::chrono::duration_cast<std::chrono::seconds>(delay);
      diagnostic::warning("{} ({}), attempt {}/{}, retrying after {}s",
                          retry->reason, error_message, attempt,
                          max_retries + 1u, delay_secs.count())
        .primary(args_.operator_location)
        .emit(dh);
      co_await folly::coro::sleep(delay);
    }
    std::ignore = response_->send(std::move(response));
  }

  auto try_single_buffered_request(std::unique_ptr<folly::IOBuf> body)
    -> Task<Result<http::Response, folly::exception_wrapper>> {
    auto attempt_res = co_await async_try(folly::coro::co_withExecutor(
      folly::Executor::KeepAlive<>{evb_},
      folly::coro::co_invoke(
        [this, body = std::move(body)]() mutable -> Task<http::Response> {
          // Routes through the configured HTTP proxy when applicable.
          auto proxy_request = co_await make_http_proxy_request(
            *evb_, parsed_url_.getHost(), parsed_url_.getPort(), conn_params_,
            proxygen::coro::HTTPCoroConnector::defaultSessionParams(),
            get_connection_timeout(), parsed_url_.isSecure());
          // HTTPFixedSource sets Content-Length automatically from the body.
          auto msg = make_request_message(proxy_request.target_form(),
                                          proxy_request.proxy());
          auto source
            = proxygen::coro::HTTPFixedSource{std::move(msg), std::move(body)};
          auto resp = proxygen::coro::HTTPClient::Response{};
          auto request_result = co_await async_try(proxy_request.send(
            &source, proxygen::coro::HTTPClient::makeDefaultReader(resp),
            get_timeout()));
          if (request_result.is_err()) {
            co_yield folly::coro::co_error(
              std::move(request_result).unwrap_err());
          }
          auto http_response = http::to_http_response(resp);
          if (http::is_retryable_http_status(http_response.status_code)) {
            throw retryable_http_response{
              http_response.status_code,
              http::parse_retry_after(
                resp.headers->getHeaders().getSingleOrEmpty("Retry-After")),
            };
          }
          co_return http_response;
        })));
    if (attempt_res.is_err()) {
      co_return Err{std::move(attempt_res).unwrap_err()};
    }
    co_return std::move(attempt_res).unwrap();
  }

  auto get_method() const -> proxygen::HTTPMethod {
    if (not args_.method) {
      return proxygen::HTTPMethod::POST;
    }
    auto method_s = args_.method->inner;
    std::ranges::transform(method_s, method_s.begin(), [](unsigned char c) {
      return static_cast<char>(std::toupper(c));
    });
    return *proxygen::stringToMethod(method_s);
  }

  auto get_timeout() const -> std::chrono::milliseconds {
    if (args_.timeout) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
        args_.timeout->inner);
    }
    return http::default_timeout;
  }

  auto get_connection_timeout() const -> std::chrono::milliseconds {
    if (args_.connection_timeout) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
        args_.connection_timeout->inner);
    }
    return http::default_connection_timeout;
  }

  auto get_max_retry_count() const -> uint32_t {
    if (args_.max_retry_count) {
      return detail::narrow<uint32_t>(args_.max_retry_count->inner);
    }
    return http::default_max_retry_count;
  }

  auto get_retry_delay() const -> std::chrono::milliseconds {
    if (args_.retry_delay) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
        args_.retry_delay->inner);
    }
    return http::default_retry_delay;
  }

  static auto sub_key() -> int64_t {
    return int64_t{0};
  }

  // --- args ---
  ToHttpArgs args_;
  std::string url_;
  Headers headers_;
  // --- transient ---
  proxygen::URL parsed_url_;
  folly::EventBase* evb_ = nullptr;
  proxygen::coro::HTTPCoroConnector::ConnectionParams conn_params_;
  Option<Sender<chunk_ptr>> body_tx_;
  Option<Receiver<chunk_ptr>> body_rx_;
  folly::IOBufQueue buffered_body_{folly::IOBufQueue::cacheChainLength()};
  mutable Arc<Oneshot<http::Response>> response_{std::in_place};
  bool request_started_ = false;
  Lifecycle lifecycle_ = Lifecycle::running;
  MetricsCounter bytes_write_counter_;
  MetricsCounter events_write_counter_;
};

class ToHttpPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "to_http";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToHttpArgs, ToHttp>{};
    d.positional("url", &ToHttpArgs::url);
    auto method_arg = d.named("method", &ToHttpArgs::method);
    auto headers_arg = d.named("headers", &ToHttpArgs::headers, "record");
    auto tls_validator
      = tls_options{{.is_server = false}}.add_to_describer(d, &ToHttpArgs::tls);
    auto timeout_arg = d.named("timeout", &ToHttpArgs::timeout);
    auto connection_timeout_arg
      = d.named("connection_timeout", &ToHttpArgs::connection_timeout);
    auto max_retry_count_arg
      = d.named("max_retry_count", &ToHttpArgs::max_retry_count);
    auto retry_delay_arg = d.named("retry_delay", &ToHttpArgs::retry_delay);
    d.named("buffer_all", &ToHttpArgs::buffer_all);
    auto printer_arg
      = d.pipeline(&ToHttpArgs::printer, SubOptimize::from_downstream);
    d.operator_location(&ToHttpArgs::operator_location);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      tls_validator(ctx);
      if (auto method = ctx.get(method_arg)) {
        auto method_s = method->inner;
        std::ranges::transform(method_s, method_s.begin(), [](unsigned char c) {
          return static_cast<char>(std::toupper(c));
        });
        if (not proxygen::stringToMethod(method_s)) {
          diagnostic::error("invalid http method: `{}`", method->inner)
            .primary(method->source)
            .emit(ctx);
        }
      }
      if (auto timeout = ctx.get(timeout_arg)) {
        if (timeout->inner < duration::zero()) {
          diagnostic::error("`timeout` must be a non-negative duration")
            .primary(timeout->source)
            .emit(ctx);
        }
      }
      if (auto connection_timeout = ctx.get(connection_timeout_arg)) {
        if (connection_timeout->inner < duration::zero()) {
          diagnostic::error(
            "`connection_timeout` must be a non-negative duration")
            .primary(connection_timeout->source)
            .emit(ctx);
        }
      }
      if (auto max_retry_count = ctx.get(max_retry_count_arg)) {
        if (max_retry_count->inner > std::numeric_limits<uint32_t>::max()) {
          diagnostic::error("`max_retry_count` must be <= {}",
                            std::numeric_limits<uint32_t>::max())
            .primary(max_retry_count->source)
            .emit(ctx);
        }
      }
      if (auto retry_delay = ctx.get(retry_delay_arg)) {
        if (retry_delay->inner < duration::zero()) {
          diagnostic::error("`retry_delay` must be a non-negative duration")
            .primary(retry_delay->source)
            .emit(ctx);
        }
      }
      if (auto headers = ctx.get(headers_arg)) {
        auto const* rec = try_as<record>(&headers->inner);
        if (not rec) {
          diagnostic::error("`headers` must be a record")
            .primary(headers->source)
            .emit(ctx);
        } else {
          for (auto const& [_, value] : *rec) {
            if (not is<std::string>(value) and not is<secret>(value)) {
              diagnostic::error("header values must be `string` or `secret`")
                .primary(headers->source)
                .emit(ctx);
              break;
            }
          }
        }
      }
      TRY(auto printer, ctx.get(printer_arg));
      auto output = printer.inner.infer_type(tag_v<table_slice>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (not *output or (*output)->is_not<chunk_ptr>()) {
        diagnostic::error("pipeline must return bytes")
          .primary(printer.source.subloc(0, 1))
          .emit(ctx);
      }
      return {};
    });
    return d.invariant_order_filter();
  }
};

} // namespace

} // namespace tenzir::plugins::to_http

TENZIR_REGISTER_PLUGIN(tenzir::plugins::to_http::ToHttpPlugin)
