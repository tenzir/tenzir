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
#include <tenzir/async/task.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try.hpp>

#include <proxygen/lib/http/HTTPMethod.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <exception>
#include <iterator>
#include <limits>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::plugins::to_http {

namespace {

constexpr auto default_parallel = 1;

using Headers = std::vector<std::pair<std::string, std::string>>;

struct ToHttpArgs {
  located<secret> url;
  Option<located<std::string>> method;
  Option<located<data>> headers;
  Option<located<data>> tls;
  Option<located<duration>> timeout;
  Option<located<duration>> connection_timeout;
  Option<located<uint64_t>> max_retry_count;
  Option<located<duration>> retry_delay;
  Option<located<uint64_t>> parallel;
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

class ToHttp final : public Operator<table_slice, void> {
public:
  explicit ToHttp(ToHttpArgs args)
    : args_{std::move(args)}, request_slots_{get_parallel()} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    bytes_write_counter_ = ctx.make_counter(
      MetricsLabel{
        "operator",
        "to_http",
      },
      MetricsDirection::write, MetricsVisibility::external_);
    // setup url, headers & tls
    if (auto result = co_await resolve_secrets(ctx, args_, url_, headers_);
        result.is_error()) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto config = http::make_http_pool_config(
      args_.tls, url_, args_.url.source, ctx, get_timeout(),
      std::addressof(ctx.actor_system().config()));
    if (config.is_success()) {
      config->connection_timeout = get_connection_timeout();
      config->max_retry_count = get_max_retry_count();
      config->retry_delay = get_retry_delay();
    }
    if (config.is_error()) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    try {
      http_pool_ = HttpPool::make(ctx.io_executor(), url_, std::move(*config));
    } catch (std::exception const& e) {
      diagnostic::error("failed to initialize HTTP client: {}", e.what())
        .primary(args_.url)
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    // spawn writer sub pipeline
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
    auto& pipeline = as<SubHandle<table_slice>>(*sub);
    if (auto result = co_await pipeline.push(std::move(input));
        result.is_err()) {
      co_await begin_draining(ctx);
    }
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx& ctx)
    -> Task<void> override {
    if (not chunk or chunk->size() == 0 or error_signal_->has_sent()) {
      co_return;
    }
    TENZIR_ASSERT(http_pool_);
    bytes_write_counter_.add(chunk->size());
    auto permit = co_await request_slots_.acquire();
    if (error_signal_->has_sent()) {
      co_return;
    }
    auto* dh = &ctx.dh();
    ctx.spawn_task([this, permit = std::move(permit), chunk = std::move(chunk),
                    dh]() mutable -> Task<void> {
      auto method = get_method();
      auto headers = std::map<std::string, std::string>{};
      for (auto const& [name, value] : headers_) {
        headers[name] = value;
      }
      auto body = std::string{reinterpret_cast<char const*>(chunk->data()),
                              chunk->size()};
      auto response = co_await (*http_pool_)
                        ->request(method, std::move(body), std::move(headers));
      if (response.is_err()) {
        auto error = fmt::format("HTTP request to `{}` failed: {}", url_,
                                 std::move(response).unwrap_err());
        std::ignore = error_signal_->send(std::move(error));
      } else {
        auto http_response = std::move(response).unwrap();
        if (http_response.status_code < 200
            or http_response.status_code > 299) {
          diagnostic::warning("HTTP request returned status {}",
                              http_response.status_code)
            .primary(args_.operator_location)
            .emit(*dh);
        }
      }
      permit.release();
    });
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (error_signal_->has_received()) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return co_await error_signal_->recv();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    auto error = std::move(result).as<std::string>();
    diagnostic::error("{}", error).primary(args_.operator_location).emit(ctx);
    co_await begin_draining(ctx);
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    // Wait until all spawned request tasks have returned their permits before
    // letting the executor tear down the operator scope.
    for (auto i = uint64_t{0}; i < get_parallel(); ++i) {
      auto permit = co_await request_slots_.acquire();
      permit.forget();
    }
    // If a request error already fired, keep the operator draining until the
    // pending `await_task()` result is consumed by `process_task()`.
    if (not error_signal_->has_sent()) {
      lifecycle_ = Lifecycle::done;
    }
    co_return;
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

  auto get_method() const -> proxygen::HTTPMethod {
    if (args_.method) {
      return proxygen::HTTPMethod::POST;
    }
    // validated before (in Describer::validate)
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

  auto get_parallel() const -> uint64_t {
    return args_.parallel ? args_.parallel->inner : default_parallel;
  }

  static auto sub_key() -> int64_t {
    return int64_t{0};
  }

  // --- args ---
  ToHttpArgs args_;
  std::string url_;
  Headers headers_;
  // --- transient ---
  Semaphore request_slots_;
  Lifecycle lifecycle_ = Lifecycle::running;
  Option<Box<HttpPool>> http_pool_;
  mutable Arc<Oneshot<std::string>> error_signal_{std::in_place};
  MetricsCounter bytes_write_counter_;
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
    auto parallel_arg = d.named("parallel", &ToHttpArgs::parallel);
    auto printer_arg = d.pipeline(&ToHttpArgs::printer);
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
      if (auto parallel = ctx.get(parallel_arg)) {
        if (parallel->inner == 0) {
          diagnostic::error("`parallel` must be at least 1")
            .primary(parallel->source)
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
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::to_http

TENZIR_REGISTER_PLUGIN(tenzir::plugins::to_http::ToHttpPlugin)
