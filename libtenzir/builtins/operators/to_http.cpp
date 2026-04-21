//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/channel.hpp>
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
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::plugins::to_http {

namespace {

using Headers = std::vector<std::pair<std::string, std::string>>;

constexpr auto queue_capacity = uint32_t{1024};
constexpr auto default_timeout = std::chrono::milliseconds{90'000};

struct ToHttpArgs {
  located<secret> url;
  Option<located<std::string>> method;
  Option<located<data>> headers;
  Option<located<data>> tls;
  Option<located<duration>> timeout;
  located<ir::pipeline> printer;
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
  explicit ToHttp(ToHttpArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    // setup url, headers & tls
    if (auto result = co_await resolve_secrets(ctx, args_, url_, headers_);
        result.is_error()) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto tls_enabled
      = http::normalize_url_and_tls(args_.tls, url_, args_.url.source, ctx);
    if (tls_enabled.is_error()) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    // init HTTP pool
    auto config = HttpPoolConfig{
      .tls = *tls_enabled,
      .ssl_context = nullptr,
      .request_timeout = get_timeout(),
    };
    if (*tls_enabled) {
      auto tls_opts = args_.tls ? tls_options{*args_.tls, {.is_server = false}}
                                : tls_options{{.is_server = false}};
      auto ssl_context = tls_opts.make_folly_ssl_context(ctx);
      if (ssl_context.is_error()) {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
      config.ssl_context = std::move(*ssl_context);
    }
    try {
      http_pool_ = HttpPool::make(ctx.io_executor(), url_, config);
    } catch (std::exception const& e) {
      diagnostic::error("failed to initialize HTTP client: {}", e.what())
        .primary(args_.url)
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    // spawn writer sub pipeline
    auto pipeline = args_.printer.inner;
    co_await ctx.spawn_sub<table_slice>(sub_key_, std::move(pipeline));
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    auto sub = ctx.get_sub(make_view(sub_key_));
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

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (not chunk or chunk->size() == 0) {
      co_return;
    }
    TENZIR_ASSERT(queue_sender_);
    co_await queue_sender_->send(std::move(chunk));
  }

  auto finish_sub(SubKeyView, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    queue_sender_ = None{}; // close the queue
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await queue_receiver_.recv();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    auto chunk = std::move(result).as<Option<chunk_ptr>>();
    if (chunk.is_none()) {
      // subpipeline finished and the queue drained completely.
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto body = std::string{reinterpret_cast<char const*>((*chunk)->data()),
                            (*chunk)->size()};
    TENZIR_ASSERT(http_pool_);
    auto headers = std::map<std::string, std::string>{};
    for (auto const& [name, value] : headers_) {
      headers[name] = value;
    }
    auto response
      = co_await (*http_pool_)
          ->request(get_method(), std::move(body), std::move(headers));
    if (response.is_err()) {
      diagnostic::error("HTTP request to `{}` failed: {}", url_,
                        std::move(response).unwrap_err())
        .primary(args_.url.source)
        .emit(ctx);
      co_await begin_draining(ctx);
      co_return;
    }
    auto http_response = std::move(response).unwrap();
    if (http_response.status_code < 200 or http_response.status_code > 299) {
      diagnostic::warning("HTTP request returned status {}",
                          http_response.status_code)
        .primary(args_.url.source)
        .emit(ctx);
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
                                         : OperatorState::unspecified;
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
    auto sub = ctx.get_sub(make_view(sub_key_));
    if (not sub) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto& pipeline = as<SubHandle<table_slice>>(*sub);
    co_await pipeline.close();
  }

  auto get_method() -> std::string {
    return args_.method ? args_.method->inner : "POST";
  }

  auto get_timeout() -> std::chrono::milliseconds {
    if (args_.timeout) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
        args_.timeout->inner);
    }
    return default_timeout;
  }

  // --- args ---
  ToHttpArgs args_;
  std::string url_;
  Headers headers_;
  // --- transient ---
  Option<Sender<chunk_ptr>> queue_sender_;
  mutable Receiver<chunk_ptr> queue_receiver_;
  data sub_key_ = int64_t{0};
  Lifecycle lifecycle_ = Lifecycle::running;
  Option<Box<HttpPool>> http_pool_;
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
    auto printer_arg = d.pipeline(&ToHttpArgs::printer);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      tls_validator(ctx);
      if (auto method = ctx.get(method_arg)) {
        auto normalized = method->inner;
        std::ranges::transform(normalized, normalized.begin(),
                               [](unsigned char c) {
                                 return static_cast<char>(std::toupper(c));
                               });
        if (not proxygen::stringToMethod(normalized)) {
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
