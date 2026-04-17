//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"

#include <tenzir/arc.hpp>
#include <tenzir/async/curl.hpp>
#include <tenzir/box.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/http.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/transfer.hpp>

#include <fmt/format.h>
#include <folly/coro/BoundedQueue.h>

namespace tenzir::plugins::to_ftp {

namespace {

constexpr auto upload_queue_capacity = size_t{16};
constexpr auto result_queue_capacity = uint32_t{2};

struct ToFtpArgs {
  located<secret> url;
  Option<located<data>> tls;
  located<ir::pipeline> printer;
};

struct UploadFinished {};

struct UploadFailed {
  std::string error;
};

using ResultMessage = variant<UploadFinished, UploadFailed>;
using ResultQueue = folly::coro::BoundedQueue<ResultMessage>;

auto add_default_ftp_scheme(std::string& url) -> void {
  if (not url.starts_with("ftp://") and not url.starts_with("ftps://")) {
    url.insert(0, "ftp://");
  }
}

auto make_tls_options(std::string_view url, Option<located<data>> const& tls)
  -> tls_options {
  if (tls) {
    return tls_options{*tls, {.is_server = false}};
  }
  auto tls_default = url.starts_with("ftps://");
  return tls_options{located{data{tls_default}, location::unknown},
                     {.is_server = false}};
}

auto resolve_url(OpCtx& ctx, ToFtpArgs const& args, std::string& resolved_url)
  -> Task<bool> {
  resolved_url.clear();
  auto requests = std::vector<secret_request>{};
  requests.emplace_back(
    make_secret_request("url", args.url, resolved_url, ctx.dh()));
  if (auto result = co_await ctx.resolve_secrets(std::move(requests));
      result.is_error()) {
    co_return false;
  }
  if (resolved_url.empty()) {
    diagnostic::error("`url` must not be empty").primary(args.url).emit(ctx);
    co_return false;
  }
  add_default_ftp_scheme(resolved_url);
  co_return true;
}

auto upload(folly::Executor::KeepAlive<folly::IOExecutor> io_executor,
            std::string url, transfer_options options,
            CurlUploadBody* upload_body, Arc<ResultQueue> results)
  -> Task<void> {
  auto request = http::request{};
  request.uri = std::move(url);
  request.method = "PUT";
  auto tx = transfer{std::move(options)};
  if (auto err = tx.prepare(std::move(request)); err.valid()) {
    co_await results->enqueue(UploadFailed{fmt::format("{}", err)});
    co_return;
  }
  auto code = tx.handle().set([](std::span<const std::byte>) {});
  TENZIR_ASSERT(code == curl::easy::code::ok);
  if (auto err = co_await perform_curl_upload(std::move(io_executor),
                                              tx.handle(), *upload_body);
      err.valid()) {
    if (upload_body->is_aborted()) {
      // A local printer failure aborted an in-flight upload; report the local
      // error instead of a derived curl callback failure.
      co_await results->enqueue(UploadFinished{});
      co_return;
    }
    co_await results->enqueue(UploadFailed{fmt::format("{}", err)});
    co_return;
  }
  auto [response_code_status, response_code]
    = tx.handle().get<curl::easy::info::response_code>();
  if (response_code_status == curl::easy::code::ok
      and (response_code < 200 or response_code > 299)) {
    co_await results->enqueue(
      UploadFailed{fmt::format("FTP response code: {}", response_code)});
    co_return;
  }
  co_await results->enqueue(UploadFinished{});
}

class ToFtp final : public Operator<table_slice, void> {
public:
  explicit ToFtp(ToFtpArgs args)
    : upload_body_{std::in_place, upload_queue_capacity},
      result_queue_{std::in_place, result_queue_capacity},
      args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (not co_await resolve_url(ctx, args_, resolved_url_)) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto tls = make_tls_options(resolved_url_, args_.tls);
    if (auto valid = tls.validate(resolved_url_, args_.url.source, ctx.dh());
        not valid) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto pipeline = args_.printer.inner;
    if (not pipeline.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    tls.update_from_config(std::addressof(ctx.actor_system().config()));
    auto options = transfer_options{};
    options.default_protocol = "ftp";
    options.ssl = tls;
    co_await ctx.spawn_sub<table_slice>(caf::none, std::move(pipeline));
    upload_task_ = ctx.spawn_task(upload(ctx.io_executor(), resolved_url_,
                                         std::move(options), &*upload_body_,
                                         result_queue_));
    co_return;
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    auto sub = ctx.get_sub(caf::none);
    if (not sub) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto& printer = as<SubHandle<table_slice>>(*sub);
    auto push_result = co_await printer.push(std::move(input));
    if (push_result.is_err()) {
      lifecycle_ = Lifecycle::done;
      upload_body_->abort();
    }
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done or not chunk or chunk->size() == 0) {
      co_return;
    }
    if (not co_await upload_body_->push(std::move(chunk))) {
      lifecycle_ = Lifecycle::done;
    }
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await result_queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    co_await co_match(
      std::move(result).as<ResultMessage>(),
      [&](UploadFinished) -> Task<void> {
        lifecycle_ = Lifecycle::done;
        co_return;
      },
      [&](UploadFailed failure) -> Task<void> {
        diagnostic::error("FTP upload to `{}` failed: {}", resolved_url_,
                          failure.error)
          .primary(args_.url.source)
          .emit(ctx);
        upload_body_->abort();
        if (auto sub = ctx.get_sub(caf::none)) {
          auto& printer = as<SubHandle<table_slice>>(*sub);
          co_await printer.close();
        }
        lifecycle_ = Lifecycle::done;
      });
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (lifecycle_ == Lifecycle::done) {
      upload_body_->abort();
      co_return FinalizeBehavior::done;
    }
    if (lifecycle_ == Lifecycle::running) {
      lifecycle_ = Lifecycle::draining;
      if (auto sub = ctx.get_sub(caf::none)) {
        auto& printer = as<SubHandle<table_slice>>(*sub);
        co_await printer.close();
        co_return FinalizeBehavior::continue_;
      }
      upload_body_->close();
      co_return FinalizeBehavior::continue_;
    }
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    upload_body_->close();
    if (upload_task_) {
      co_await upload_task_->try_join();
      upload_task_ = None{};
    }
    co_return;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    lifecycle_ = Lifecycle::done;
    upload_body_->abort();
    if (auto sub = ctx.get_sub(caf::none)) {
      auto& printer = as<SubHandle<table_slice>>(*sub);
      co_await printer.close();
    }
    co_return;
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

  Box<CurlUploadBody> upload_body_;
  mutable Arc<ResultQueue> result_queue_;
  Option<AsyncHandle<void>> upload_task_;
  ToFtpArgs args_;
  std::string resolved_url_;
  Lifecycle lifecycle_ = Lifecycle::running;
};

class ToFtpPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "to_ftp";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToFtpArgs, ToFtp>{};
    d.positional("url", &ToFtpArgs::url);
    auto tls_validator
      = tls_options{{.is_server = false}}.add_to_describer(d, &ToFtpArgs::tls);
    auto printer_arg = d.pipeline(&ToFtpArgs::printer);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      tls_validator(ctx);
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

} // namespace tenzir::plugins::to_ftp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::to_ftp::ToFtpPlugin)
