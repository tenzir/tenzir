//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"

#include <tenzir/async/curl.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>

#include <folly/coro/BoundedQueue.h>

namespace tenzir::plugins::to_ftp {

namespace {

constexpr auto upload_queue_capacity = size_t{16};
constexpr auto upload_message_queue_capacity = uint32_t{16};

struct ToFtpArgs {
  located<secret> url;
  Option<located<data>> tls;
  located<ir::pipeline> printer;
};

struct UploadChunk {
  chunk_ptr chunk;
};

struct UploadInputDone {};

using UploadMessage = variant<UploadChunk, UploadInputDone>;
using UploadMessageQueue = folly::coro::BoundedQueue<UploadMessage>;

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

class ToFtp final : public Operator<table_slice, void> {
public:
  explicit ToFtp(ToFtpArgs args) : args_{std::move(args)} {
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
    session_.emplace(CurlSession::make(ctx.io_executor()));
    auto& easy = session_->easy();
    if (not curl::try_set(easy, CURLOPT_DEFAULT_PROTOCOL, "ftp")) {
      diagnostic::error("failed to configure FTP upload")
        .primary(args_.url.source)
        .note("failed to set curl option `CURLOPT_DEFAULT_PROTOCOL`")
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    if (not curl::try_set(easy, CURLOPT_URL, resolved_url_)) {
      diagnostic::error("failed to configure FTP upload")
        .primary(args_.url.source)
        .note("failed to set curl option `CURLOPT_URL`")
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    if (not curl::try_set(easy, CURLOPT_UPLOAD, 1L)) {
      diagnostic::error("failed to configure FTP upload")
        .primary(args_.url.source)
        .note("failed to set curl option `CURLOPT_UPLOAD`")
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    if (auto err = tls.apply_to(easy, resolved_url_, nullptr); err.valid()) {
      diagnostic::error("failed to configure FTP upload")
        .primary(args_.url.source)
        .note("{}", err)
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto code = easy.set([](std::span<const std::byte>) {});
    if (code != curl::easy::code::ok) {
      diagnostic::error("failed to configure FTP upload")
        .primary(args_.url.source)
        .note("curl error: {}", curl::to_string(code))
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    upload_.emplace(session_->start_upload(upload_queue_capacity));
    co_await ctx.spawn_sub<table_slice>(caf::none, std::move(pipeline));
    co_return;
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    auto sub = ctx.get_sub(caf::none);
    if (not sub) {
      lifecycle_ = Lifecycle::done;
      if (upload_) {
        upload_->abort();
      }
      co_return;
    }
    auto& printer = as<SubHandle<table_slice>>(*sub);
    auto push_result = co_await printer.push(std::move(input));
    if (push_result.is_err()) {
      lifecycle_ = Lifecycle::draining;
      if (upload_) {
        upload_->abort();
      }
    }
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (not chunk or chunk->size() == 0) {
      co_return;
    }
    co_await upload_messages_->enqueue(UploadChunk{std::move(chunk)});
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await upload_messages_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    co_await co_match(
      std::move(result).as<UploadMessage>(),
      [&](UploadChunk message) -> Task<void> {
        if (lifecycle_ == Lifecycle::done or not upload_) {
          co_return;
        }
        if (not co_await upload_->push(std::move(message.chunk))) {
          co_await finish_upload(ctx);
          if (lifecycle_ == Lifecycle::running) {
            lifecycle_ = Lifecycle::draining;
            if (auto sub = ctx.get_sub(caf::none)) {
              auto& printer = as<SubHandle<table_slice>>(*sub);
              co_await printer.close();
            } else {
              lifecycle_ = Lifecycle::done;
            }
          }
          co_return;
        }
        uploaded_anything_ = true;
      },
      [&](UploadInputDone) -> Task<void> {
        co_await finish_upload(ctx);
        lifecycle_ = Lifecycle::done;
      });
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (lifecycle_ == Lifecycle::done) {
      if (upload_) {
        upload_->abort();
      }
      co_return FinalizeBehavior::done;
    }
    if (lifecycle_ == Lifecycle::running) {
      lifecycle_ = Lifecycle::draining;
      if (auto sub = ctx.get_sub(caf::none)) {
        auto& printer = as<SubHandle<table_slice>>(*sub);
        co_await printer.close();
        co_return FinalizeBehavior::continue_;
      }
      if (upload_) {
        co_await finish_upload(ctx);
      }
      lifecycle_ = Lifecycle::done;
      co_return FinalizeBehavior::done;
    }
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto finish_sub(SubKeyView, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    ctx.spawn_task([this]() -> Task<void> {
      co_await upload_messages_->enqueue(UploadInputDone{});
    });
    co_return;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    lifecycle_ = Lifecycle::done;
    if (upload_) {
      upload_->abort();
    }
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
  auto finish_upload(OpCtx& ctx) -> Task<void> {
    if (not upload_) {
      co_return;
    }
    upload_->close();
    auto upload_result = co_await upload_->result();
    if (upload_result.is_ok()
        and upload_result.unwrap() == CurlTransferStatus::finished
        and uploaded_anything_) {
      auto [response_code_status, response_code]
        = session_->easy().get<curl::easy::info::response_code>();
      if (response_code_status == curl::easy::code::ok
          and (response_code < 200 or response_code > 299)) {
        diagnostic::error("FTP upload failed")
          .primary(args_.url.source)
          .note("FTP response code: {}", response_code)
          .emit(ctx);
      }
    }
    if (upload_result.is_err()) {
      diagnostic::error("FTP upload failed")
        .primary(args_.url.source)
        .note("curl error: {}", to_string(upload_result.unwrap_err()))
        .emit(ctx);
    }
    upload_.reset();
    co_return;
  }

  enum class Lifecycle {
    running,
    draining,
    done,
  };

  Option<CurlSession> session_;
  Option<CurlUploadTransfer> upload_;
  mutable Box<UploadMessageQueue> upload_messages_{
    std::in_place,
    upload_message_queue_capacity,
  };
  ToFtpArgs args_;
  std::string resolved_url_;
  Lifecycle lifecycle_ = Lifecycle::running;
  bool uploaded_anything_ = false;
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
