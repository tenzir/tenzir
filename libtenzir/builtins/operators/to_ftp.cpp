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

struct UploadAbort {
  bool terminal = false;
};

using UploadMessage = variant<UploadChunk, UploadInputDone, UploadAbort>;
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
    co_await ctx.spawn_sub<table_slice>(caf::none, std::move(pipeline));
    upload_task_.emplace(ctx.spawn_task(upload_loop(ctx)));
    co_return;
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    auto sub = ctx.get_sub(caf::none);
    if (not sub) {
      lifecycle_ = Lifecycle::draining;
      co_await request_upload_abort(true);
      co_await finish_upload_task();
      co_return;
    }
    auto& printer = as<SubHandle<table_slice>>(*sub);
    auto push_result = co_await printer.push(std::move(input));
    if (push_result.is_err()) {
      // A closed printer input can be an intentional early shutdown, e.g., from
      // `head`. Let `finish_sub()` close the upload with EOF instead of
      // turning already-produced printer output into a local abort.
      lifecycle_ = Lifecycle::draining;
    }
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (not chunk or chunk->size() == 0) {
      co_return;
    }
    co_await upload_messages_->enqueue(UploadChunk{std::move(chunk)});
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return FinalizeBehavior::done;
    }
    if (lifecycle_ == Lifecycle::running) {
      lifecycle_ = Lifecycle::draining;
      if (auto sub = ctx.get_sub(caf::none)) {
        auto& printer = as<SubHandle<table_slice>>(*sub);
        co_await printer.close();
        co_return FinalizeBehavior::continue_;
      }
      co_await request_upload_done();
      co_await finish_upload_task();
      co_return FinalizeBehavior::done;
    }
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    co_await upload_messages_->enqueue(UploadInputDone{});
    co_await finish_upload_task();
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    lifecycle_ = Lifecycle::done;
    co_await request_upload_abort(true);
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
  auto request_upload_done() -> Task<void> {
    if (not upload_task_) {
      co_return;
    }
    co_await upload_messages_->enqueue(UploadInputDone{});
  }

  auto request_upload_abort(bool terminal) -> Task<void> {
    if (not upload_task_) {
      co_return;
    }
    co_await upload_messages_->enqueue(UploadAbort{.terminal = terminal});
  }

  auto finish_upload_task() -> Task<void> {
    if (not upload_task_) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto upload_task = std::move(*upload_task_);
    upload_task_.reset();
    co_await upload_task.join();
    lifecycle_ = Lifecycle::done;
  }

  auto upload_loop(OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(session_);
    auto upload = session_->start_upload(upload_queue_capacity);
    auto uploaded_anything = false;
    auto local_abort = false;
    auto accepting_chunks = true;
    auto finish = [&]() -> Task<void> {
      auto upload_result = co_await upload.result();
      emit_upload_result(ctx, upload_result, uploaded_anything, local_abort);
    };
    while (true) {
      auto message = co_await upload_messages_->dequeue();
      auto done = co_await co_match(
        std::move(message),
        [&](UploadChunk message) -> Task<bool> {
          if (not accepting_chunks) {
            co_return false;
          }
          if (co_await upload.push(std::move(message.chunk))) {
            uploaded_anything = true;
            co_return false;
          }
          accepting_chunks = false;
          co_return false;
        },
        [&](UploadInputDone) -> Task<bool> {
          if (not local_abort) {
            upload.close();
          }
          co_await finish();
          co_return true;
        },
        [&](UploadAbort abort) -> Task<bool> {
          local_abort = true;
          accepting_chunks = false;
          upload.abort();
          if (not abort.terminal) {
            co_return false;
          }
          co_await finish();
          co_return true;
        });
      if (done) {
        co_return;
      }
    }
  }

  auto emit_upload_result(OpCtx& ctx, CurlUploadResult& upload_result,
                          bool uploaded_anything, bool local_abort) -> void {
    if (upload_result.is_ok()) {
      auto const& done = upload_result.unwrap();
      if (done.status == CurlTransferStatus::finished and done.response_code
          and uploaded_anything) {
        auto response_code = *done.response_code;
        if (response_code < 200 or response_code > 299) {
          diagnostic::error("FTP upload failed")
            .primary(args_.url.source)
            .note("FTP response code: {}", response_code)
            .emit(ctx);
        }
      }
    }
    if (upload_result.is_err() and not local_abort) {
      diagnostic::error("FTP upload failed")
        .primary(args_.url.source)
        .note("curl error: {}", to_string(upload_result.unwrap_err()))
        .emit(ctx);
    }
  }

  enum class Lifecycle {
    running,
    draining,
    done,
  };

  Option<CurlSession> session_;
  Option<AsyncHandle<void>> upload_task_;
  mutable Box<UploadMessageQueue> upload_messages_{
    std::in_place,
    upload_message_queue_capacity,
  };
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
