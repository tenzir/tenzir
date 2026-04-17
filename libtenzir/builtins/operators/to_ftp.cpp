//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"

#include <tenzir/arc.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/http.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/transfer.hpp>

#include <curl/curl.h>
#include <fmt/format.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/executors/GlobalExecutor.h>

#include <condition_variable>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>

namespace tenzir::plugins::to_ftp {

namespace {

constexpr auto input_queue_capacity = uint32_t{16};
constexpr auto result_queue_capacity = uint32_t{2};
constexpr auto upload_bridge_capacity = size_t{16};

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
using PrinterQueue = folly::coro::BoundedQueue<chunk_ptr>;
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

class UploadBridge {
public:
  explicit UploadBridge(size_t capacity) : capacity_{capacity} {
  }

  auto push(chunk_ptr chunk) -> bool {
    TENZIR_ASSERT(chunk);
    auto lock = std::unique_lock{mutex_};
    space_cv_.wait(lock, [&] {
      return aborted_ or buffered_.size() < capacity_;
    });
    if (aborted_) {
      return false;
    }
    buffered_.push_back(std::move(chunk));
    data_cv_.notify_one();
    return true;
  }

  auto close() -> void {
    auto lock = std::lock_guard{mutex_};
    closed_ = true;
    data_cv_.notify_all();
  }

  auto abort() -> void {
    auto lock = std::lock_guard{mutex_};
    aborted_ = true;
    data_cv_.notify_all();
    space_cv_.notify_all();
  }

  auto read(std::span<std::byte> buffer) -> size_t {
    auto lock = std::unique_lock{mutex_};
    data_cv_.wait(lock, [&] {
      return aborted_ or not buffered_.empty() or closed_;
    });
    if (aborted_) {
      return CURL_READFUNC_ABORT;
    }
    if (buffered_.empty()) {
      TENZIR_ASSERT(closed_);
      return 0;
    }
    auto written = size_t{0};
    while (written < buffer.size() and not buffered_.empty()) {
      auto const& front = buffered_.front();
      TENZIR_ASSERT(front);
      auto remaining = front->size() - front_offset_;
      auto count = std::min(buffer.size() - written, remaining);
      std::memcpy(buffer.data() + written, front->data() + front_offset_,
                  count);
      written += count;
      front_offset_ += count;
      if (front_offset_ == front->size()) {
        buffered_.pop_front();
        front_offset_ = 0;
        space_cv_.notify_one();
      }
    }
    return written;
  }

private:
  std::mutex mutex_;
  std::condition_variable data_cv_;
  std::condition_variable space_cv_;
  std::deque<chunk_ptr> buffered_;
  size_t capacity_ = 0;
  size_t front_offset_ = 0;
  bool closed_ = false;
  bool aborted_ = false;
};

auto feed_upload(Arc<PrinterQueue> input, Arc<UploadBridge> bridge)
  -> Task<void> {
  auto discard_remaining_chunks = false;
  while (true) {
    auto chunk = co_await input->dequeue();
    if (not chunk) {
      if (not discard_remaining_chunks) {
        bridge->close();
      }
      co_return;
    }
    if (chunk->size() == 0) {
      continue;
    }
    if (discard_remaining_chunks) {
      continue;
    }
    if (not bridge->push(std::move(chunk))) {
      // Keep draining printer output after an aborted upload so concurrent
      // producers can finish instead of blocking forever on the bounded
      // queue.
      discard_remaining_chunks = true;
    }
  }
}

auto upload(std::string url, Option<located<data>> tls,
            caf::actor_system_config const* cfg, Arc<UploadBridge> bridge,
            Arc<ResultQueue> results) -> Task<void> {
  auto options = transfer_options{};
  options.default_protocol = "ftp";
  auto request = http::request{};
  request.uri = std::move(url);
  options.ssl = make_tls_options(request.uri, tls);
  options.ssl.update_from_config(cfg);
  request.method = "PUT";
  auto tx = transfer{std::move(options)};
  if (auto err = tx.prepare(std::move(request)); err.valid()) {
    co_await results->enqueue(UploadFailed{fmt::format("{}", err)});
    co_return;
  }
  auto code = tx.handle().set([](std::span<const std::byte>) {});
  TENZIR_ASSERT(code == curl::easy::code::ok);
  code
    = tx.handle().set([bridge](std::span<std::byte> buffer) mutable -> size_t {
        return bridge->read(buffer);
      });
  TENZIR_ASSERT(code == curl::easy::code::ok);
  if (auto err = tx.perform(); err.valid()) {
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
    : printer_queue_{std::in_place, input_queue_capacity},
      result_queue_{std::in_place, result_queue_capacity},
      upload_bridge_{std::in_place, upload_bridge_capacity},
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
    auto* cfg = std::addressof(ctx.actor_system().config());
    co_await ctx.spawn_sub<table_slice>(caf::none, std::move(pipeline));
    ctx.spawn_task(folly::coro::co_withExecutor(folly::getGlobalCPUExecutor(),
                                                feed_upload(printer_queue_,
                                                            upload_bridge_)));
    ctx.spawn_task(folly::coro::co_withExecutor(
      folly::getGlobalCPUExecutor(),
      upload(resolved_url_, args_.tls, cfg, upload_bridge_, result_queue_)));
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
      upload_bridge_->abort();
    }
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done or not chunk or chunk->size() == 0) {
      co_return;
    }
    co_await printer_queue_->enqueue(std::move(chunk));
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
        upload_bridge_->abort();
        if (auto sub = ctx.get_sub(caf::none)) {
          auto& printer = as<SubHandle<table_slice>>(*sub);
          co_await printer.close();
        }
        lifecycle_ = Lifecycle::done;
      });
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (lifecycle_ == Lifecycle::done) {
      upload_bridge_->abort();
      co_return FinalizeBehavior::done;
    }
    if (lifecycle_ == Lifecycle::running) {
      lifecycle_ = Lifecycle::draining;
      if (auto sub = ctx.get_sub(caf::none)) {
        auto& printer = as<SubHandle<table_slice>>(*sub);
        co_await printer.close();
        co_return FinalizeBehavior::continue_;
      }
      upload_bridge_->close();
      co_return FinalizeBehavior::continue_;
    }
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    co_await printer_queue_->enqueue(chunk_ptr{});
    co_return;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    lifecycle_ = Lifecycle::done;
    upload_bridge_->abort();
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

  Arc<PrinterQueue> printer_queue_;
  mutable Arc<ResultQueue> result_queue_;
  Arc<UploadBridge> upload_bridge_;
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
