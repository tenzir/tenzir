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
#include <tenzir/async/scope.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/error.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>

#include <fmt/format.h>
#include <folly/coro/BoundedQueue.h>

namespace tenzir::plugins::from_ftp {

namespace {

constexpr auto message_queue_capacity = uint32_t{16};

struct FromFtpArgs {
  located<secret> url;
  Option<located<data>> tls;
  located<ir::pipeline> parser;
};

struct Payload {
  chunk_ptr chunk;
};

struct TransferFailed {
  std::string error;
};

struct TransferDone {};

using Message = variant<Payload, TransferFailed, TransferDone>;
using MessageQueue = folly::coro::BoundedQueue<Message>;

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

auto resolve_url(OpCtx& ctx, FromFtpArgs const& args, std::string& resolved_url)
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

auto set_curl_option(curl::easy& easy, CURLoption option, long value,
                     std::string_view name) -> caf::error {
  if (curl::try_set(easy, option, value)) {
    return {};
  }
  return caf::make_error(ec::unspecified,
                         fmt::format("curl: failed to set `{}`", name));
}

auto set_curl_option(curl::easy& easy, CURLoption option,
                     std::string_view value, std::string_view name)
  -> caf::error {
  if (curl::try_set(easy, option, value)) {
    return {};
  }
  return caf::make_error(ec::unspecified,
                         fmt::format("curl: failed to set `{}`", name));
}

auto configure_download(CurlSession& session, std::string_view url,
                        tls_options const& tls) -> caf::error {
  auto& easy = session.easy();
  if (auto err = set_curl_option(easy, CURLOPT_DEFAULT_PROTOCOL, "ftp",
                                 "CURLOPT_DEFAULT_PROTOCOL");
      err.valid()) {
    return err;
  }
  if (auto err = set_curl_option(easy, CURLOPT_URL, url, "CURLOPT_URL");
      err.valid()) {
    return err;
  }
  if (auto err = set_curl_option(easy, CURLOPT_HTTPGET, 1L, "CURLOPT_HTTPGET");
      err.valid()) {
    return err;
  }
  return tls.apply_to(easy, url, nullptr);
}

auto download(CurlSession* session, CurlReceiveTransfer* receive,
              Arc<MessageQueue> queue) -> Task<void> {
  auto curl_result = CurlResult{CurlCompletion{}};
  co_await async_scope([&](AsyncScope& scope) -> Task<void> {
    scope.spawn([&]() -> Task<void> {
      while (auto chunk = co_await receive->next()) {
        co_await queue->enqueue(Payload{std::move(*chunk)});
      }
    });
    curl_result = co_await receive->wait();
  });
  if (curl_result.is_err()) {
    co_await queue->enqueue(
      TransferFailed{std::move(curl_result).unwrap_err().message});
    co_return;
  }
  if (curl_result.unwrap().kind == CurlCompletionKind::local_abort) {
    co_await queue->enqueue(TransferDone{});
    co_return;
  }
  auto [code, response_code]
    = session->easy().get<curl::easy::info::response_code>();
  if (code == curl::easy::code::ok
      and (response_code < 200 or response_code > 299)) {
    co_await queue->enqueue(
      TransferFailed{fmt::format("FTP response code: {}", response_code)});
    co_return;
  }
  co_await queue->enqueue(TransferDone{});
}

class FromFtp final : public Operator<void, table_slice> {
public:
  explicit FromFtp(FromFtpArgs args)
    : message_queue_{std::in_place, message_queue_capacity},
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
    auto pipeline = args_.parser.inner;
    if (not pipeline.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    tls.update_from_config(std::addressof(ctx.actor_system().config()));
    session_.emplace(CurlSession::make(ctx.io_executor()));
    if (auto err = configure_download(*session_, resolved_url_, tls);
        err.valid()) {
      diagnostic::error("FTP download from `{}` failed: {}", resolved_url_, err)
        .primary(args_.url.source)
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    receive_.emplace(session_->start_receive(
      {.receive_buffer_capacity = message_queue_capacity}));
    co_await ctx.spawn_sub<chunk_ptr>(caf::none, std::move(pipeline));
    ctx.spawn_task(download(&*session_, &*receive_, message_queue_));
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    co_await co_match(
      std::move(result).as<Message>(),
      [&](Payload payload) -> Task<void> {
        if (lifecycle_ != Lifecycle::running) {
          co_return;
        }
        auto sub = ctx.get_sub(caf::none);
        if (not sub) {
          if (receive_) {
            receive_->abort();
          }
          lifecycle_ = Lifecycle::done;
          co_return;
        }
        auto& parser = as<SubHandle<chunk_ptr>>(*sub);
        auto push_result = co_await parser.push(std::move(payload.chunk));
        if (push_result.is_err()) {
          if (receive_) {
            receive_->abort();
          }
          lifecycle_ = Lifecycle::done;
        }
      },
      [&](TransferFailed failure) -> Task<void> {
        diagnostic::error("FTP download from `{}` failed: {}", resolved_url_,
                          failure.error)
          .primary(args_.url.source)
          .emit(ctx);
        if (auto sub = ctx.get_sub(caf::none)) {
          lifecycle_ = Lifecycle::draining;
          auto& parser = as<SubHandle<chunk_ptr>>(*sub);
          co_await parser.close();
        } else {
          lifecycle_ = Lifecycle::done;
        }
      },
      [&](TransferDone) -> Task<void> {
        if (auto sub = ctx.get_sub(caf::none)) {
          lifecycle_ = Lifecycle::draining;
          auto& parser = as<SubHandle<chunk_ptr>>(*sub);
          co_await parser.close();
        } else {
          lifecycle_ = Lifecycle::done;
        }
      });
  }

  auto process_sub(SubKeyView, table_slice slice, Push<table_slice>& push,
                   OpCtx&) -> Task<void> override {
    co_await push(std::move(slice));
  }

  auto finish_sub(SubKeyView, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    lifecycle_ = Lifecycle::done;
    co_return;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    if (receive_) {
      receive_->abort();
    }
    if (auto sub = ctx.get_sub(caf::none)) {
      auto& parser = as<SubHandle<chunk_ptr>>(*sub);
      co_await parser.close();
    }
    lifecycle_ = Lifecycle::done;
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

  Option<CurlSession> session_;
  Option<CurlReceiveTransfer> receive_;
  mutable Arc<MessageQueue> message_queue_;
  FromFtpArgs args_;
  std::string resolved_url_;
  Lifecycle lifecycle_ = Lifecycle::running;
};

class FromFtpPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_ftp";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromFtpArgs, FromFtp>{};
    d.positional("url", &FromFtpArgs::url);
    auto tls_validator = tls_options{
      {.is_server = false}}.add_to_describer(d, &FromFtpArgs::tls);
    auto parser_arg = d.pipeline(&FromFtpArgs::parser);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      tls_validator(ctx);
      TRY(auto parser, ctx.get(parser_arg));
      auto output = parser.inner.infer_type(tag_v<chunk_ptr>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (not *output or (*output)->is_not<table_slice>()) {
        diagnostic::error("pipeline must return events")
          .primary(parser.source.subloc(0, 1))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::from_ftp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_ftp::FromFtpPlugin)
