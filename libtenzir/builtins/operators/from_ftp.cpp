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

namespace tenzir::plugins::from_ftp {

namespace {

constexpr auto download_buffer_capacity = size_t{16};

struct FromFtpArgs {
  located<secret> url;
  Option<located<data>> tls;
  located<ir::pipeline> parser;
};

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

class FromFtp final : public Operator<void, table_slice> {
public:
  explicit FromFtp(FromFtpArgs args) : args_{std::move(args)} {
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
    auto& easy = session_->easy();
    if (not curl::try_set(easy, CURLOPT_DEFAULT_PROTOCOL, "ftp")) {
      diagnostic::error("failed to configure FTP download from `{}`",
                        resolved_url_)
        .primary(args_.url.source)
        .note("failed to set curl option `CURLOPT_DEFAULT_PROTOCOL`")
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    if (not curl::try_set(easy, CURLOPT_URL, resolved_url_)) {
      diagnostic::error("failed to configure FTP download from `{}`",
                        resolved_url_)
        .primary(args_.url.source)
        .note("failed to set curl option `CURLOPT_URL`")
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    if (not curl::try_set(easy, CURLOPT_HTTPGET, 1L)) {
      diagnostic::error("failed to configure FTP download from `{}`",
                        resolved_url_)
        .primary(args_.url.source)
        .note("failed to set curl option `CURLOPT_HTTPGET`")
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    if (auto err = tls.apply_to(easy, resolved_url_, nullptr); err.valid()) {
      diagnostic::error("failed to configure FTP download from `{}`",
                        resolved_url_)
        .primary(args_.url.source)
        .note("{}", err)
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    download_.emplace(session_->start_download(download_buffer_capacity));
    co_await ctx.spawn_sub<chunk_ptr>(caf::none, std::move(pipeline));
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    TENZIR_ASSERT(download_);
    if (auto event = co_await download_->next()) {
      co_return Any{std::in_place_type<CurlDownloadEvent>, std::move(*event)};
    }
    co_return Any{std::in_place_type<CurlDownloadEvent>,
                  CurlDownloadDone{CurlTransferStatus::local_abort}};
  }

  auto process_task(Any result, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    co_await co_match(
      std::move(result).as<CurlDownloadEvent>(),
      [&](CurlDownloadChunk event) -> Task<void> {
        if (lifecycle_ != Lifecycle::running) {
          co_return;
        }
        auto sub = ctx.get_sub(caf::none);
        if (not sub) {
          if (download_) {
            download_->abort();
          }
          lifecycle_ = Lifecycle::done;
          co_return;
        }
        auto& parser = as<SubHandle<chunk_ptr>>(*sub);
        auto push_result = co_await parser.push(std::move(event.chunk));
        if (push_result.is_err()) {
          if (download_) {
            download_->abort();
          }
          lifecycle_ = Lifecycle::done;
        }
      },
      [&](CurlDownloadFailed failure) -> Task<void> {
        diagnostic::error("FTP download from `{}` failed", resolved_url_)
          .primary(args_.url.source)
          .note("curl error: {}", to_string(failure.error))
          .emit(ctx);
        if (auto sub = ctx.get_sub(caf::none)) {
          lifecycle_ = Lifecycle::draining;
          auto& parser = as<SubHandle<chunk_ptr>>(*sub);
          co_await parser.close();
        } else {
          lifecycle_ = Lifecycle::done;
        }
      },
      [&](CurlDownloadDone done) -> Task<void> {
        if (done.status == CurlTransferStatus::finished) {
          auto [code, response_code]
            = session_->easy().get<curl::easy::info::response_code>();
          if (code == curl::easy::code::ok
              and (response_code < 200 or response_code > 299)) {
            diagnostic::error("FTP download from `{}` failed", resolved_url_)
              .primary(args_.url.source)
              .note("FTP response code: {}", response_code)
              .emit(ctx);
          }
        }
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
    if (download_) {
      download_->abort();
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
  Option<CurlDownloadTransfer> download_;
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
