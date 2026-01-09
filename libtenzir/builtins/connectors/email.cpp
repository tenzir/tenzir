//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/transfer.hpp>

#include <string_view>

using namespace std::chrono_literals;

namespace tenzir::plugins::email {

constexpr auto default_smtp_server = "smtp://localhost:25";

namespace {

struct saver_args {
  std::string endpoint;
  std::string to;
  std::optional<std::string> from;
  std::optional<std::string> subject;
  transfer_options transfer_opts;
  std::optional<located<secret>> username;
  std::optional<located<secret>> password;
  std::optional<located<secret>> authzid;

  bool mime;

  friend auto inspect(auto& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.email.saver_args")
      .fields(f.field("endpoint", x.endpoint), f.field("to", x.to),
              f.field("from", x.from), f.field("subject", x.subject),
              f.field("username", x.username), f.field("password", x.password),
              f.field("authzid", x.authzid), f.field("mime", x.mime));
  }
};

auto make_headers(const saver_args& args) {
  auto result = std::vector<std::pair<std::string, std::string>>{};
  // According to RFC 5322, the Date and From headers are mandatory.
  const auto time = std::time(nullptr);
  auto tm = std::tm{};
  const auto* res = localtime_r(&time, &tm);
  TENZIR_ASSERT(res != nullptr);
  result.emplace_back("Date", fmt::format("{:%a, %d %b %Y %H:%M:%S %z}", tm));
  result.emplace_back("To", args.to);
  if (args.from) {
    result.emplace_back("From", *args.from);
  }
  if (args.subject) {
    result.emplace_back("Subject", *args.subject);
  }
  return result;
}

class saver final : public crtp_operator<saver> {
public:
  saver() = default;

  explicit saver(saver_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    auto& dh = ctrl.diagnostics();
    auto transfer_opts = args_.transfer_opts;
    auto requests = std::vector<secret_request>{};
    if (args_.username) {
      transfer_opts.username.emplace();
      requests.emplace_back(make_secret_request("username", *args_.username,
                                                *transfer_opts.username, dh));
    }
    if (args_.password) {
      transfer_opts.password.emplace();
      requests.emplace_back(make_secret_request("password", *args_.password,
                                                *transfer_opts.password, dh));
    }
    if (args_.authzid) {
      transfer_opts.authzid.emplace();
      requests.emplace_back(make_secret_request("authzid", *args_.authzid,
                                                *transfer_opts.authzid, dh));
    }
    co_yield ctrl.resolve_secrets_must_yield(std::move(requests));
    transfer_opts.ssl.update_from_config(ctrl);
    auto tx = transfer{transfer_opts};
    if (auto err = tx.prepare(std::move(args_.endpoint)); err.valid()) {
      diagnostic::error("failed to prepare SMTP server request")
        .note("{}", err)
        .emit(ctrl.diagnostics());
    }
    if (auto err = to_error(tx.handle().set(CURLOPT_UPLOAD, 1)); err.valid()) {
      diagnostic::error(err).emit(ctrl.diagnostics());
    }
    if (args_.from) {
      if (auto err = to_error(tx.handle().set(CURLOPT_MAIL_FROM, *args_.from));
          err.valid()) {
        diagnostic::error("failed to set MAIL FROM")
          .note("from: {}", *args_.from)
          .note("{}", err)
          .emit(ctrl.diagnostics());
      }
    }
    // Allow one of the recipients to fail and still consider it okay.
#if LIBCURL_VERSION_NUM < 0x080200
    auto allowfails = CURLOPT_MAIL_RCPT_ALLLOWFAILS;
#else
    auto allowfails = CURLOPT_MAIL_RCPT_ALLOWFAILS;
#endif
    if (auto err = to_error(tx.handle().set(allowfails, 1)); err.valid()) {
      diagnostic::error("failed to adjust recipient failure mode")
        .note("{}", err)
        .emit(ctrl.diagnostics());
    }
    if (auto err = to_error(tx.handle().add_mail_recipient(args_.to));
        err.valid()) {
      diagnostic::error("failed to set To header")
        .note("to: {}", args_.to)
        .note("{}", err)
        .emit(ctrl.diagnostics());
    }
    if (args_.mime) {
      for (auto chunk : input) {
        if (not chunk || chunk->size() == 0) {
          co_yield {};
        }
        // When sending a MIME message, we set the mail headers via
        // CURLOPT_HTTPHEADER as opposed to building the entire message
        // manually.
        auto headers = make_headers(args_);
        for (const auto& [name, value] : headers) {
          auto code = tx.handle().set_http_header(name, value);
          TENZIR_ASSERT(code == curl::easy::code::ok);
        }
        // Create the MIME parts.
        auto mime = curl::mime{tx.handle()};
        auto part = mime.add();
        auto code = part.data(as_bytes(chunk));
        TENZIR_ASSERT(code == curl::easy::code::ok);
        code = part.type(chunk->metadata().content_type
                           ? *chunk->metadata().content_type
                           : "text/plain");
        TENZIR_ASSERT(code == curl::easy::code::ok);
        code = tx.handle().set(std::move(mime));
        TENZIR_ASSERT(code == curl::easy::code::ok);
        // Send the message.
        if (auto err = tx.perform(); err.valid()) {
          diagnostic::error("failed to send message")
            .note("{}", err)
            .emit(ctrl.diagnostics());
        }
      };
    }
    for (auto chunk : input) {
      if (not chunk || chunk->size() == 0) {
        co_yield {};
        continue;
      }
      // Format headers.
      auto headers = std::vector<std::string>{};
      for (const auto& [name, value] : make_headers(args_)) {
        headers.push_back(fmt::format("{}: {}", name, value));
      }
      auto body = std::string_view{reinterpret_cast<const char*>(chunk->data()),
                                   chunk->size()};
      auto mail = fmt::format("{}\r\n{}", fmt::join(headers, "\r\n"), body);
      TENZIR_DEBUG("sending {}-byte chunk as email to {}", chunk->size(),
                   args_.to);
      if (auto err = set(tx.handle(), chunk::make(std::move(mail)));
          err.valid()) {
        diagnostic::error("failed to assign message")
          .note("{}", err)
          .emit(ctrl.diagnostics());
      }
      // Send the message.
      if (auto err = tx.perform(); err.valid()) {
        diagnostic::error("failed to send message")
          .note("{}", err)
          .emit(ctrl.diagnostics());
      }
    };
  }

  auto detached() const -> bool override {
    return true;
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "save_email";
  }

  friend auto inspect(auto& f, saver& x) -> bool {
    return f.object(x).pretty_name("saver").fields(f.field("args", x.args_));
  }

private:
  saver_args args_;
};

class save_plugin final : public virtual operator_plugin2<saver> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = saver_args{};
    auto endpoint = std::optional<std::string>{default_smtp_server};
    auto to = located<std::string>{};
    auto parser = argument_parser2::operator_(name());
    parser.positional("recipient", to);
    parser.named("endpoint", endpoint);
    parser.named("from", args.from);
    parser.named("subject", args.subject);
    parser.named("username", args.username);
    parser.named("password", args.password);
    parser.named("authzid", args.authzid);
    parser.named("authorization", args.transfer_opts.authorization);
    args.transfer_opts.ssl.add_tls_options(parser);
    parser.named("mime", args.mime);
    parser.named("_verbose", args.transfer_opts.verbose);
    TRY(parser.parse(inv, ctx));
    args.endpoint = std::move(endpoint).value();
    if (args.endpoint.find("://") == std::string_view::npos) {
      args.endpoint.insert(0, "smtps://");
    } else if (args.endpoint.starts_with("email://")) {
      args.endpoint.erase(0, 5);
      args.endpoint.insert(0, "smtp");
    }
    TRY(args.transfer_opts.ssl.validate(args.endpoint, location::unknown, ctx));
    if (to.inner.empty()) {
      diagnostic::error("empty recipient specified").primary(to).emit(ctx);
      return failure::promise();
    }
    args.to = std::move(to.inner);
    return std::make_unique<saver>(std::move(args));
  }

  auto save_properties() const -> save_properties_t override {
    return {
      .schemes = {"smtp", "smtps", "mailto", "email"},
    };
  }
};

} // namespace

} // namespace tenzir::plugins::email

TENZIR_REGISTER_PLUGIN(tenzir::plugins::email::save_plugin)
