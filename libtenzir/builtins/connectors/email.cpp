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
  bool mime;

  friend auto inspect(auto& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.email.saver_args")
      .fields(f.field("endpoint", x.endpoint), f.field("to", x.to),
              f.field("from", x.from), f.field("subject", x.subject),
              f.field("mime", x.mime));
  }
};

auto make_headers(const saver_args& args) {
  auto result = std::vector<std::pair<std::string, std::string>>{};
  // According to RFC 5322, the Date and From headers are mandatory.
  auto now = fmt::localtime(std::time(nullptr));
  result.emplace_back("Date", fmt::format("{:%a, %d %b %Y %H:%M:%S %z}", now));
  result.emplace_back("To", args.to);
  if (args.from) {
    result.emplace_back("From", *args.from);
  }
  if (args.subject) {
    result.emplace_back("Subject", *args.subject);
  }
  return result;
}

class saver final : public plugin_saver {
public:
  saver() = default;

  explicit saver(saver_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    auto transfer_opts = args_.transfer_opts;
    transfer_opts.ssl.update_cacert(ctrl);
    auto tx = transfer{transfer_opts};
    if (auto err = tx.prepare(std::move(args_.endpoint))) {
      diagnostic::error("failed to prepare SMTP server request")
        .note("{}", err)
        .emit(ctrl.diagnostics());
      return err;
    }
    if (auto err = to_error(tx.handle().set(CURLOPT_UPLOAD, 1))) {
      return err;
    }
    if (args_.from) {
      if (auto err
          = to_error(tx.handle().set(CURLOPT_MAIL_FROM, *args_.from))) {
        diagnostic::error("failed to set MAIL FROM")
          .note("from: {}", *args_.from)
          .note("{}", err)
          .emit(ctrl.diagnostics());
        return err;
      }
    }
    // Allow one of the recipients to fail and still consider it okay.
#if LIBCURL_VERSION_NUM < 0x080200
    auto allowfails = CURLOPT_MAIL_RCPT_ALLLOWFAILS;
#else
    auto allowfails = CURLOPT_MAIL_RCPT_ALLOWFAILS;
#endif
    if (auto err = to_error(tx.handle().set(allowfails, 1))) {
      diagnostic::error("failed to adjust recipient failure mode")
        .note("{}", err)
        .emit(ctrl.diagnostics());
      return err;
    }
    if (auto err = to_error(tx.handle().add_mail_recipient(args_.to))) {
      diagnostic::error("failed to set To header")
        .note("to: {}", args_.to)
        .note("{}", err)
        .emit(ctrl.diagnostics());
      return err;
    }
    if (args_.mime) {
      return [&ctrl, tx = std::make_shared<transfer>(std::move(tx)),
              args = args_](chunk_ptr chunk) mutable {
        if (not chunk || chunk->size() == 0) {
          return;
        }
        // When sending a MIME message, we set the mail headers via
        // CURLOPT_HTTPHEADER as opposed to building the entire message
        // manually.
        auto headers = make_headers(args);
        for (const auto& [name, value] : headers) {
          auto code = tx->handle().set_http_header(name, value);
          TENZIR_ASSERT(code == curl::easy::code::ok);
        }
        // Create the MIME parts.
        auto mime = curl::mime{tx->handle()};
        auto part = mime.add();
        auto code = part.data(as_bytes(chunk));
        TENZIR_ASSERT(code == curl::easy::code::ok);
        code = part.type(chunk->metadata().content_type
                           ? *chunk->metadata().content_type
                           : "text/plain");
        TENZIR_ASSERT(code == curl::easy::code::ok);
        code = tx->handle().set(std::move(mime));
        TENZIR_ASSERT(code == curl::easy::code::ok);
        // Send the message.
        if (auto err = tx->perform()) {
          diagnostic::error("failed to send message")
            .note("{}", err)
            .emit(ctrl.diagnostics());
          return;
        }
      };
    }
    return [&ctrl, tx = std::make_shared<transfer>(std::move(tx)),
            args = args_](chunk_ptr chunk) mutable {
      if (not chunk || chunk->size() == 0) {
        return;
      }
      // Format headers.
      auto headers = std::vector<std::string>{};
      for (const auto& [name, value] : make_headers(args)) {
        headers.push_back(fmt::format("{}: {}", name, value));
      }
      auto body = std::string_view{reinterpret_cast<const char*>(chunk->data()),
                                   chunk->size()};
      auto mail = fmt::format("{}\r\n{}", fmt::join(headers, "\r\n"), body);
      TENZIR_DEBUG("sending {}-byte chunk as email to {}", chunk->size(),
                   args.to);
      if (auto err = set(tx->handle(), chunk::make(std::move(mail)))) {
        diagnostic::error("failed to assign message")
          .note("{}", err)
          .emit(ctrl.diagnostics());
        return;
      }
      // Send the message.
      if (auto err = tx->perform()) {
        diagnostic::error("failed to send message")
          .note("{}", err)
          .emit(ctrl.diagnostics());
        return;
      }
    };
  }

  auto name() const -> std::string override {
    return "email";
  }

  auto default_printer() const -> std::string override {
    return "json";
  }

  auto is_joining() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, saver& x) -> bool {
    return f.object(x).pretty_name("saver").fields(f.field("args", x.args_));
  }

private:
  saver_args args_;
};

class plugin final : public virtual saver_plugin<saver> {
public:
  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = saver_args{};
    parser.add("-e,--endpoint", args.endpoint, "<string>");
    parser.add("-f,--from", args.from, "<email>");
    parser.add("-s,--subject", args.subject, "<string>");
    parser.add("-u,--username", args.transfer_opts.username, "<string>");
    parser.add("-p,--password", args.transfer_opts.password, "<string>");
    parser.add("-i,--authzid", args.transfer_opts.authzid, "<string>");
    parser.add("-a,--authorization", args.transfer_opts.authorization,
               "<string>");
    parser.add("-P,--skip-peer-verification",
               args.transfer_opts.ssl.skip_peer_verification);
    parser.add("-H,--skip-hostname-verification",
               args.transfer_opts.ssl.skip_peer_verification);
    parser.add("-m,--mime", args.mime);
    parser.add("-v,--verbose", args.transfer_opts.verbose);
    parser.add(args.to, "<email>");
    parser.parse(p);
    if (args.endpoint.empty()) {
      args.endpoint = default_smtp_server;
    } else if (args.endpoint.find("://") == std::string_view::npos) {
      args.endpoint.insert(0, "smtps://");
    } else if (args.endpoint.starts_with("email://")) {
      args.endpoint.erase(0, 5);
      args.endpoint.insert(0, "smtp");
    }
    if (args.to.empty()) {
      diagnostic::error("no recipient specified")
        .hint("add --to <recipient> to your invocation")
        .throw_();
    }
    return std::make_unique<saver>(std::move(args));
  }

  auto name() const -> std::string override {
    return "email";
  }

  auto supported_uri_schemes() const -> std::vector<std::string> override {
    return {"mailto"};
  }
};

class save_plugin final
  : public virtual operator_plugin2<saver_adapter<saver>> {
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
    parser.named("username", args.transfer_opts.username);
    parser.named("password", args.transfer_opts.password);
    parser.named("authzid", args.transfer_opts.authzid);
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
    TRY(args.transfer_opts.ssl.validate(
      located{args.endpoint, location::unknown}, ctx));
    if (to.inner.empty()) {
      diagnostic::error("empty recipient specified").primary(to).emit(ctx);
      return failure::promise();
    }
    args.to = std::move(to.inner);
    return std::make_unique<saver_adapter<saver>>(saver{std::move(args)});
  }
};

} // namespace

} // namespace tenzir::plugins::email

TENZIR_REGISTER_PLUGIN(tenzir::plugins::email::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::email::save_plugin)
