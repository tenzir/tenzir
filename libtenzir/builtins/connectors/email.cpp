//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/curl.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>

using namespace std::chrono_literals;

namespace tenzir::plugins::email {

constexpr auto default_smtp_server = "smtp://localhost:25";

namespace {

struct saver_args {
  std::string smtp_server;
  std::string to;
  std::optional<std::string> from;
  std::optional<std::string> subject;
  std::optional<std::string> username;
  std::optional<std::string> password;
  bool skip_peer_verification;
  bool skip_hostname_verification;
  bool verbose;

  friend auto inspect(auto& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.email.saver_args")
      .fields(f.field("smtp_server", x.smtp_server), f.field("to", x.to),
              f.field("from", x.from), f.field("subject", x.subject),
              f.field("username", x.username), f.field("password", x.password),
              f.field("skip_peer_verification", x.skip_peer_verification),
              f.field("skip_host_verification", x.skip_hostname_verification),
              f.field("verbose", x.verbose));
  }
};

auto make_header(const saver_args& args) {
  auto result = std::string{};
  // According to RFC 5322, the Date and From headers are mandatory.
  auto now = fmt::localtime(std::time(nullptr));
  result += fmt::format("Date: {:%a, %d %b %Y %H:%M:%S %z}\r\n", now);
  result += fmt::format("To: {}\r\n", args.to);
  if (args.from) {
    result += fmt::format("From: {}\r\n", *args.from);
  }
  if (args.subject) {
    result += fmt::format("Subject: {}\r\n", *args.subject);
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
    auto easy = curl::easy{};
    if (args_.verbose) {
      auto code = easy.set(CURLOPT_VERBOSE, 1);
      TENZIR_ASSERT(code == curl::easy::code::ok);
    }
    auto code = easy.set(CURLOPT_URL, args_.smtp_server);
    if (code != curl::easy::code::ok) {
      auto err = to_error(code);
      diagnostic::error("failed to set SMTP server request")
        .note("server: {}", args_.smtp_server)
        .note("{}", err)
        .emit(ctrl.diagnostics());
      return err;
    }
    if (args_.skip_peer_verification) {
      code = easy.set(CURLOPT_SSL_VERIFYPEER, 0);
      TENZIR_ASSERT(code == curl::easy::code::ok);
    }
    if (args_.skip_hostname_verification) {
      code = easy.set(CURLOPT_SSL_VERIFYHOST, 0);
      TENZIR_ASSERT(code == curl::easy::code::ok);
    }
    if (args_.username) {
      code = easy.set(CURLOPT_USERNAME, *args_.username);
      if (code != curl::easy::code::ok) {
        auto err = to_error(code);
        diagnostic::error("failed to set user name")
          .note("{}", err)
          .emit(ctrl.diagnostics());
        return err;
      }
    }
    if (args_.password) {
      code = easy.set(CURLOPT_PASSWORD, *args_.password);
      if (code != curl::easy::code::ok) {
        auto err = to_error(code);
        diagnostic::error("failed to set password")
          .note("{}", err)
          .emit(ctrl.diagnostics());
        return err;
      }
    }
    if (args_.from) {
      code = easy.set(CURLOPT_MAIL_FROM, *args_.from);
      if (code != curl::easy::code::ok) {
        auto err = to_error(code);
        diagnostic::error("failed to set From header")
          .note("from: {}", *args_.from)
          .note("{}", err)
          .emit(ctrl.diagnostics());
        return err;
      }
    }
    // Allow one of the recipients to fail and still consider it okay.
    code = easy.set(CURLOPT_MAIL_RCPT_ALLOWFAILS, 1);
    if (code != curl::easy::code::ok) {
      auto err = to_error(code);
      diagnostic::error("failed to adjust recipient failure mode")
        .note("{}", err)
        .note("cURL option: CURLOPT_MAIL_RCPT_ALLOWFAILS")
        .emit(ctrl.diagnostics());
      return err;
    }
    code = easy.add_mail_recipient(args_.to);
    if (code != curl::easy::code::ok) {
      auto err = to_error(code);
      diagnostic::error("failed to set To header")
        .note("to: {}", args_.to)
        .note("{}", err)
        .emit(ctrl.diagnostics());
      return err;
    }
    return [&ctrl, easy = std::make_shared<curl::easy>(std::move(easy)),
            args = args_](chunk_ptr chunk) mutable {
      if (not chunk || chunk->size() == 0) {
        return;
      }
      auto header = make_header(args);
      auto body = std::string_view{reinterpret_cast<const char*>(chunk->data()),
                                   chunk->size()};
      // The RFC demands that we end with `.<CR><LF>`.
      auto mail = fmt::format("{}\r\n{}.\r\n", header, body);
      TENZIR_DEBUG("sending {}-byte chunk as email to {}", chunk->size(),
                   args.to);
      if (auto err = upload(*easy, chunk::make(std::move(mail)))) {
        diagnostic::error("failed to assign message")
          .note("{}", err)
          .emit(ctrl.diagnostics());
        return;
      }
      if (auto err = to_error(easy->perform())) {
        diagnostic::error("failed to send message")
          .note("{}", err)
          .emit(ctrl.diagnostics());
        return;
      }
      // Unless we clean up the cURL handle, we're not going to send a QUIT
      // command. This allows us to reuse the same connection, but it might
      // not be a good idea because the server will ultimately time out our
      // connection.
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
      name(),
      fmt::format("https://docs.tenzir.com/docs/connectors/{}", name())};
    auto args = saver_args{};
    parser.add("-S,--server", args.smtp_server, "<string>");
    parser.add("-f,--from", args.from, "<email>");
    parser.add("-s,--subject", args.subject, "<string>");
    parser.add("-P,--skip-peer-verification", args.skip_peer_verification);
    parser.add("-H,--skip-hostname-verification",
               args.skip_hostname_verification);
    parser.add("-v,--verbose", args.verbose);
    parser.add(args.to, "<email>");
    parser.parse(p);
    if (args.smtp_server.empty()) {
      args.smtp_server = default_smtp_server;
    } else if (args.smtp_server.find("://") == std::string_view::npos) {
      args.smtp_server.insert(0, "smtp://");
    } else if (args.smtp_server.starts_with("email://")) {
      args.smtp_server.erase(0, 5);
      args.smtp_server.insert(0, "smtp");
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
};

} // namespace

} // namespace tenzir::plugins::email

TENZIR_REGISTER_PLUGIN(tenzir::plugins::email::plugin)
