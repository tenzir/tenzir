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

constexpr auto default_smtp_server = "localhost";
constexpr auto default_from = "Tenzir <noreply@tenzir.com>";
constexpr auto default_subject = "Tenzir Pipeline Data";

namespace {

struct saver_args {
  std::string smtp_server;
  std::string from;
  std::string to;
  std::string subject;

  friend auto inspect(auto& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.email.saver_args")
      .fields(f.field("smtp_server", x.smtp_server), f.field("from", x.from),
              f.field("to", x.to), f.field("subject", x.subject));
  }
};

class saver final : public plugin_saver {
public:
  saver() = default;

  explicit saver(saver_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    auto easy = curl::easy{};
    auto code = easy.set(CURLOPT_URL, args_.smtp_server);
    if (code != curl::easy::code::ok) {
      auto err = to_error(code);
      diagnostic::error("failed to set SMTP server request")
        .note("server: {}", args_.smtp_server)
        .note("{}", err)
        .emit(ctrl.diagnostics());
      return err;
    }
    // This option isn't strictly required and we may consider making it
    // optional.
    code = easy.set(CURLOPT_MAIL_FROM, args_.from);
    if (code != curl::easy::code::ok) {
      auto err = to_error(code);
      diagnostic::error("failed to set From header")
        .note("from: {}", args_.from)
        .note("{}", err)
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
    return [&ctrl, easy = std::make_shared<curl::easy>(std::move(easy))](
             chunk_ptr chunk) mutable {
      if (not chunk || chunk->size() == 0) {
        return;
      }
      // Unless we clean up the cURL handle, we're not going to send the SMPT
      // QUIT command. This allows us to reuse the same connection, but it might
      // not be a good idea because the server will ultimately time out our
      // connection.
      if (auto err = upload(*easy, chunk)) {
        diagnostic::error("failed to set email body")
          .note("{}", err)
          .emit(ctrl.diagnostics());
        return;
      }
      if (auto err = to_error(easy->perform())) {
        diagnostic::error("failed to send email")
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
      name(),
      fmt::format("https://docs.tenzir.com/docs/connectors/{}", name())};
    auto args = saver_args{};
    parser.add(args.smtp_server, "<server>");
    parser.add(args.from, "-f,--from");
    parser.add(args.to, "-t,--to");
    parser.add(args.subject, "-s,--subject");
    parser.parse(p);
    if (args.smtp_server.empty()) {
      args.smtp_server = default_smtp_server;
    } else if (args.smtp_server.find("://") == std::string_view::npos) {
      args.smtp_server.insert(0, "smtp://");
    } else if (args.smtp_server.starts_with("email://")) {
      args.smtp_server.erase(0, 5);
      args.smtp_server.insert(0, "smtp");
    }
    if (args.from.empty()) {
      args.from = default_from;
    }
    if (args.to.empty()) {
      diagnostic::error("no recipient specified")
        .hint("add --to <recipient> to your invocation")
        .throw_();
    }
    if (args.subject.empty()) {
      args.subject = default_subject;
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
