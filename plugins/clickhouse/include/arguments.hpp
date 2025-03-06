//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/tql2/plugin.hpp"
#include "tenzir/detail/enum.hpp"
#include "tenzir/argument_parser2.hpp"

#include <boost/url/parse.hpp>

#include "clickhouse/client.h"

namespace tenzir::plugins::clickhouse {

  TENZIR_ENUM(mode, create_append, create, append);

  struct Arguments {
    tenzir::location operator_location;
    located<std::string> host = {"localhost", operator_location};
    located<uint16_t> port = {9000, operator_location};
    located<std::string> user = {"default", operator_location};
    located<std::string> password = {"", operator_location};
    located<std::string> table = {"REQUIRED", location::unknown};
    located<enum mode> mode = located{mode::create_append, operator_location};
    std::optional<located<std::string>> primary = std::nullopt;

    std::optional<located<bool>> tls = std::nullopt;
    std::optional<located<bool>> skip_peer_verification = std::nullopt;
    std::optional<located<std::string>> cacert = std::nullopt;
    std::optional<located<std::string>> certfile = std::nullopt;
    std::optional<located<std::string>> keyfile = std::nullopt;

    static auto try_parse(std::string operator_name,
                          operator_factory_plugin::invocation inv,
                          session ctx) -> failure_or<Arguments> {
      auto res = Arguments{inv.self.get_location()};
      auto mode_str = located<std::string>{
        to_string(mode::create_append),
        res.operator_location,
      };
      auto url_str = located<std::string>{
        res.host.inner + ":" + std::to_string(res.port.inner),
        res.operator_location,
      };
      auto primary_selector = std::optional<ast::simple_selector>{};
      auto parser = argument_parser2::operator_(operator_name);
      parser.named_optional("url", url_str);
      parser.named_optional("user", res.user);
      parser.named_optional("password", res.password);
      parser.named("table", res.table);
      parser.named_optional("mode", mode_str);
      parser.named("primary", primary_selector, "field");
      parser.named("tls", res.tls);
      parser.named("skip_peer_verification", res.skip_peer_verification);
      parser.named("cacert", res.cacert);
      parser.named("certfile", res.certfile);
      parser.named("keyfile", res.keyfile);
      TRY(parser.parse(inv, ctx));
      auto parsed_url = boost::urls::parse_uri(url_str.inner);
      if (not parsed_url) {
        res.host = url_str;
      } else {
        if (parsed_url->has_port()) {
          res.port = {parsed_url->port_number(), url_str.source};
        }
        res.host = {parsed_url->host(), url_str.source};
      }
      if (auto x = from_string<enum mode>(mode_str.inner)) {
        res.mode = located{*x, mode_str.source};
      } else {
        diagnostic::error(
          "`mode` must be one of `create`, `append` or `create_append`")
          .primary(mode_str, "got `{}`", mode_str.inner)
          .emit(ctx);
        return failure::promise();
      }
      if (res.mode.inner == mode::create and not res.primary) {
        diagnostic::error("mode `create` requires `primary` to be set")
          .primary(mode_str)
          .emit(ctx);
        return failure::promise();
      }
      if (primary_selector) {
        auto p = primary_selector->path();
        if (p.size() > 1) {
          diagnostic::error("`primary`, must be a top level field")
            .primary(primary_selector->get_location())
            .emit(ctx);
          return failure::promise();
        }
        res.primary = {p.front().name, primary_selector->get_location()};
      }
      // TODO this thing should really be a common component somewhere...
      auto tls_logic
        = [&](auto& thing, std::string_view name) -> failure_or<void> {
        if (res.tls and not res.tls->inner and thing) {
          diagnostic::error("`{}` requires TLS", name)
            .primary(res.tls->source, "TLS is disabled")
            .primary(*thing)
            .emit(ctx);
          return failure::promise();
        }
        if (thing and not res.tls) {
          res.tls = located{true, into_location{*thing}};
        }
        return {};
      };
      TRY(tls_logic(res.skip_peer_verification, "skip_peer_verification"));
      TRY(tls_logic(res.cacert, "cacert"));
      TRY(tls_logic(res.certfile, "certfile"));
      TRY(tls_logic(res.keyfile, "keyfile"));
      return res;
    }

    auto make_options() const -> ::clickhouse::ClientOptions {
      auto opts = ::clickhouse::ClientOptions()
                    .SetEndpoints({{host.inner, port.inner}})
                    .SetUser(user.inner)
                    .SetPassword(password.inner);
      if (tls and tls->inner) {
        auto tls_opts = ::clickhouse::ClientOptions::SSLOptions{};
        tls_opts.SetSkipVerification(skip_peer_verification
                                     and skip_peer_verification->inner);
        auto commands = std::vector<
          ::clickhouse::ClientOptions::SSLOptions::CommandAndValue>{};
        if (cacert) {
          commands.emplace_back("ChainCAFile", cacert->inner);
        }
        if (certfile) {
          commands.emplace_back("Certificate", certfile->inner);
        }
        if (keyfile) {
          commands.emplace_back("PrivateKey", keyfile->inner);
        }
        tls_opts.SetConfiguration(commands);
        opts.SetSSLOptions(std::move(tls_opts));
      }
      return opts;
    }

    friend auto inspect(auto& f, Arguments& x) -> bool {
      return f.object(x).fields(
        f.field("operator_location", x.operator_location),
        f.field("host", x.host), f.field("port", x.port),
        f.field("user", x.user), f.field("password", x.password),
        f.field("table", x.table), f.field("mode", x.mode),
        f.field("primary", x.primary), f.field("tls", x.tls),
        f.field("skip_peer_verification", x.skip_peer_verification),
        f.field("cacert", x.cacert), f.field("certfile", x.certfile),
        f.field("keyfile", x.keyfile));
    }
  };
}
