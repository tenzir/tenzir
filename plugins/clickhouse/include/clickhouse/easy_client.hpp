//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "clickhouse/arguments.hpp"
#include "clickhouse/transformers.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/type.hpp"

#include <clickhouse/client.h>

namespace tenzir::plugins::clickhouse {

auto inline has_location(const diagnostic& diag) -> bool {
  for (const auto& a : diag.annotations) {
    if (a.source != location::unknown) {
      return true;
    }
  }
  return false;
}

class easy_client {
public:
  struct arguments {
    std::string host;
    located<uint16_t> port = {9000, operator_location};
    std::string user;
    std::string password;
    ssl_options ssl;
    located<std::string> table = {"REQUIRED", location::unknown};
    located<enum mode> mode = located{mode::create_append, operator_location};
    std::optional<located<std::string>> primary = std::nullopt;
    location operator_location;

    auto make_options() const -> ::clickhouse::ClientOptions {
      auto opts = ::clickhouse::ClientOptions()
                    .SetEndpoints({{std::string{host}, port.inner}})
                    .SetUser(std::string{user})
                    .SetPassword(std::string{password});
      if (ssl.get_tls().inner) {
        auto tls_opts = ::clickhouse::ClientOptions::SSLOptions{};
        tls_opts.SetSkipVerification(ssl.skip_peer_verification.has_value());
        auto commands = std::vector<
          ::clickhouse::ClientOptions::SSLOptions::CommandAndValue>{};
        if (ssl.cacert) {
          commands.emplace_back("ChainCAFile", ssl.cacert->inner);
        }
        if (ssl.certfile) {
          commands.emplace_back("Certificate", ssl.certfile->inner);
        }
        if (ssl.keyfile) {
          commands.emplace_back("PrivateKey", ssl.keyfile->inner);
        }
        tls_opts.SetConfiguration(commands);
        opts.SetSSLOptions(std::move(tls_opts));
      }
      return opts;
    }
  };

private:
  struct ctor_token {};

public:
  explicit easy_client(arguments args, diagnostic_handler& dh, ctor_token)
    : client_{args.make_options()},
      args_{std::move(args)},
      dh_{dh, [loc = args.operator_location](diagnostic diag) -> diagnostic {
            if (not has_location(diag)) {
              diag.annotations.emplace_back(true, std::string{}, loc);
            }
            return diag;
          }} {
  }

  static auto make(arguments args, diagnostic_handler& dh)
    -> std::unique_ptr<easy_client>;

  void ping();

  auto insert(const table_slice& slice) -> bool;

private:
  auto check_if_table_exists() -> bool;
  auto get_schema_transformations() -> failure_or<void>;
  auto create_table(const tenzir::record_type& schema) -> failure_or<void>;

private:
  ::clickhouse::Client client_;
  arguments args_;
  transforming_diagnostic_handler dh_;
  std::optional<transformer_record> transformations_;
  dropmask_type dropmask_;
};

} // namespace tenzir::plugins::clickhouse
