//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "clickhouse/arguments.hpp"
#include "clickhouse/transformers.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/type.hpp"

#include <caf/actor_system_config.hpp>
#include <clickhouse/client.h>

#include <map>
#include <memory>
#include <mutex>
#include <string_view>

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
    located<uint64_t> port = {9000, operator_location};
    std::string user;
    std::string password;
    tls_options ssl;
    ast::expression table = {};
    located<enum mode> mode = located{mode::create_append, operator_location};
    Option<located<std::string>> primary = None{};
    location operator_location;

    auto make_options(const caf::actor_system_config& cfg) const
      -> ::clickhouse::ClientOptions {
      auto opts
        = ::clickhouse::ClientOptions()
            .SetEndpoints({::clickhouse::Endpoint{
              std::string{host}, detail::narrow_cast<uint16_t>(port.inner)}})
            .SetUser(std::string{user})
            .SetPassword(std::string{password})
            .SetDefaultDatabase("");
      if (ssl.get_tls(&cfg).inner) {
        auto tls_opts = ::clickhouse::ClientOptions::SSLOptions{};
        tls_opts.SetSkipVerification(
          ssl.get_skip_peer_verification(&cfg).inner);
        auto commands = std::vector<
          ::clickhouse::ClientOptions::SSLOptions::CommandAndValue>{};
        if (auto x = ssl.get_cacert(&cfg)) {
          commands.emplace_back("ChainCAFile", x->inner);
        }
        if (auto x = ssl.get_certfile(&cfg)) {
          commands.emplace_back("Certificate", x->inner);
        }
        if (auto x = ssl.get_keyfile(&cfg)) {
          commands.emplace_back("PrivateKey", x->inner);
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
  explicit easy_client(arguments args, const caf::actor_system_config& cfg,
                       diagnostic_handler& dh, ctor_token)
    : client_{args.make_options(cfg)},
      args_{std::move(args)},
      dh_{dh, [loc = args_.operator_location](diagnostic diag) -> diagnostic {
            if (not has_location(diag)) {
              diag.annotations.emplace_back(true, std::string{}, loc);
            }
            return diag;
          }} {
  }

  static auto make(arguments args, const caf::actor_system_config& cfg,
                   diagnostic_handler& dh) -> std::shared_ptr<easy_client>;

  auto dh() -> diagnostic_handler& {
    return dh_;
  }

  void ping();

  auto insert_dynamic(const table_slice& slice,
                      std::string_view query_id
                      = ::clickhouse::Query::default_query_id)
    -> failure_or<void>;

private:
  auto insert(const table_slice& slice, std::string_view table_name,
              std::string_view query_id) -> failure_or<void>;
  /// Ensures that the transformation for the given name + schema exists. This
  /// is the main entry point, doing all checking and conditional table creation.
  auto ensure_transformations(const tenzir::record_type& schema,
                              std::string_view table_name)
    -> failure_or<transformer_record*>;
  /// Checks the DB if an object of the given kind exists.
  auto remote_check_exists(std::string_view object_kind,
                           std::string_view object_name) -> failure_or<bool>;
  /// Fetches the transformations from remote. Assumes the table exists remotely.
  auto remote_fetch_schema_transformations(std::string_view table_name)
    -> failure_or<transformer_record*>;
  auto remote_create_database(std::string_view database_name) -> void;
  auto
  /// Creates a table with the given schema.
  remote_create_table(const tenzir::record_type& schema,
                      std::string_view table_name)
    -> failure_or<transformer_record*>;

  ::clickhouse::Client client_;
  std::mutex client_mutex_;
  arguments args_;
  transforming_diagnostic_handler dh_;
  detail::heterogeneous_string_hashmap<transformer_record> transformations_;
  dropmask_type dropmask_;
};

} // namespace tenzir::plugins::clickhouse
