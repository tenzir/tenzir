//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "clickhouse/table_description.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tls_options.hpp"

#include <boost/url/url_view_base.hpp>
#include <clickhouse/block.h>
#include <clickhouse/client.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::clickhouse {

/// Stay below the server's default receive timeout of five minutes.
inline constexpr auto connection_ping_interval = std::chrono::minutes{3};

/// Everything needed to open a connection to the server.
struct ConnectionArgs {
  std::string host;
  uint16_t port = 9000;
  std::string user = "default";
  std::string password;
  /// Raw, unquoted database name. Table names without a database are
  /// qualified with it.
  Option<std::string> default_database = None{};
  /// Must be set before connecting.
  Option<TlsConfig> tls = None{};

  auto apply_uri(boost::urls::url_view_base const& uri) -> void;
  auto to_client_options() const -> ::clickhouse::ClientOptions;
};

/// One server connection. It is not thread-safe, so every user must hold it
/// exclusively.
class Connection {
public:
  explicit Connection(ConnectionArgs const& args);

  auto client() -> ::clickhouse::Client& {
    return client_;
  }

private:
  ::clickhouse::Client client_;
};

/// Prefixes `table` with the quoted `default_database` unless it already names
/// a database.
auto qualified_table_name(Option<std::string_view> default_database,
                          std::string_view table) -> std::string;

// The functions below perform blocking round-trips and throw on transport
// errors. Run them through `spawn_blocking`.

/// Runs `EXISTS <kind> <name>`.
auto exists(Connection& connection, std::string_view kind,
            std::string_view name, diagnostic_handler& dh) -> failure_or<bool>;

auto create_database(Connection& connection, std::string_view database) -> void;

/// Executes a `CREATE TABLE` statement without the server's query size limit,
/// as the statement is derived from a possibly wide schema.
auto create_table(Connection& connection, std::string query) -> void;

auto describe_table(Connection& connection, std::string_view table,
                    diagnostic_handler& dh)
  -> failure_or<std::vector<ColumnDescription>>;

/// Sends `block` as one streaming INSERT into `table`. Resets the connection
/// before rethrowing a failure, because the client otherwise leaves the
/// streaming insert open.
auto insert(Connection& connection, ::clickhouse::Block const& block,
            std::string_view table, std::string_view query_id, bool escape_dots,
            diagnostic_handler& dh) -> void;

} // namespace tenzir::plugins::clickhouse
