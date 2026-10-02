//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/connection.hpp"

#include "clickhouse/arguments.hpp"

#include <clickhouse/columns/numeric.h>
#include <fmt/format.h>

#include <ranges>

namespace tenzir::plugins::clickhouse {

auto ConnectionArgs::apply_uri(boost::urls::url_view_base const& uri) -> void {
  host = std::string{uri.host()};
  if (uri.has_port()) {
    port = uri.port_number();
  }
  if (uri.has_userinfo()) {
    user = std::string{uri.user()};
  }
  if (uri.has_password()) {
    password = std::string{uri.password()};
  }
  default_database = None{};
  auto segments = uri.segments();
  if (not segments.empty()) {
    auto database = std::string{segments.front()};
    if (not database.empty()) {
      default_database = std::move(database);
    }
  }
}

auto ConnectionArgs::to_client_options() const -> ::clickhouse::ClientOptions {
  auto options = ::clickhouse::ClientOptions()
                   .SetEndpoints({::clickhouse::Endpoint{host, port}})
                   .SetUser(user)
                   .SetPassword(password);
  TENZIR_ASSERT(tls);
  if (tls->tls.inner) {
    auto ssl = ::clickhouse::ClientOptions::SSLOptions{};
    ssl.SetSkipVerification(tls->skip_peer_verification.inner);
    auto commands
      = std::vector<::clickhouse::ClientOptions::SSLOptions::CommandAndValue>{};
    if (tls->cacert) {
      commands.emplace_back("ChainCAFile", tls->cacert->inner);
    }
    if (tls->certfile) {
      commands.emplace_back("Certificate", tls->certfile->inner);
    }
    if (tls->keyfile) {
      commands.emplace_back("PrivateKey", tls->keyfile->inner);
    }
    ssl.SetConfiguration(commands);
    options.SetSSLOptions(std::move(ssl));
  }
  return options;
}

Connection::Connection(ConnectionArgs const& args)
  : client_{args.to_client_options()} {
}

auto qualified_table_name(Option<std::string_view> default_database,
                          std::string_view table) -> std::string {
  if (not default_database
      or table_name_quoting.split_at_unquoted(table, '.')) {
    return std::string{table};
  }
  return fmt::format("{}.{}", quote_identifier_component(*default_database),
                     table);
}

auto exists(Connection& connection, std::string_view kind,
            std::string_view name, diagnostic_handler& dh) -> failure_or<bool> {
  auto query = ::clickhouse::Query{fmt::format("EXISTS {} {}", kind, name)};
  auto result = Option<bool>{};
  auto ok = true;
  auto unexpected = [&](std::string_view note) {
    diagnostic::error("unexpected clickhouse response")
      .note("when checking for existence of {} `{}`", kind, name)
      .note("{}", note)
      .emit(dh);
    ok = false;
  };
  query.OnData([&](::clickhouse::Block const& block) {
    if (not ok or block.GetColumnCount() == 0) {
      return;
    }
    if (block.GetColumnCount() != 1) {
      unexpected("block should have exactly one column");
      return;
    }
    auto column = block[0]->As<::clickhouse::ColumnUInt8>();
    if (not column) {
      unexpected("expected uint8 column");
      return;
    }
    if (column->Size() == 0) {
      return;
    }
    if (column->Size() != 1 or block.GetRowCount() != 1) {
      unexpected("expected exactly one row in the data block");
      return;
    }
    if (result) {
      unexpected("expected exactly one data block");
      return;
    }
    result = column->At(0) == 1;
  });
  connection.client().Execute(query);
  if (not ok) {
    return failure::promise();
  }
  if (not result) {
    unexpected("expected exactly one block with one uint8 column and one row");
    return failure::promise();
  }
  return *result;
}

auto create_database(Connection& connection, std::string_view database)
  -> void {
  connection.client().Execute(::clickhouse::Query{
    fmt::format("CREATE DATABASE IF NOT EXISTS {}", database)});
}

auto create_table(Connection& connection, std::string query) -> void {
  auto statement = ::clickhouse::Query{std::move(query)};
  statement.SetSetting("max_query_size",
                       {"0", ::clickhouse::QuerySettingsField::IMPORTANT});
  connection.client().Execute(statement);
}

auto describe_table(Connection& connection, std::string_view table,
                    diagnostic_handler& dh)
  -> failure_or<std::vector<ColumnDescription>> {
  auto query = ::clickhouse::Query{fmt::format(
    "DESCRIBE TABLE {} SETTINGS describe_compact_output = 0", table)};
  auto description = std::vector<ColumnDescription>{};
  auto failed = false;
  query.OnData([&](::clickhouse::Block const& block) {
    if (failed or block.GetRowCount() == 0) {
      return;
    }
    auto columns = read_description(block, dh);
    if (not columns) {
      failed = true;
      return;
    }
    description.append_range(std::views::as_rvalue(*columns));
  });
  connection.client().Execute(query);
  if (failed) {
    return failure::promise();
  }
  return description;
}

auto insert(Connection& connection, ::clickhouse::Block const& block,
            std::string_view table, std::string_view query_id, bool escape_dots,
            diagnostic_handler& dh) -> void {
  auto fields = std::vector<std::string>{};
  fields.reserve(block.GetColumnCount());
  for (auto i = size_t{0}; i < block.GetColumnCount(); ++i) {
    fields.push_back(quote_identifier_component(block.GetColumnName(i)));
  }
  // Escaping keeps dotted JSON keys literal instead of nesting them.
  auto query = fmt::format(
    "INSERT INTO {} ({}){} VALUES", table, fmt::join(fields, ","),
    escape_dots ? " SETTINGS json_type_escape_dots_in_keys=1" : "");
  auto& client = connection.client();
  try {
    std::ignore = client.BeginInsert(query, std::string{query_id});
    client.SendInsertBlock(block);
    client.EndInsert();
  } catch (std::exception const& error) {
    // The reset reconnects without replaying the block, also when a transport
    // failure leaves its acceptance unknown.
    try {
      client.ResetConnection();
    } catch (std::exception const& reset_error) {
      diagnostic::error("cannot reset ClickHouse connection after failed "
                        "insert: {}",
                        reset_error.what())
        .note("original insertion error: {}", error.what())
        .emit(dh);
    }
    throw;
  }
}

} // namespace tenzir::plugins::clickhouse
