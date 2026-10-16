//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/test/test.hpp>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <clickhouse/query_diagnostics.hpp>
#include <fmt/format.h>

#include <string_view>

using namespace tenzir;

namespace {

/// Checks that every instance of `name` that the spawn `Output` creates
/// rejects saving and restoring a checkpoint.
template <class Output>
auto check_rejects_checkpoints(std::string_view name) -> void {
  auto const* plugin = plugins::find<OperatorPlugin>(name);
  REQUIRE(plugin);
  auto desc = plugin->describe();
  REQUIRE(desc.set_operator_location);
  auto source = location{17, 29};
  for (auto& spawn : desc.spawns) {
    auto* create = try_as<Spawn<void, Output>>(spawn);
    if (not create) {
      continue;
    }
    for (auto loading : {false, true}) {
      auto args = desc.make_args();
      (*desc.set_operator_location)(args, source);
      auto op = (*create)(std::move(args));
      auto bytes = caf::byte_buffer{};
      auto serializer = caf::binary_serializer{bytes};
      auto deserializer = caf::binary_deserializer{bytes};
      auto serde = loading ? Serde{deserializer} : Serde{serializer};
      auto rejected = false;
      try {
        op->snapshot(serde);
      } catch (diagnostic const& error) {
        rejected = true;
        CHECK_EQUAL(error.severity, severity::error);
        CHECK_EQUAL(error.message,
                    fmt::format("{} does not support checkpoints yet", name));
        REQUIRE_EQUAL(error.annotations.size(), size_t{1});
        CHECK(error.annotations.front().primary);
        CHECK_EQUAL(error.annotations.front().source, source);
      }
      CHECK(rejected);
      CHECK(bytes.empty());
    }
    return;
  }
  FAIL("{} has no spawn for this output", name);
}

} // namespace

TEST("from_clickhouse rejects saving and restoring checkpoints") {
  check_rejects_checkpoints<nova::Events>("from_clickhouse");
  check_rejects_checkpoints<table_slice>("from_clickhouse");
}

TEST("from_hydrolix rejects saving and restoring checkpoints") {
  check_rejects_checkpoints<nova::Events>("from_hydrolix");
  check_rejects_checkpoints<table_slice>("from_hydrolix");
}

TEST("from_hydrolix authentication diagnostics omit ClickHouse advice") {
  auto const first = std::string{
    "DB::Exception: secret-token: Authentication failed: password is "
    "incorrect, or there is no user with such name."};
  auto const message
    = first
      + "\n\nIf you use ClickHouse Cloud, the password can be reset at "
        "https://clickhouse.cloud/\non the settings page for the corresponding "
        "service.\n\nIf you have installed ClickHouse and forgot password you "
        "can reset it in the configuration file.\nThe password for default "
        "user is typically located at "
        "/etc/clickhouse-server/users.d/default-password.xml\nand deleting "
        "this file will reset the password.\nSee also "
        "/etc/clickhouse-server/users.xml on the server where ClickHouse "
        "is installed.\n\n";
  for (auto const& input : {first, message}) {
    auto diag
      = plugins::clickhouse::query_error(true, "secret-token", true, 516, input)
          .done();
    CHECK_EQUAL(diag.message,
                "Hydrolix error 516: DB::Exception: ***: Authentication "
                "failed: "
                "password is incorrect, or there is no user with such name.");
    REQUIRE_EQUAL(diag.notes.size(), size_t{1});
    CHECK_EQUAL(diag.notes[0].message,
                "check that `token` is a valid Hydrolix token that has not "
                "expired");
  }
  auto unchanged = plugins::clickhouse::query_error(false, "secret-token", true,
                                                    516, message)
                     .done();
  CHECK_EQUAL(unchanged.message, "ClickHouse error 516: " + message);
  auto other
    = plugins::clickhouse::query_error(true, "", true, 497, message).done();
  CHECK_EQUAL(other.message, "Hydrolix error 497: " + message);
}

TEST("from_hydrolix privilege diagnostics distinguish schema access") {
  for (auto grant : {"SHOW COLUMNS", "SELECT"}) {
    auto message = fmt::format(
      "DB::Exception: Not enough privileges. To execute this query, "
      "it's necessary to have the grant {} ON db.table",
      grant);
    auto diag
      = plugins::clickhouse::query_error(true, "", true, 497, message).done();
    CHECK_EQUAL(diag.message, "Hydrolix error 497: " + message);
    if (std::string_view{grant} == "SHOW COLUMNS") {
      REQUIRE_EQUAL(diag.notes.size(), size_t{1});
      CHECK_EQUAL(diag.notes[0].message,
                  "check that the account of `token` has the "
                  "`show_columns_sql` "
                  "permission on the table for schema access (also granted by "
                  "`select_sql`)");
    } else {
      REQUIRE_EQUAL(diag.notes.size(), size_t{2});
      CHECK_EQUAL(diag.notes[0].message,
                  "check that the table exists and has a transform");
      CHECK_EQUAL(diag.notes[1].message,
                  "check that the account of `token` has the `select_sql` "
                  "permission on the table");
    }
    auto plain
      = plugins::clickhouse::query_error(false, "", true, 497, message).done();
    CHECK_EQUAL(plain.message, "ClickHouse error 497: " + message);
    REQUIRE_EQUAL(plain.notes.size(), size_t{3});
    CHECK_EQUAL(plain.notes[0].message, "`tls` is enabled for this connection");
    CHECK_EQUAL(plain.notes[1].message,
                "common ClickHouse ports are `9000` without TLS and `9440` "
                "with TLS");
    CHECK_EQUAL(plain.notes[2].message,
                "if the ClickHouse server expects a plaintext connection, "
                "set `tls=false`");
  }
}
