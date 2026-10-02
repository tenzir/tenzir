//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/connection.hpp"

#include "clickhouse/arguments.hpp"

#include <tenzir/test/test.hpp>

using namespace tenzir;
using namespace tenzir::plugins::clickhouse;

namespace {

auto apply(std::string_view uri) -> ConnectionArgs {
  auto dh = collecting_diagnostic_handler{};
  auto parsed = parse_connection_uri(uri, location::unknown, dh);
  REQUIRE(parsed);
  auto args = ConnectionArgs{};
  args.apply_uri(*parsed);
  return args;
}

} // namespace

TEST("a URI without user connects as the default user") {
  auto args = apply("clickhouse://host/db");
  CHECK_EQUAL(args.host, "host");
  CHECK_EQUAL(args.user, "default");
  CHECK_EQUAL(args.password, "");
  REQUIRE(args.default_database);
  CHECK_EQUAL(*args.default_database, "db");
}

TEST("a URI with user and password overrides the defaults") {
  auto args = apply("clickhouse://alice:secret@host:1234");
  CHECK_EQUAL(args.host, "host");
  CHECK_EQUAL(args.port, uint16_t{1234});
  CHECK_EQUAL(args.user, "alice");
  CHECK_EQUAL(args.password, "secret");
  CHECK(not args.default_database);
}
