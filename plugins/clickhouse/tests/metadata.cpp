//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/insert_retry.hpp"
#include "clickhouse/table_description.hpp"

#include <tenzir/series_builder.hpp>
#include <tenzir/test/test.hpp>

#include <clickhouse/columns/string.h>
#include <clickhouse/error_codes.h>
#include <clickhouse/exceptions.h>

using namespace tenzir;
using namespace tenzir::plugins::clickhouse;

struct PendingGroup {
  std::vector<table_slice> slices;
  std::vector<table_slice> originals;
};

namespace {

auto description() -> std::vector<ColumnDescription> {
  return {{"n", "UInt32", "", "", ""},
          {"extra", "JSON", "", "", "tenzir:catch_all"}};
}

auto append(::clickhouse::Block& block, std::string name, std::string value)
  -> void {
  auto column = std::make_shared<::clickhouse::ColumnString>();
  column->Append(value);
  block.AppendColumn(name, column);
}

} // namespace

TEST("description reads named columns and distinguishes empty from missing "
     "metadata") {
  auto block = ::clickhouse::Block{};
  append(block, "comment", "tenzir:catch_all");
  append(block, "default_expression", "");
  append(block, "type", "JSON");
  append(block, "name", "extra");
  append(block, "default_type", "");
  auto dh = collecting_diagnostic_handler{};
  auto columns = read_description(block, dh);
  REQUIRE(columns.is_success());
  REQUIRE_EQUAL(columns->size(), 1u);
  CHECK_EQUAL((*columns)[0].comment, "tenzir:catch_all");
  CHECK((*columns)[0].default_kind.empty());
  REQUIRE(build_transformations(*columns, dh).is_success());
  auto missing = ::clickhouse::Block{};
  append(missing, "name", "extra");
  append(missing, "type", "JSON");
  CHECK(not read_description(missing, dh).is_success());
}

TEST("catch-all detection is independent of column ordering and defaults") {
  auto columns = description();
  auto dh = collecting_diagnostic_handler{};
  auto first = build_transformations(columns, dh);
  REQUIRE(first.is_success());
  CHECK_EQUAL(*first->catch_all, "extra");
  std::ranges::reverse(columns);
  auto second = build_transformations(columns, dh);
  REQUIRE(second.is_success());
  CHECK_EQUAL(*second->catch_all, "extra");
}

TEST("marked descriptions reject invalid markers and mappings") {
  for (auto const& invalid : std::vector<std::vector<ColumnDescription>>{
         {{"extra", "String", "", "", "tenzir:catch_all"}},
         {{"extra", "JSON", "DEFAULT", "'{}'", "tenzir:catch_all"}},
         {{"extra", "JSON", "MATERIALIZED", "'{}'", "tenzir:catch_all"}},
         {{"extra", "JSON", "ALIAS", "'{}'", "tenzir:catch_all"}},
         {{"extra", "JSON", "EPHEMERAL", "'{}'", "tenzir:catch_all"}},
         {{"a", "JSON", "", "", "tenzir:catch_all"},
          {"b", "JSON", "", "", "tenzir:catch_all"}},
         {{"extra", "JSON", "", "", "tenzir:catch_all"},
          {"a", "JSON", "", "", ""},
          {"a.b", "String", "", "", ""}},
         {{"extra", "JSON", "", "", "tenzir:catch_all"},
          {"a", "Tuple(n Int64)", "", "", ""}},
       }) {
    auto dh = collecting_diagnostic_handler{};
    CHECK(not build_transformations(invalid, dh).is_success());
  }
}

TEST("type annotation diagnostic explains the table-wide restriction") {
  auto columns = std::vector<ColumnDescription>{
    {"extra", "JSON", "", "", "tenzir:catch_all"},
    {"n", "Int64", "", "", "tenzir:type=duration"}};
  auto dh = collecting_diagnostic_handler{};
  CHECK(not build_transformations(columns, dh).is_success());
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), 1u);
  CHECK_EQUAL(diagnostics[0].message,
              "unsupported ClickHouse type annotation on column `n`");
  REQUIRE_EQUAL(diagnostics[0].notes.size(), 1u);
  CHECK_EQUAL(diagnostics[0].notes[0].message,
              "type annotations are not supported in tables with a "
              "catch-all column");
}

TEST("dotted catch-all names report the naming restriction") {
  auto columns = std::vector<ColumnDescription>{
    {"nested.extra", "JSON", "", "", "tenzir:catch_all"}};
  auto dh = collecting_diagnostic_handler{};
  CHECK(not build_transformations(columns, dh).is_success());
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), 1u);
  CHECK_EQUAL(diagnostics[0].message,
              "ClickHouse catch-all column name `nested.extra` must not "
              "contain dots");
}

TEST("unchanged metadata retains mutable transformers and advances refresh "
     "time") {
  auto columns = description();
  auto dh = collecting_diagnostic_handler{};
  auto tr = build_transformations(columns, dh);
  REQUIRE(tr.is_success());
  auto const original = tr->transfrom_and_index_for("n").trafo;
  original->has_default = true; // Mutable state must not enter comparisons.
  auto now = std::chrono::steady_clock::time_point{};
  auto cache = CachedDescription{columns, now};
  now += std::chrono::seconds{30};
  REQUIRE(refresh_transformations(*tr, cache, columns, now, dh).is_success());
  CHECK_EQUAL(tr->transfrom_and_index_for("n").trafo, original);
  CHECK_EQUAL(cache.refreshed, now);
  columns[0].comment = "changed comment";
  REQUIRE(refresh_transformations(*tr, cache, columns, now, dh).is_success());
  CHECK_NOT_EQUAL(tr->transfrom_and_index_for("n").trafo, original);
  CHECK(not tr->transfrom_and_index_for("n").trafo->has_default);
}

TEST("invalid refresh and marker removal leave the previous cache intact") {
  auto columns = description();
  auto dh = collecting_diagnostic_handler{};
  auto tr = build_transformations(columns, dh);
  REQUIRE(tr.is_success());
  auto now = std::chrono::steady_clock::time_point{};
  auto cache = CachedDescription{columns, now};
  auto const original = tr->transfrom_and_index_for("n").trafo;
  columns[1].comment.clear();
  CHECK(not refresh_transformations(*tr, cache, columns,
                                    now + std::chrono::seconds{30}, dh)
              .is_success());
  CHECK_EQUAL(tr->transfrom_and_index_for("n").trafo, original);
  CHECK_EQUAL(cache.refreshed, now);
  CHECK_EQUAL(cache.columns[1].comment, "tenzir:catch_all");
}

namespace {

auto rejected() -> ::clickhouse::ServerException {
  auto error = std::make_shared<::clickhouse::Exception>();
  error->code = ::clickhouse::NO_SUCH_COLUMN_IN_TABLE;
  error->display_text = "original rejection";
  return ::clickhouse::ServerException{std::move(error)};
}

auto pending_rows() -> std::vector<PendingGroup> {
  auto result = std::vector<PendingGroup>{};
  for (auto rows : {1, 2, 3}) {
    auto builder = series_builder{};
    for (auto i = 0; i < rows; ++i) {
      builder.record().field("n").data(int64_t{i});
    }
    auto slices = builder.finish_as_table_slice("test");
    result.push_back({slices, slices});
  }
  return result;
}

} // namespace

TEST("retry remaps only rejected and unattempted originals") {
  auto dh = collecting_diagnostic_handler{};
  auto writes = std::vector<size_t>{};
  auto attempts = 0;
  auto refreshes = 0;
  auto result = drain_pending(
    dh, pending_rows(),
    [&](PendingGroup& group) -> failure_or<void> {
      if (++attempts == 2) {
        throw rejected();
      }
      writes.push_back(group.originals.front().rows());
      return {};
    },
    [&]() -> failure_or<bool> {
      ++refreshes;
      return true;
    },
    [&](std::vector<table_slice> const& originals)
      -> failure_or<std::vector<PendingGroup>> {
      REQUIRE_EQUAL(originals.size(), 2u);
      CHECK_EQUAL(originals[0].rows(), 2u);
      CHECK_EQUAL(originals[1].rows(), 3u);
      auto groups = std::vector<PendingGroup>{};
      for (auto& original : originals) {
        groups.push_back({{original}, {original}});
      }
      return groups;
    });
  CHECK(result.is_success());
  CHECK_EQUAL(writes, (std::vector<size_t>{1, 2, 3}));
  CHECK_EQUAL(refreshes, 1);
}

TEST("unchanged metadata reports the original rejection without remapping") {
  auto dh = collecting_diagnostic_handler{};
  auto refreshes = 0;
  auto remaps = 0;
  auto caught = false;
  try {
    std::ignore = drain_pending(
      dh, pending_rows(),
      [](PendingGroup&) -> failure_or<void> {
        throw rejected();
      },
      [&]() -> failure_or<bool> {
        ++refreshes;
        return false;
      },
      [&](std::vector<table_slice> const&)
        -> failure_or<std::vector<PendingGroup>> {
        ++remaps;
        return pending_rows();
      });
  } catch (::clickhouse::ServerException const& error) {
    caught = true;
    CHECK_EQUAL(std::string{error.what()}, "original rejection");
  }
  CHECK(caught);
  CHECK_EQUAL(refreshes, 1);
  CHECK_EQUAL(remaps, 0);
}

TEST("schema retry is bounded and transport outcomes are never replayed") {
  auto dh = collecting_diagnostic_handler{};
  for (auto transport : {false, true}) {
    auto attempts = 0;
    auto refreshes = 0;
    auto caught = false;
    try {
      std::ignore = drain_pending(
        dh, pending_rows(),
        [&](PendingGroup&) -> failure_or<void> {
          ++attempts;
          if (transport) {
            throw std::runtime_error{"connection closed"};
          }
          throw rejected();
        },
        [&]() -> failure_or<bool> {
          ++refreshes;
          return true;
        },
        [](std::vector<table_slice> const&)
          -> failure_or<std::vector<PendingGroup>> {
          return pending_rows();
        });
    } catch (std::exception const&) {
      caught = true;
    }
    CHECK(caught);
    CHECK_EQUAL(attempts, transport ? 1 : 2);
    CHECK_EQUAL(refreshes, transport ? 0 : 1);
  }
}

TEST("preparation or column-building failures are not acknowledged") {
  auto dh = collecting_diagnostic_handler{};
  auto writes = 0;
  auto refreshes = 0;
  auto result = drain_pending(
    dh, pending_rows(),
    [&](PendingGroup&) -> failure_or<void> {
      ++writes;
      return failure::promise();
    },
    [&]() -> failure_or<bool> {
      ++refreshes;
      return true;
    },
    [](std::vector<table_slice> const&)
      -> failure_or<std::vector<PendingGroup>> {
      return pending_rows();
    });
  CHECK(not result.is_success());
  CHECK_EQUAL(writes, 1);
  CHECK_EQUAL(refreshes, 0);
}

TEST("refresh failures stop pending insertion") {
  auto dh = collecting_diagnostic_handler{};
  auto writes = 0;
  auto remaps = 0;
  auto result = drain_pending(
    dh, pending_rows(),
    [&](PendingGroup&) -> failure_or<void> {
      ++writes;
      throw rejected();
    },
    [&]() -> failure_or<bool> {
      diagnostic::error("invalid refreshed metadata").emit(dh);
      return failure::promise();
    },
    [&](std::vector<table_slice> const&)
      -> failure_or<std::vector<PendingGroup>> {
      ++remaps;
      return pending_rows();
    });
  CHECK(not result.is_success());
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), 2u);
  CHECK_EQUAL(diagnostics[0].message, "invalid refreshed metadata");
  CHECK_EQUAL(diagnostics[1].message, "insertion rejected: original rejection");
  CHECK_EQUAL(writes, 1);
  CHECK_EQUAL(remaps, 0);
}

TEST("maintenance and insertion share the refreshed metadata deadline") {
  auto now = std::chrono::steady_clock::time_point{};
  auto cached = CachedDescription{description(), now};
  CHECK(not refresh_due(cached, now + schema_refresh_interval
                                  - std::chrono::nanoseconds{1}));
  CHECK(refresh_due(cached, now + schema_refresh_interval));
  cached.refreshed = now + schema_refresh_interval;
  CHECK(not refresh_due(cached, now + schema_refresh_interval));
}

TEST("refresh exceptions retain the insertion rejection and never replay") {
  auto dh = collecting_diagnostic_handler{};
  auto writes = 0;
  auto remaps = 0;
  auto result = drain_pending(
    dh, pending_rows(),
    [&](PendingGroup&) -> failure_or<void> {
      ++writes;
      throw rejected();
    },
    []() -> failure_or<bool> {
      throw std::runtime_error{"refresh connection closed"};
    },
    [&](std::vector<table_slice> const&)
      -> failure_or<std::vector<PendingGroup>> {
      ++remaps;
      return pending_rows();
    });
  CHECK(not result.is_success());
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), 2u);
  CHECK_EQUAL(diagnostics[0].message,
              "metadata refresh failed: refresh connection closed");
  CHECK_EQUAL(diagnostics[1].message, "insertion rejected: original rejection");
  CHECK_EQUAL(writes, 1);
  CHECK_EQUAL(remaps, 0);
}

TEST("mapping validation examines nested type calls and empty path "
     "components") {
  for (auto type : {"Array(Tuple(n Int64))", "Map(String,Tuple(n Int64))",
                    "Array(JSON(max_dynamic_paths=0))"}) {
    auto columns = description();
    columns[0].type = type;
    columns[0].default_kind = "DEFAULT";
    columns[0].default_expression = "[]";
    auto dh = collecting_diagnostic_handler{};
    CHECK(not build_transformations(columns, dh).is_success());
  }
  for (auto name : {"a.", ".a", "a..b"}) {
    auto columns = description();
    columns[0].name = name;
    auto dh = collecting_diagnostic_handler{};
    CHECK(not build_transformations(columns, dh).is_success());
  }
}
