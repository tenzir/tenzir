//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/chunk.hpp"
#include "tenzir/data.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/test/nova.hpp"
#include "tenzir/test/test.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <folly/coro/BlockingWait.h>

#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;

namespace {

class CollectEvents final : public Push<nova::Events> {
public:
  auto operator()(nova::Events events) -> Task<void> override {
    auto values = nova::Array<nova::Data>{events.data};
    for (auto i = nova::storage::Index{0}; i < events.length(); ++i) {
      CHECK(events.mask.get(i));
      rows.push_back(nova::materialize_legacy(values.get(i)));
      names.emplace_back(*events.meta.name.get(i));
    }
    co_return;
  }

  std::vector<data> rows;
  std::vector<std::string> names;
};

auto make_reader() -> Box<Operator<chunk_ptr, nova::Events>> {
  auto const* plugin = plugins::find<OperatorPlugin>("read_zeek_tsv");
  REQUIRE(plugin);
  auto desc = plugin->describe();
  for (auto& spawn : desc.spawns) {
    if (auto* create = try_as<Spawn<chunk_ptr, nova::Events>>(spawn)) {
      return (*create)(desc.make_args());
    }
  }
  FAIL("event-based Zeek TSV reader is not registered");
}

} // namespace

TEST("Zeek TSV checkpoints preserve headers, partial lines, and log "
     "boundaries") {
  auto const input = std::string_view{"#separator \\x7c\r\n"
                                      "#set_separator|;\r\n"
                                      "#empty_field|EMPTY\r\n"
                                      "#unset_field|UNSET\r\n"
                                      "#path|first\r\n"
                                      "#fields|id.value|values\r\n"
                                      "#types|count|vector[int]\r\n"
                                      "1|2;3\r\n"
                                      "UNSET|EMPTY\r\n"
                                      "#close|2026-01-01-00-00-00\r\n"
                                      "#path\tsecond\r\n"
                                      "#fields\tflag\r\n"
                                      "#types\tbool\r\n"
                                      "T"};
  auto const expected = std::vector<data>{
    record{{"id", record{{"value", uint64_t{1}}}},
           {"values", list{int64_t{2}, int64_t{3}}}},
    record{{"id", record{{"value", caf::none}}}, {"values", list{}}},
    record{{"flag", true}},
  };
  auto const names
    = std::vector<std::string>{"zeek.first", "zeek.first", "zeek.second"};
  // Check every boundary, including inside headers, values, and CRLF pairs.
  for (auto split = size_t{0}; split <= input.size(); ++split) {
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto ctx = test::NovaOpCtx{dh, reg};
    auto output = CollectEvents{};
    auto reader = make_reader();
    folly::coro::blockingWait(reader->start(ctx));
    folly::coro::blockingWait(
      reader->process(chunk::copy(input.substr(0, split)), output, ctx));
    folly::coro::blockingWait(reader->prepare_snapshot(output, ctx));
    auto bytes = caf::byte_buffer{};
    auto serializer = caf::binary_serializer{bytes};
    auto saving = Serde{serializer};
    reader->snapshot(saving);
    reader = make_reader();
    auto deserializer = caf::binary_deserializer{bytes};
    auto loading = Serde{deserializer};
    reader->snapshot(loading);
    folly::coro::blockingWait(reader->start(ctx));
    folly::coro::blockingWait(
      reader->process(chunk::copy(input.substr(split)), output, ctx));
    CHECK_EQUAL(folly::coro::blockingWait(reader->finalize(output, ctx)),
                FinalizeBehavior::done);
    CHECK(dh.empty());
    CHECK_EQUAL(output.rows, expected);
    CHECK_EQUAL(output.names, names);
  }
}
