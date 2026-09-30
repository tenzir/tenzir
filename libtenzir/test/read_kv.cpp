//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/chunk.hpp"
#include "tenzir/data.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/test/nova.hpp"
#include "tenzir/test/test.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <folly/coro/BlockingWait.h>

#include <algorithm>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;
using namespace tenzir::si_literals;

namespace {

constexpr auto max_record_size = size_t{16_Mi};

class CollectEvents final : public Push<nova::Events> {
public:
  auto operator()(nova::Events events) -> Task<void> override {
    auto values = nova::Array<nova::Data>{events.data};
    for (auto row : nova::storage::true_bits(events.mask)) {
      auto materialized = nova::materialize_legacy(values.get(row));
      auto const& value = as<std::string>(as<record>(materialized).at("value"));
      // Keep failure output small even if an oversized record is emitted.
      prefixes.push_back(value.substr(0, 8));
      lengths.push_back(value.size());
    }
    co_return;
  }

  std::vector<std::string> prefixes;
  std::vector<size_t> lengths;
};

auto make_reader() -> Box<Operator<chunk_ptr, nova::Events>> {
  auto const* plugin = plugins::find<OperatorPlugin>("read_kv");
  REQUIRE(plugin);
  auto desc = plugin->describe();
  auto args = desc.make_args();
  for (auto const& named : desc.named) {
    if (std::ranges::contains(named.names, "_batch_timeout")) {
      as<std::function<void(Any&, located<duration>)>>(
        named.setter)(args, located{duration::max(), location::unknown});
    }
  }
  for (auto& spawn : desc.spawns) {
    if (auto* create = try_as<Spawn<chunk_ptr, nova::Events>>(spawn)) {
      return (*create)(std::move(args));
    }
  }
  FAIL("event-based KV reader is not registered");
}

auto save_checkpoint(Operator<chunk_ptr, nova::Events>& reader,
                     CollectEvents& output, OpCtx& ctx) -> caf::byte_buffer {
  folly::coro::blockingWait(reader.prepare_snapshot(output, ctx));
  auto bytes = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{bytes};
  auto saving = Serde{serializer};
  reader.snapshot(saving);
  return bytes;
}

auto restore_reader(caf::byte_buffer const& bytes, OpCtx& ctx)
  -> Box<Operator<chunk_ptr, nova::Events>> {
  auto reader = make_reader();
  auto deserializer = caf::binary_deserializer{bytes};
  auto loading = Serde{deserializer};
  reader->snapshot(loading);
  folly::coro::blockingWait(reader->start(ctx));
  return reader;
}

auto check_size_error(collecting_diagnostic_handler& dh, std::string_view line)
  -> void {
  auto diagnostics = std::move(dh).collect();
  CHECK_EQUAL(diagnostics.size(), size_t{1});
  for (auto const& diag : diagnostics) {
    CHECK_EQUAL(diag.severity, severity::error);
    CHECK_EQUAL(diag.message,
                "key-value record exceeds maximum 16777216 bytes");
    CHECK_EQUAL(diag.notes.size(), size_t{1});
    for (auto const& note : diag.notes) {
      CHECK_EQUAL(note.message, line);
    }
  }
}

} // namespace

TEST("KV rejects oversized records before end-of-input") {
  auto accepted = std::string{"value=First\nvalue=Second\n"};
  auto oversized = "value=" + std::string(max_record_size - 5, 'x');
  for (auto delimiter : {"", "\n", "\r\n"}) {
    auto input = accepted + oversized + delimiter;
    for (auto split : {size_t{0}, accepted.size() + max_record_size}) {
      auto output = CollectEvents{};
      auto diagnostics = collecting_diagnostic_handler{};
      auto dh = transforming_diagnostic_handler{
        diagnostics, [&](diagnostic diag) {
          if (diag.severity == severity::error) {
            CHECK_EQUAL(output.prefixes,
                        (std::vector<std::string>{"First", "Second"}));
          }
          return diag;
        }};
      auto reg = registry{};
      auto ctx = test::NovaOpCtx{dh, reg};
      auto reader = make_reader();
      folly::coro::blockingWait(reader->start(ctx));
      folly::coro::blockingWait(reader->process(
        chunk::copy(std::string_view{input}.substr(0, split)), output, ctx));
      CHECK_EQUAL(reader->state(), OperatorState::normal);
      CHECK(diagnostics.empty());
      folly::coro::blockingWait(reader->process(
        chunk::copy(std::string_view{input}.substr(split)), output, ctx));
      CHECK_EQUAL(reader->state(), OperatorState::done);
      check_size_error(diagnostics, "line 3");
      CHECK_EQUAL(output.prefixes,
                  (std::vector<std::string>{"First", "Second"}));
      folly::coro::blockingWait(
        reader->process(chunk::copy("value=Third\n"), output, ctx));
      folly::coro::blockingWait(reader->finalize(output, ctx));
      CHECK(diagnostics.empty());
      CHECK_EQUAL(output.prefixes,
                  (std::vector<std::string>{"First", "Second"}));
      auto bytes = save_checkpoint(*reader, output, ctx);
      CHECK(bytes.size() < 1_Ki);
      reader = restore_reader(bytes, ctx);
      CHECK_EQUAL(reader->state(), OperatorState::done);
      folly::coro::blockingWait(
        reader->process(chunk::copy("value=Third\n"), output, ctx));
      folly::coro::blockingWait(reader->finalize(output, ctx));
      CHECK(diagnostics.empty());
      CHECK_EQUAL(output.prefixes,
                  (std::vector<std::string>{"First", "Second"}));
    }
  }
}

TEST("KV accepts records at the size limit") {
  auto accepted = std::string{"value=First\n"};
  auto record = "value=" + std::string(max_record_size - 6, 'x');
  for (auto delimiter : {"", "\n", "\r\n"}) {
    auto input = accepted + record + delimiter;
    for (auto split : {size_t{0}, accepted.size() + max_record_size - 1}) {
      auto dh = collecting_diagnostic_handler{};
      auto reg = registry{};
      auto ctx = test::NovaOpCtx{dh, reg};
      auto output = CollectEvents{};
      auto reader = make_reader();
      folly::coro::blockingWait(reader->start(ctx));
      folly::coro::blockingWait(reader->process(
        chunk::copy(std::string_view{input}.substr(0, split)), output, ctx));
      folly::coro::blockingWait(reader->process(
        chunk::copy(std::string_view{input}.substr(split)), output, ctx));
      CHECK_EQUAL(folly::coro::blockingWait(reader->finalize(output, ctx)),
                  FinalizeBehavior::done);
      CHECK(dh.empty());
      CHECK_EQUAL(output.prefixes,
                  (std::vector<std::string>{"First", "xxxxxxxx"}));
      CHECK_EQUAL(output.lengths,
                  (std::vector<size_t>{5, max_record_size - 6}));
    }
  }
}

TEST("KV retains its record limit across checkpoints") {
  auto record = "value=" + std::string(max_record_size - 6, 'x');
  for (auto delimiter : {"", "\n"}) {
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto ctx = test::NovaOpCtx{dh, reg};
    auto output = CollectEvents{};
    auto reader = make_reader();
    folly::coro::blockingWait(reader->start(ctx));
    folly::coro::blockingWait(
      reader->process(chunk::copy(record), output, ctx));
    auto bytes = save_checkpoint(*reader, output, ctx);
    CHECK(bytes.size() <= max_record_size + 1_Ki);
    CHECK(output.prefixes.empty());
    CHECK(dh.empty());
    reader = restore_reader(bytes, ctx);
    folly::coro::blockingWait(
      reader->process(chunk::copy(std::string{"x"} + delimiter), output, ctx));
    CHECK_EQUAL(reader->state(), OperatorState::done);
    check_size_error(dh, "line 1");
    folly::coro::blockingWait(reader->finalize(output, ctx));
    CHECK(output.prefixes.empty());
    CHECK(dh.empty());
    bytes = save_checkpoint(*reader, output, ctx);
    CHECK(bytes.size() < 1_Ki);
  }
}
