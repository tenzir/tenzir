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
#include "tenzir/test/nova.hpp"
#include "tenzir/test/test.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <folly/coro/BlockingWait.h>

#include <algorithm>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace tenzir;

namespace {

class CollectEvents final : public Push<nova::Events> {
public:
  auto operator()(nova::Events events) -> Task<void> override {
    auto values = nova::Array<nova::Data>{events.data};
    for (auto row : nova::storage::true_bits(events.mask)) {
      messages.push_back(
        as<record>(nova::materialize_legacy(values.get(row))).at("message"));
    }
    co_return;
  }

  std::vector<data> messages;
};

auto make_reader(bool octet_counting)
  -> Box<Operator<chunk_ptr, nova::Events>> {
  auto const* plugin = plugins::find<OperatorPlugin>("read_syslog");
  REQUIRE(plugin);
  auto desc = plugin->describe();
  auto args = desc.make_args();
  for (auto const& named : desc.named) {
    if (std::ranges::contains(named.names, "octet_counting")) {
      as<std::function<void(Any&, located<bool>)>>(
        named.setter)(args, located{octet_counting, location::unknown});
    }
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
  FAIL("event-based Syslog reader is not registered");
}

auto frame(std::string_view message) -> std::string {
  return fmt::format("{} {}", message.size(), message);
}

auto messages() -> std::string {
  return frame("<165>1 2023-01-01T00:00:00Z host app 123 - - First")
         + frame("<165>1 2023-01-01T00:00:00Z host app 123 - - Second");
}

auto check_diagnostics(collecting_diagnostic_handler& dh,
                       std::string_view expected_error = {}) -> void {
  auto errors = size_t{0};
  for (auto const& diag : std::move(dh).collect()) {
    if (diag.severity == severity::warning) {
      // The unit context has no schema registry.
      CHECK_EQUAL(diag.message, "schema `syslog.rfc5424` does not exist");
    } else {
      CHECK_EQUAL(diag.severity, severity::error);
      CHECK_EQUAL(diag.message, expected_error);
      ++errors;
    }
  }
  CHECK_EQUAL(errors, expected_error.empty() ? size_t{0} : size_t{1});
}

} // namespace

TEST("Syslog publishes accepted frames before terminal framing diagnostics") {
  for (auto const& [tail, error] : {
         std::pair{"x", "failed to parse octet-counting length prefix"},
         std::pair{" ", "failed to parse octet-counting length prefix"},
         std::pair{"4294967296 ", "failed to parse octet-counting length "
                                  "prefix"},
         std::pair{"16777217 ", "octet-counted message length 16777217 exceeds "
                                "maximum 16777216"},
         std::pair{"12345678901", "octet-counting length prefix exceeds 10 "
                                  "bytes without delimiter"},
       }) {
    auto output = CollectEvents{};
    auto diagnostics = collecting_diagnostic_handler{};
    auto dh = transforming_diagnostic_handler{
      diagnostics, [&](diagnostic diag) {
        if (diag.severity == severity::error) {
          CHECK_EQUAL(output.messages, (std::vector<data>{"First", "Second"}));
        }
        return diag;
      }};
    auto reg = registry{};
    auto ctx = test::NovaOpCtx{dh, reg};
    auto reader = make_reader(true);
    folly::coro::blockingWait(reader->start(ctx));
    folly::coro::blockingWait(
      reader->process(chunk::copy(messages() + tail), output, ctx));
    CHECK_EQUAL(reader->state(), OperatorState::done);
    check_diagnostics(diagnostics, error);
    CHECK_EQUAL(output.messages, (std::vector<data>{"First", "Second"}));
    folly::coro::blockingWait(reader->process(
      chunk::copy(frame("<165>1 2023-01-01T00:00:00Z host app 123 - - Third")),
      output, ctx));
    folly::coro::blockingWait(reader->finalize(output, ctx));
    check_diagnostics(diagnostics);
    CHECK_EQUAL(output.messages, (std::vector<data>{"First", "Second"}));
  }
}

TEST("Syslog publishes accepted frames before incomplete-input diagnostics") {
  for (auto const& [tail, error] : {
         std::pair{"12", "unexpected end of input in octet-counting length "
                         "prefix"},
         std::pair{"50 x", "unexpected end of input in octet-counted syslog "
                           "message"},
       }) {
    auto output = CollectEvents{};
    auto diagnostics = collecting_diagnostic_handler{};
    auto dh = transforming_diagnostic_handler{
      diagnostics, [&](diagnostic diag) {
        if (diag.severity == severity::error) {
          CHECK_EQUAL(output.messages, (std::vector<data>{"First", "Second"}));
        }
        return diag;
      }};
    auto reg = registry{};
    auto ctx = test::NovaOpCtx{dh, reg};
    auto reader = make_reader(true);
    folly::coro::blockingWait(reader->start(ctx));
    folly::coro::blockingWait(
      reader->process(chunk::copy(messages() + tail), output, ctx));
    check_diagnostics(diagnostics);
    folly::coro::blockingWait(reader->finalize(output, ctx));
    check_diagnostics(diagnostics, error);
    CHECK_EQUAL(output.messages, (std::vector<data>{"First", "Second"}));
  }
}

TEST("Syslog checkpoints retain pending frames and multiline messages") {
  for (auto octet_counting : {false, true}) {
    auto first = std::string{
      "<165>1 2023-01-01T00:00:00Z host app 123 - - First\ncontinuation"};
    auto second
      = std::string{"<165>1 2023-01-01T00:00:00Z host app 123 - - Second"};
    auto input
      = octet_counting ? frame(first) + frame(second) : first + "\r\n" + second;
    for (auto split = size_t{0}; split <= input.size(); ++split) {
      auto dh = collecting_diagnostic_handler{};
      auto reg = registry{};
      auto ctx = test::NovaOpCtx{dh, reg};
      auto output = CollectEvents{};
      auto reader = make_reader(octet_counting);
      folly::coro::blockingWait(reader->start(ctx));
      folly::coro::blockingWait(reader->process(
        chunk::copy(std::string_view{input}.substr(0, split)), output, ctx));
      folly::coro::blockingWait(reader->prepare_snapshot(output, ctx));
      auto bytes = caf::byte_buffer{};
      auto serializer = caf::binary_serializer{bytes};
      auto saving = Serde{serializer};
      reader->snapshot(saving);
      reader = make_reader(octet_counting);
      auto deserializer = caf::binary_deserializer{bytes};
      auto loading = Serde{deserializer};
      reader->snapshot(loading);
      folly::coro::blockingWait(reader->start(ctx));
      folly::coro::blockingWait(reader->process(
        chunk::copy(std::string_view{input}.substr(split)), output, ctx));
      folly::coro::blockingWait(reader->finalize(output, ctx));
      check_diagnostics(dh);
      CHECK_EQUAL(output.messages,
                  (std::vector<data>{"First\ncontinuation", "Second"}));
    }
  }
}
