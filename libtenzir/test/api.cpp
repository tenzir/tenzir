//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/any.hpp"
#include "tenzir/async.hpp"
#include "tenzir/async/push_pull.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/box.hpp"
#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http_api.hpp"
#include "tenzir/location.hpp"
#include "tenzir/nova/array_base.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/test/nova.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/variant_traits.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/expected.hpp>
#include <caf/test/test.hpp>
#include <folly/coro/BlockingWait.h>

#include <cstddef>
#include <functional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace tenzir;

namespace {

class CollectEvents final : public Push<nova::Events> {
public:
  auto operator()(nova::Events events) -> Task<void> override {
    batches.push_back(std::move(events));
    co_return;
  }

  std::vector<nova::Events> batches;
};

auto make_api() -> Box<Operator<void, nova::Events>> {
  auto const* plugin = plugins::find<OperatorPlugin>("api");
  REQUIRE(plugin);
  auto desc = plugin->describe();
  auto args = desc.make_args();
  as<std::function<void(Any&, located<std::string>)>>(
    desc.positional.front().setter)(args, located{std::string{"/test"},
                                                  location{1, 4}});
  for (auto& spawn : desc.spawns) {
    if (auto* create = try_as<Spawn<void, nova::Events>>(spawn)) {
      return (*create)(std::move(args));
    }
  }
  FAIL("event-producing API operator is not registered");
}

auto deliver(Operator<void, nova::Events>& op, std::string const& body,
             CollectEvents& output, OpCtx& ctx) -> void {
  // Simulate a wire response without the local constructor's JSON
  // validation, so malformed payloads actually reach the operator decoder.
  auto bytes = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{bytes};
  auto code = size_t{200};
  auto headers = std::unordered_map<std::string, std::string>{};
  auto detail = caf::error{};
  REQUIRE(serializer.apply(code));
  REQUIRE(serializer.apply(body));
  REQUIRE(serializer.apply(headers));
  REQUIRE(serializer.apply(detail));
  auto response = rest_response{};
  auto deserializer = caf::binary_deserializer{bytes};
  REQUIRE(deserializer.apply(response));
  REQUIRE_EQUAL(response.body(), body);
  REQUIRE(not response.is_error());
  folly::coro::blockingWait(op.process_task(
    Any{caf::expected<rest_response>{std::move(response)}}, output, ctx));
}

} // namespace

TEST("API response decoding preserves field order types and inference") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto ctx = test::NovaOpCtx{dh, reg};
  auto output = CollectEvents{};
  auto op = make_api();
  auto body = std::string{R"({
    "b": 1,
    "a": "true",
    "a": "ignored",
    "ip": "192.0.2.1",
    "subnet": "192.0.2.0/24",
    "time": "2024-01-01T00:00:00Z",
    "duration": "1h",
    "uint": 18446744073709551615,
    "nested": {"text": "false", "number": "42"},
    "list": [true, "false", null, {"x": "2001:db8::1"}]
  })"};
  deliver(*op, body, output, ctx);
  REQUIRE_EQUAL(output.batches.size(), size_t{1});
  auto const& events = output.batches.front();
  REQUIRE_EQUAL(events.length(), 1);
  CHECK_EQUAL(events.active_count(), 1);
  CHECK_EQUAL(*events.meta.name.get(0), "tenzir.api");
  auto array = nova::Array<nova::Data>{events.data};
  auto expected = from_json(body);
  REQUIRE(expected);
  CHECK_EQUAL(nova::materialize_legacy(array.get(0)), *expected);
  CHECK_EQUAL(op->state(), OperatorState::done);
  CHECK(dh.empty());
}

TEST("API rejects malformed and non-record responses without partial events") {
  for (auto const* body :
       {"", "[]", "null", "true", R"("text")", "{} {}", "{} trailing",
        R"({"ok":1,"bad":[true,]})", R"({"ok":1,"bad":18446744073709551616})",
        R"({"x":1,"x":1e})"}) {
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto ctx = test::NovaOpCtx{dh, reg};
    auto output = CollectEvents{};
    auto op = make_api();
    deliver(*op, body, output, ctx);
    CHECK(output.batches.empty());
    CHECK_EQUAL(op->state(), OperatorState::done);
    auto diagnostics = std::move(dh).collect();
    REQUIRE(not diagnostics.empty());
    for (auto const& diag : diagnostics) {
      CHECK_EQUAL(diag.severity, severity::error);
      CHECK(diag.has_location());
    }
  }
}

TEST("API accepts an empty record and checkpoints completion") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto ctx = test::NovaOpCtx{dh, reg};
  auto output = CollectEvents{};
  auto op = make_api();
  deliver(*op, "{}", output, ctx);
  REQUIRE_EQUAL(output.batches.size(), size_t{1});
  CHECK_EQUAL(output.batches.front().length(), 1);
  auto bytes = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{bytes};
  auto saving = Serde{serializer};
  op->snapshot(saving);
  auto restored = make_api();
  auto deserializer = caf::binary_deserializer{bytes};
  auto loading = Serde{deserializer};
  restored->snapshot(loading);
  CHECK_EQUAL(restored->state(), OperatorState::done);
  CHECK(dh.empty());
}
