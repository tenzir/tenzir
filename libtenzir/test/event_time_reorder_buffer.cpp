//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/event_time_reorder_buffer.hpp"

#include "tenzir/async.hpp"
#include "tenzir/test/test.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

#include <chrono>
#include <string>

namespace tenzir {

namespace {

using namespace std::chrono_literals;
using Buffer = detail::EventTimeReorderBuffer<std::string>;

TEST("drains through the watermark and preserves stable ties") {
  auto buffer = Buffer{3s};
  CHECK_EQUAL(buffer.insert(time{2s}, "later"), Buffer::InsertResult::accepted);
  CHECK(buffer.drain().empty());
  CHECK_EQUAL(buffer.insert(time{1s}, "first"), Buffer::InsertResult::accepted);
  CHECK_EQUAL(buffer.insert(time{1s}, "second"),
              Buffer::InsertResult::accepted);
  CHECK_EQUAL(buffer.insert(time{4s}, "watermark"),
              Buffer::InsertResult::accepted);
  auto ready = buffer.drain();
  REQUIRE_EQUAL(ready.size(), size_t{2});
  CHECK_EQUAL(ready[0].payload, "first");
  CHECK_EQUAL(ready[1].payload, "second");
  CHECK_EQUAL(buffer.last_emitted_timestamp(), Option{time{1s}});
  CHECK_EQUAL(buffer.insert(time{0s}, "late"), Buffer::InsertResult::late);
  CHECK_EQUAL(buffer.insert(time{1s}, "equal"), Buffer::InsertResult::accepted);
  ready = buffer.drain();
  REQUIRE_EQUAL(ready.size(), size_t{1});
  CHECK_EQUAL(ready[0].payload, "equal");
  ready = buffer.flush();
  REQUIRE_EQUAL(ready.size(), size_t{2});
  CHECK_EQUAL(ready[0].payload, "later");
  CHECK_EQUAL(ready[1].payload, "watermark");
}

TEST("accepts timestamps behind the watermark before later output") {
  auto buffer = Buffer{5s};
  CHECK_EQUAL(buffer.insert(time{10s}, "future"),
              Buffer::InsertResult::accepted);
  CHECK_EQUAL(buffer.watermark(), Option{time{5s}});
  CHECK(buffer.drain().empty());
  CHECK_EQUAL(buffer.insert(time{4s}, "earlier"),
              Buffer::InsertResult::accepted);
  auto ready = buffer.drain();
  REQUIRE_EQUAL(ready.size(), size_t{1});
  CHECK_EQUAL(ready[0].payload, "earlier");
  ready = buffer.flush();
  REQUIRE_EQUAL(ready.size(), size_t{1});
  CHECK_EQUAL(ready[0].payload, "future");
}

TEST("saturates the watermark at the timestamp minimum") {
  auto buffer = Buffer{10s};
  CHECK_EQUAL(buffer.insert(time::min(), "minimum"),
              Buffer::InsertResult::accepted);
  CHECK_EQUAL(buffer.watermark(), Option{time::min()});
  auto ready = buffer.drain();
  REQUIRE_EQUAL(ready.size(), size_t{1});
  CHECK_EQUAL(ready[0].payload, "minimum");
}

TEST("snapshot preserves payloads and tie sequence") {
  auto buffer = Buffer{10s};
  CHECK_EQUAL(buffer.insert(time{1s}, "first"), Buffer::InsertResult::accepted);
  CHECK_EQUAL(buffer.insert(time{1s}, "second"),
              Buffer::InsertResult::accepted);
  auto bytes = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{bytes};
  REQUIRE(serializer.begin_object(caf::invalid_type_id, ""));
  auto saving = Serde{serializer};
  buffer.snapshot(saving);
  REQUIRE(serializer.end_object());
  auto restored = Buffer{10s};
  auto deserializer = caf::binary_deserializer{bytes};
  REQUIRE(deserializer.begin_object(caf::invalid_type_id, ""));
  auto loading = Serde{deserializer};
  restored.snapshot(loading);
  REQUIRE(deserializer.end_object());
  CHECK_EQUAL(restored.insert(time{1s}, "third"),
              Buffer::InsertResult::accepted);
  CHECK_EQUAL(restored.insert(time{11s}, "advance"),
              Buffer::InsertResult::accepted);
  auto ready = restored.drain();
  REQUIRE_EQUAL(ready.size(), size_t{3});
  CHECK_EQUAL(ready[0].payload, "first");
  CHECK_EQUAL(ready[1].payload, "second");
  CHECK_EQUAL(ready[2].payload, "third");
}

} // namespace

} // namespace tenzir
