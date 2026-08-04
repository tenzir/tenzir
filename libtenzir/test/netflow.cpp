//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/netflow.hpp"

#include "tenzir/async.hpp"
#include "tenzir/blob.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/test/test.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::netflow {

namespace {

using bytes = std::vector<std::byte>;

struct Set {
  uint16_t id;
  bytes payload;
};

struct FieldSpecifier {
  uint16_t id;
  uint16_t length;
  uint32_t enterprise_number = 0;
};

auto append_u8(bytes& output, uint8_t value) -> void {
  output.push_back(static_cast<std::byte>(value));
}

auto append_u16(bytes& output, uint16_t value) -> void {
  append_u8(output, static_cast<uint8_t>(value >> 8));
  append_u8(output, static_cast<uint8_t>(value));
}

auto append_u32(bytes& output, uint32_t value) -> void {
  append_u16(output, static_cast<uint16_t>(value >> 16));
  append_u16(output, static_cast<uint16_t>(value));
}

auto patch_u16(bytes& output, size_t offset, uint16_t value) -> void {
  output[offset] = static_cast<std::byte>(value >> 8);
  output[offset + 1] = static_cast<std::byte>(value);
}

auto raw(std::initializer_list<uint8_t> values) -> bytes {
  auto output = bytes{};
  output.reserve(values.size());
  for (auto value : values) {
    append_u8(output, value);
  }
  return output;
}

auto append_set(bytes& output, Set const& set) -> void {
  append_u16(output, set.id);
  append_u16(output, static_cast<uint16_t>(set.payload.size() + 4));
  output.insert(output.end(), set.payload.begin(), set.payload.end());
}

auto template_record(uint16_t id, std::initializer_list<FieldSpecifier> fields)
  -> bytes {
  auto output = bytes{};
  append_u16(output, id);
  append_u16(output, static_cast<uint16_t>(fields.size()));
  for (auto field : fields) {
    auto raw_id = field.id;
    if (field.enterprise_number != 0) {
      raw_id |= uint16_t{0x8000};
    }
    append_u16(output, raw_id);
    append_u16(output, field.length);
    if (field.enterprise_number != 0) {
      append_u32(output, field.enterprise_number);
    }
  }
  return output;
}

auto ipfix(uint32_t domain_id, std::initializer_list<Set> sets,
           uint32_t sequence_number = 1,
           uint32_t export_time_seconds = 1'700'000'000) -> bytes {
  auto output = bytes{};
  append_u16(output, 10);
  append_u16(output, 0);
  append_u32(output, export_time_seconds);
  append_u32(output, sequence_number);
  append_u32(output, domain_id);
  for (auto const& set : sets) {
    append_set(output, set);
  }
  patch_u16(output, 2, static_cast<uint16_t>(output.size()));
  return output;
}

auto netflow_v9(uint32_t uptime, uint32_t domain_id,
                std::initializer_list<Set> sets, uint32_t sequence_number = 1,
                Option<uint16_t> record_count = None{},
                uint32_t export_time_seconds = 1'700'000'000) -> bytes {
  auto output = bytes{};
  append_u16(output, 9);
  append_u16(output,
             record_count ? *record_count : static_cast<uint16_t>(sets.size()));
  append_u32(output, uptime);
  append_u32(output, export_time_seconds);
  append_u32(output, sequence_number);
  append_u32(output, domain_id);
  for (auto const& set : sets) {
    append_set(output, set);
  }
  return output;
}

auto netflow_v5(bool with_record = true, uint32_t uptime = 10'000,
                uint32_t first = 9'000, uint32_t last = 9'500,
                uint32_t nanoseconds = 0) -> bytes {
  auto output = bytes{};
  append_u16(output, 5);
  append_u16(output, with_record ? 1 : 0);
  append_u32(output, uptime);
  append_u32(output, 1'000);
  append_u32(output, nanoseconds);
  append_u32(output, 42);
  append_u8(output, 1);
  append_u8(output, 2);
  append_u16(output, uint16_t{0x4005});
  if (not with_record) {
    return output;
  }
  for (auto value : {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}) {
    append_u8(output, static_cast<uint8_t>(value));
  }
  append_u16(output, 13);
  append_u16(output, 14);
  append_u32(output, 15);
  append_u32(output, 16);
  append_u32(output, first);
  append_u32(output, last);
  append_u16(output, 17);
  append_u16(output, 18);
  append_u8(output, 0);
  append_u8(output, 0x12);
  append_u8(output, 17);
  append_u8(output, 20);
  append_u16(output, 21);
  append_u16(output, 22);
  append_u8(output, 23);
  append_u8(output, 24);
  append_u16(output, 0);
  return output;
}

auto value(DecodedRecord const& record, std::string_view name) -> data const& {
  auto const position
    = std::ranges::find(record.fields, name, &DecodedField::name);
  REQUIRE(position != record.fields.end());
  return position->value;
}

auto has_field(DecodedRecord const& record, std::string_view name) -> bool {
  return std::ranges::find(record.fields, name, &DecodedField::name)
         != record.fields.end();
}

auto decode(Decoder& decoder, bytes const& message,
            collecting_diagnostic_handler& dh, Option<Peer> peer = None{})
  -> DecodeResult {
  return decoder.decode_message(message, std::move(peer), dh);
}

} // namespace

TEST("frames arbitrary chunks and consecutive messages") {
  auto decoder = Decoder{};
  auto v5 = netflow_v5();
  auto v10 = ipfix(7, {});
  auto combined = v5;
  combined.insert(combined.end(), v10.begin(), v10.end());
  auto partial = decoder.frame(std::span{combined}.first(23), false);
  CHECK_EQUAL(partial.status, FrameStatus::incomplete);
  auto first = decoder.frame(combined, false);
  REQUIRE_EQUAL(first.status, FrameStatus::ready);
  CHECK_EQUAL(first.size, v5.size());
  auto second = decoder.frame(std::span{combined}.subspan(first.size), false);
  REQUIRE_EQUAL(second.status, FrameStatus::ready);
  CHECK_EQUAL(second.size, v10.size());
  auto v9 = netflow_v9(100, 8, {{256, raw({1, 2, 3, 4})}});
  auto v9_and_v5 = v9;
  v9_and_v5.insert(v9_and_v5.end(), v5.begin(), v5.end());
  auto framed_v9 = decoder.frame(v9_and_v5, false);
  REQUIRE_EQUAL(framed_v9.status, FrameStatus::ready);
  CHECK_EQUAL(framed_v9.size, v9.size());
}

TEST("frames NetFlow v9 at an exact stream boundary") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definition = template_record(256, {{8, 4}});
  REQUIRE(not decode(decoder, netflow_v9(100, 8, {{0, definition}}), dh).error);
  auto message = netflow_v9(101, 8, {{256, raw({10, 0, 0, 1, 10, 0, 0, 2})}}, 2,
                            uint16_t{2});
  auto framed = decoder.frame(message, false);
  REQUIRE_EQUAL(framed.status, FrameStatus::ready);
  CHECK_EQUAL(framed.size, message.size());
}

TEST("tracks ambiguous frame deadlines") {
  using clock = IdleFrameTimer::clock;
  using namespace std::chrono_literals;
  auto const start = clock::time_point{};
  auto timer = IdleFrameTimer{1s};
  timer.observe(40, start);
  timer.on_input(0);
  REQUIRE(timer.wait_for(start + 400ms));
  CHECK_EQUAL(*timer.wait_for(start + 400ms), 600ms);
  timer.on_input(1);
  CHECK(timer.wait_for(start + 400ms).is_none());

  timer.observe(40, start + 400ms);
  CHECK(timer.take_expired(start + 1s).is_none());
  auto expired = timer.take_expired(start + 1400ms);
  REQUIRE(expired);
  CHECK_EQUAL(*expired, size_t{40});

  timer.observe(40, start + 2s);
  timer.on_input(2);
  timer.observe(40, start + 2500ms);
  CHECK(timer.take_expired(start + 3s).is_none());
  expired = timer.take_expired(start + 3500ms);
  REQUIRE(expired);
  CHECK_EQUAL(*expired, size_t{40});

  // Restored ambiguity starts a fresh grace period on the new steady clock.
  auto restored = IdleFrameTimer{1s};
  restored.observe(40, start + 10s);
  REQUIRE(restored.wait_for(start + 10s));
  CHECK_EQUAL(*restored.wait_for(start + 10s), 1s);

  auto builder = series_builder::YieldReadyResult{};
  builder.wait_for = 2s;
  auto framing = series_builder::YieldReadyResult{};
  framing.wait_for = 1s;
  builder.merge(std::move(framing));
  REQUIRE(builder.wait_for);
  CHECK_EQUAL(*builder.wait_for, 1s);
}

TEST("delays ambiguous NetFlow v9 stream boundaries") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definition = template_record(256, {{7, 2}});
  REQUIRE(not decode(decoder, netflow_v9(100, 8, {{0, definition}}), dh).error);
  auto message
    = netflow_v9(101, 8, {{256, raw({0, 53, 0, 0})}}, 2, uint16_t{2});
  auto framed = decoder.frame(message, false);
  CHECK_EQUAL(framed.status, FrameStatus::ambiguous);
  append_set(message, Set{256, raw({1, 187, 0, 0})});
  framed = decoder.frame(message, false);
  REQUIRE_EQUAL(framed.status, FrameStatus::ready);
  CHECK_EQUAL(framed.size, message.size());
  auto result = decode(decoder, message, dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{2});
  CHECK_EQUAL(value(result.records.front(), "source_transport_port"),
              uint64_t{53});
  CHECK_EQUAL(value(result.records.back(), "source_transport_port"),
              uint64_t{443});
  auto complete
    = netflow_v9(102, 8, {{256, raw({0, 53, 1, 187})}}, 3, uint16_t{2});
  framed = decoder.frame(complete, false);
  CHECK_EQUAL(framed.status, FrameStatus::ambiguous);
  CHECK_EQUAL(framed.size, complete.size());
  framed = decoder.frame(complete, false);
  CHECK_EQUAL(framed.status, FrameStatus::ambiguous);
  CHECK_EQUAL(framed.size, complete.size());
  framed = decoder.frame(complete, true);
  REQUIRE_EQUAL(framed.status, FrameStatus::ready);
  CHECK_EQUAL(framed.size, complete.size());
  framed = decoder.frame(complete, false);
  CHECK_EQUAL(framed.status, FrameStatus::ambiguous);
  CHECK_EQUAL(framed.size, complete.size());
  auto following = netflow_v5();
  for (auto suffix_size = size_t{1}; suffix_size < 4; ++suffix_size) {
    auto partial = complete;
    partial.insert(partial.end(), following.begin(),
                   following.begin() + suffix_size);
    framed = decoder.frame(partial, false);
    CHECK_EQUAL(framed.status, FrameStatus::ambiguous);
    CHECK_EQUAL(framed.size, complete.size());
    framed = decoder.frame(std::span{partial}.first(complete.size()), true);
    REQUIRE_EQUAL(framed.status, FrameStatus::ready);
    CHECK_EQUAL(framed.size, complete.size());
  }
  auto stream = complete;
  stream.insert(stream.end(), following.begin(), following.end());
  framed = decoder.frame(stream, false);
  REQUIRE_EQUAL(framed.status, FrameStatus::ready);
  CHECK_EQUAL(framed.size, complete.size());
  result = decode(decoder, complete, dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{2});
  CHECK_EQUAL(value(result.records.front(), "source_transport_port"),
              uint64_t{53});
  CHECK_EQUAL(value(result.records.back(), "source_transport_port"),
              uint64_t{443});
  framed = decoder.frame(std::span{stream}.last(following.size()), false);
  REQUIRE_EQUAL(framed.status, FrameStatus::ready);
  CHECK_EQUAL(framed.size, following.size());

  auto unknown
    = netflow_v9(103, 8, {{257, raw({10, 0, 0, 1})}}, 4, uint16_t{1});
  for (auto suffix_size = size_t{0}; suffix_size < 4; ++suffix_size) {
    auto partial = unknown;
    partial.insert(partial.end(), following.begin(),
                   following.begin() + suffix_size);
    framed = decoder.frame(partial, false);
    CHECK_EQUAL(framed.status, FrameStatus::ambiguous);
    CHECK_EQUAL(framed.size, unknown.size());
    framed = decoder.frame(unknown, true);
    REQUIRE_EQUAL(framed.status, FrameStatus::ready);
    CHECK_EQUAL(framed.size, unknown.size());
  }
  auto unknown_stream = unknown;
  unknown_stream.insert(unknown_stream.end(), following.begin(),
                        following.end());
  framed = decoder.frame(unknown_stream, false);
  REQUIRE_EQUAL(framed.status, FrameStatus::ready);
  CHECK_EQUAL(framed.size, unknown.size());
  result = decode(decoder, unknown, dh);
  REQUIRE(not result.error);
  CHECK(result.records.empty());
  auto unknown_definition = template_record(257, {{8, 4}});
  result
    = decode(decoder, netflow_v9(104, 8, {{0, unknown_definition}}, 5), dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  CHECK_EQUAL(value(result.records.front(), "source_ipv4_address"),
              ip::v4(0x0a000001));
  CHECK(dh.empty());
}

TEST("does not frame reordered NetFlow v9 data with a current template") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto old_definition = template_record(256, {{7, 2}});
  REQUIRE(not decode(decoder,
                     netflow_v9(100, 8, {{0, old_definition}}, 100, None{},
                                1'700'000'000),
                     dh)
                .error);
  auto current_definition = template_record(256, {{8, 8}});
  REQUIRE(not decode(decoder,
                     netflow_v9(300, 8, {{0, current_definition}}, 102, None{},
                                1'700'000'002),
                     dh)
                .error);
  auto delayed = netflow_v9(200, 8, {{256, raw({0, 53, 0, 0})}}, 101,
                            uint16_t{1}, 1'700'000'001);
  auto following = netflow_v5();
  auto stream = delayed;
  stream.insert(stream.end(), following.begin(), following.end());
  auto framed = decoder.frame(stream, false);
  REQUIRE_EQUAL(framed.status, FrameStatus::ready);
  CHECK_EQUAL(framed.size, delayed.size());
  auto discarded = decode(decoder, delayed, dh);
  REQUIRE(not discarded.error);
  CHECK(discarded.records.empty());
  auto current
    = decode(decoder,
             netflow_v9(400, 8, {{256, raw({10, 0, 0, 1, 10, 0, 0, 2})}}, 103,
                        None{}, 1'700'000'003),
             dh);
  REQUIRE(not current.error);
  REQUIRE_EQUAL(current.records.size(), size_t{1});
  CHECK(has_field(current.records.front(), "source_ipv4_address"));
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{2});
  CHECK_EQUAL(diagnostics.front().message, "redefined NetFlow template");
  CHECK_EQUAL(diagnostics.back().message,
              "discarded reordered NetFlow v9 data set");
}

TEST("uses NetFlow v9 record counts to strip alignment padding") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definition = template_record(256, {{7, 2}});
  REQUIRE(not decode(decoder, netflow_v9(100, 8, {{0, definition}}), dh).error);
  auto message
    = netflow_v9(101, 8, {{256, raw({0, 53, 0, 0})}}, 2, uint16_t{1});
  auto framed = decoder.frame(message, false);
  REQUIRE_EQUAL(framed.status, FrameStatus::ready);
  CHECK_EQUAL(framed.size, message.size());
  auto result = decode(decoder, message, dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  CHECK_EQUAL(value(result.records.front(), "source_transport_port"),
              uint64_t{53});

  auto two_sets
    = netflow_v9(102, 8,
                 {{256, raw({0, 53, 0, 0})}, {256, raw({1, 187, 0, 0})}}, 3,
                 uint16_t{2});
  framed = decoder.frame(two_sets, false);
  REQUIRE_EQUAL(framed.status, FrameStatus::ready);
  CHECK_EQUAL(framed.size, two_sets.size());
  result = decode(decoder, two_sets, dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{2});
  CHECK_EQUAL(value(result.records.back(), "source_transport_port"),
              uint64_t{443});

  auto ambiguous
    = netflow_v9(103, 8,
                 {{256, raw({0, 53, 0, 0})}, {256, raw({1, 187, 0, 0})}}, 4,
                 uint16_t{3});
  auto rejected = decode(decoder, ambiguous, dh);
  REQUIRE(rejected.error);
  CHECK_EQUAL(rejected.error->message,
              "NetFlow v9 record count has an ambiguous data set assignment");

  auto buffered_decoder = Decoder{};
  auto buffered = decode(buffered_decoder, message, dh);
  REQUIRE(not buffered.error);
  CHECK(buffered.records.empty());
  auto replayed
    = decode(buffered_decoder, netflow_v9(102, 8, {{0, definition}}, 3), dh);
  REQUIRE(not replayed.error);
  REQUIRE_EQUAL(replayed.records.size(), size_t{1});
  CHECK_EQUAL(value(replayed.records.front(), "source_transport_port"),
              uint64_t{53});
  CHECK(dh.empty());
}

TEST("defers mixed NetFlow v9 sets until record counts are known") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto known_definition = template_record(256, {{4, 1}});
  REQUIRE(
    not decode(decoder, netflow_v9(100, 8, {{0, known_definition}}, 1), dh)
          .error);
  auto mixed = decode(
    decoder,
    netflow_v9(101, 8, {{257, raw({10, 0, 0, 1})}, {256, raw({6, 0, 0, 0})}}, 2,
               uint16_t{2}),
    dh);
  REQUIRE(not mixed.error);
  CHECK(mixed.records.empty());
  auto missing_definition = template_record(257, {{8, 4}});
  auto replayed
    = decode(decoder, netflow_v9(102, 8, {{0, missing_definition}}, 3), dh);
  REQUIRE(not replayed.error);
  REQUIRE_EQUAL(replayed.records.size(), size_t{2});
  CHECK_EQUAL(value(replayed.records.front(), "source_ipv4_address"),
              ip::v4(0x0a000001));
  CHECK_EQUAL(value(replayed.records.back(), "protocol_identifier"),
              uint64_t{6});
  CHECK(dh.empty());
}

TEST("rejects ambiguous buffered NetFlow v9 record counts") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto known_definition = template_record(256, {{7, 2}});
  REQUIRE(
    not decode(decoder, netflow_v9(100, 8, {{0, known_definition}}, 1), dh)
          .error);
  auto mixed = decode(decoder,
                      netflow_v9(101, 8,
                                 {{257, raw({10, 0, 0, 1})},
                                  {256, raw({0, 53, 0, 0})},
                                  {256, raw({1, 187, 0, 0})}},
                                 2, uint16_t{4}),
                      dh);
  REQUIRE(not mixed.error);
  CHECK(mixed.records.empty());
  auto missing_definition = template_record(257, {{8, 4}});
  auto replayed
    = decode(decoder, netflow_v9(102, 8, {{0, missing_definition}}, 3), dh);
  REQUIRE(not replayed.error);
  CHECK(replayed.records.empty());
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message,
              "discarded malformed buffered NetFlow data set");
}

TEST("bounds NetFlow v9 record-count selection") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definition = template_record(256, {{8, 4}});
  REQUIRE(not decode(decoder, netflow_v9(100, 8, {{0, definition}}), dh).error);
  auto message = bytes{};
  append_u16(message, 9);
  append_u16(message, 1'024);
  append_u32(message, 101);
  append_u32(message, 1'700'000'001);
  append_u32(message, 2);
  append_u32(message, 8);
  auto framed = FrameResult{};
  for (auto index = size_t{0}; index < 1'024; ++index) {
    append_set(message, Set{256, raw({10, 0, 0, 1})});
    framed = decoder.frame(message, false);
    if (index + 1 < 1'024) {
      CHECK_EQUAL(framed.status, FrameStatus::incomplete);
    }
  }
  REQUIRE_EQUAL(framed.status, FrameStatus::ready);
  CHECK_EQUAL(framed.size, message.size());
  auto result = decode(decoder, message, dh);
  REQUIRE(not result.error);
  CHECK_EQUAL(result.records.size(), size_t{1'024});
  CHECK(dh.empty());
}

TEST("bounds NetFlow v9 sets during framing and decoding") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto message = netflow_v9(100, 8, {}, 1, uint16_t{1});
  for (auto index = size_t{0}; index < 1'024; ++index) {
    append_set(message, Set{256, {}});
  }
  auto within_limit = decoder.frame(message, false);
  CHECK_EQUAL(within_limit.status, FrameStatus::ambiguous);
  CHECK_EQUAL(within_limit.size, message.size());
  append_u16(message, 256);
  append_u16(message, std::numeric_limits<uint16_t>::max());
  auto framed = decoder.frame(message, false);
  REQUIRE_EQUAL(framed.status, FrameStatus::error);
  CHECK_EQUAL(framed.size, size_t{0});
  CHECK_EQUAL(framed.version, uint16_t{9});
  CHECK_EQUAL(framed.message,
              "NetFlow v9 message contains more than 1024 sets");
  auto result = decode(decoder, message, dh);
  REQUIRE(result.error);
  CHECK_EQUAL(result.error->message,
              "NetFlow message contains more than 1024 sets");
  CHECK(result.records.empty());
  decoder.finish(dh);
  CHECK(dh.empty());
}

TEST("bounds IPFIX sets during decoding") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto message = ipfix(8, {});
  for (auto index = size_t{0}; index < 1'025; ++index) {
    append_set(message, Set{256, {}});
  }
  patch_u16(message, 2, static_cast<uint16_t>(message.size()));
  auto result = decode(decoder, message, dh);
  REQUIRE(result.error);
  CHECK_EQUAL(result.error->message,
              "NetFlow message contains more than 1024 sets");
  CHECK(result.records.empty());
  decoder.finish(dh);
  CHECK(dh.empty());
}

TEST("bounds aggregate template fields during framing and decoding") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definition = bytes{};
  append_u16(definition, 256);
  append_u16(definition, 1'024);
  for (auto index = size_t{0}; index < 1'024; ++index) {
    append_u16(definition, 4);
    append_u16(definition, 1);
  }
  auto message = netflow_v9(100, 8, {}, 1, uint16_t{257});
  for (auto index = size_t{0}; index < 256; ++index) {
    append_set(message, Set{0, definition});
  }
  auto within_limit = decoder.frame(message, false);
  CHECK_EQUAL(within_limit.status, FrameStatus::incomplete);
  auto boundary_message = message;
  patch_u16(boundary_message, 2, 256);
  auto boundary_decoder = Decoder{};
  auto boundary_dh = collecting_diagnostic_handler{};
  auto boundary = decode(boundary_decoder, boundary_message, boundary_dh);
  REQUIRE(not boundary.error);
  CHECK(boundary.records.empty());
  auto boundary_record
    = decode(boundary_decoder,
             netflow_v9(101, 8, {{256, bytes(1024, std::byte{6})}}, 2),
             boundary_dh);
  REQUIRE(not boundary_record.error);
  REQUIRE_EQUAL(boundary_record.records.size(), size_t{1});
  CHECK_EQUAL(value(boundary_record.records.front(), "protocol_identifier"),
              uint64_t{6});
  CHECK(boundary_dh.empty());
  auto options_definition = bytes{};
  append_u16(options_definition, 400);
  append_u16(options_definition, 4);
  append_u16(options_definition, 0);
  append_u16(options_definition, 1);
  append_u16(options_definition, 4);
  append_set(message, Set{1, std::move(options_definition)});
  auto framed = decoder.frame(message, false);
  REQUIRE_EQUAL(framed.status, FrameStatus::error);
  CHECK_EQUAL(framed.size, size_t{0});
  CHECK_EQUAL(framed.version, uint16_t{9});
  CHECK_EQUAL(framed.message,
              "NetFlow message contains more than 262144 template fields");
  auto result = decode(decoder, message, dh);
  REQUIRE(result.error);
  CHECK_EQUAL(result.error->message,
              "NetFlow message contains more than 262144 template fields");
  CHECK(result.records.empty());
  auto uncommitted
    = decode(decoder, netflow_v9(101, 8, {{256, raw({6})}}, 2), dh);
  REQUIRE(not uncommitted.error);
  CHECK(uncommitted.records.empty());
  auto valid_definition = template_record(256, {{4, 1}});
  auto replayed
    = decode(decoder, netflow_v9(102, 8, {{0, valid_definition}}, 3), dh);
  REQUIRE(not replayed.error);
  REQUIRE_EQUAL(replayed.records.size(), size_t{1});
  CHECK_EQUAL(value(replayed.records.front(), "protocol_identifier"),
              uint64_t{6});
  CHECK(dh.empty());
}

TEST("decodes NetFlow v5 metadata and fields") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto result = decode(decoder, netflow_v5(), dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  auto const& record = result.records.front();
  CHECK_EQUAL(record.metadata.version, Version::v5);
  REQUIRE(record.metadata.v5);
  CHECK_EQUAL(record.metadata.v5->engine_type, uint8_t{1});
  CHECK_EQUAL(record.metadata.v5->engine_id, uint8_t{2});
  CHECK_EQUAL(record.metadata.v5->sampling_mode, uint8_t{1});
  CHECK_EQUAL(record.metadata.v5->sampling_interval, uint16_t{5});
  CHECK_EQUAL(value(record, "source_ipv4_address"), ip::v4(0x01020304));
  CHECK_EQUAL(value(record, "destination_ipv4_address"), ip::v4(0x05060708));
  CHECK_EQUAL(value(record, "packet_delta_count"), uint64_t{15});
  CHECK_EQUAL(value(record, "source_transport_port"), uint64_t{17});
  CHECK_EQUAL(value(record, "protocol_identifier"), uint64_t{17});
  CHECK_EQUAL(value(record, "flow_start"), time{} + std::chrono::seconds{999});
  CHECK_EQUAL(value(record, "flow_end"),
              time{} + std::chrono::milliseconds{999'500});
  CHECK(not has_field(record, "community_id"));
  CHECK(dh.empty());
}

TEST("rejects invalid NetFlow v5 nanosecond residues") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto accepted
    = decode(decoder, netflow_v5(true, 10'000, 9'000, 9'500, 999'999'999), dh);
  REQUIRE(not accepted.error);
  REQUIRE_EQUAL(accepted.records.size(), size_t{1});
  CHECK_EQUAL(accepted.records.front().metadata.export_time,
              time{} + std::chrono::seconds{1'000}
                + std::chrono::nanoseconds{999'999'999});
  CHECK_EQUAL(value(accepted.records.front(), "flow_start"),
              time{} + std::chrono::seconds{999}
                + std::chrono::nanoseconds{999'999'999});
  CHECK_EQUAL(value(accepted.records.front(), "flow_end"),
              time{} + std::chrono::seconds{1'000}
                + std::chrono::nanoseconds{499'999'999});
  auto message = netflow_v5(false, 10'000, 9'000, 9'500, 1'000'000'000);
  auto framed = decoder.frame(message, false);
  REQUIRE_EQUAL(framed.status, FrameStatus::ready);
  CHECK_EQUAL(framed.size, message.size());
  auto result = decode(decoder, message, dh);
  REQUIRE(result.error);
  CHECK_EQUAL(result.error->kind, DecodeErrorKind::malformed);
  CHECK_EQUAL(result.error->version, uint16_t{5});
  CHECK(result.records.empty());
  CHECK_EQUAL(result.error->message,
              "NetFlow v5 unix_nsecs 1000000000 is outside [0, 1000000000)");
  result = decode(decoder,
                  netflow_v5(false, 10'000, 9'000, 9'500,
                             std::numeric_limits<uint32_t>::max()),
                  dh);
  REQUIRE(result.error);
  CHECK(result.records.empty());
  CHECK_EQUAL(result.error->message,
              "NetFlow v5 unix_nsecs 4294967295 is outside [0, 1000000000)");
  CHECK(dh.empty());
}

TEST("decodes NetFlow v5 timestamps across sysUptime rollover") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto result
    = decode(decoder, netflow_v5(true, 500, uint32_t{0xffff'fe0c}, 250), dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  auto const& record = result.records.front();
  CHECK_EQUAL(value(record, "flow_start"), time{} + std::chrono::seconds{999});
  CHECK_EQUAL(value(record, "flow_end"),
              time{} + std::chrono::milliseconds{999'750});
  CHECK(dh.empty());
}

TEST("buffers IPFIX data and decodes IANA and enterprise types") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto const peer = Peer{ip::v4(0xc0000201), 2055};
  auto payload = raw({10, 0, 0, 1,   0,   1,   2,    1,    1, 2, 3,    4,
                      5,  6, 3, 'd', 'n', 's', 0xaa, 0xbb, 1, 2, 0xcc, 0xdd});
  auto buffered = decode(decoder, ipfix(7, {{256, payload}}, 17), dh, peer);
  REQUIRE(not buffered.error);
  CHECK(buffered.records.empty());
  auto definitions = template_record(256, {{8, 4},
                                           {1, 3},
                                           {276, 1},
                                           {56, 6},
                                           {111, 0xffff, 3054},
                                           {600, 2},
                                           {291, 2},
                                           {700, 2, 4242}});
  auto replayed = decode(decoder, ipfix(7, {{2, definitions}}, 18), dh, peer);
  REQUIRE(not replayed.error);
  REQUIRE_EQUAL(replayed.records.size(), size_t{1});
  auto const& record = replayed.records.front();
  CHECK_EQUAL(record.metadata.sequence_number, uint32_t{17});
  REQUIRE(record.metadata.exporter);
  CHECK_EQUAL(record.metadata.exporter->address, peer.address);
  CHECK_EQUAL(record.metadata.exporter->port, peer.port);
  CHECK_EQUAL(value(record, "source_ipv4_address"), ip::v4(0x0a000001));
  CHECK_EQUAL(value(record, "octet_delta_count"), uint64_t{258});
  CHECK_EQUAL(value(record, "data_records_reliability"), true);
  CHECK_EQUAL(value(record, "source_mac_address"), "01:02:03:04:05:06");
  CHECK_EQUAL(value(record, "ixia_l7_application_name"), "dns");
  CHECK_EQUAL(value(record, "ie_600"),
              (blob{std::byte{0xaa}, std::byte{0xbb}}));
  CHECK_EQUAL(value(record, "basic_list"), (blob{std::byte{1}, std::byte{2}}));
  CHECK_EQUAL(value(record, "pen_4242_ie_700"),
              (blob{std::byte{0xcc}, std::byte{0xdd}}));
  auto diagnostics = std::move(dh).collect();
  CHECK_EQUAL(diagnostics.size(), size_t{2});
}

TEST("drops a malformed buffered IPFIX data set atomically") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto payload = raw({1, 'a', 255, 0});
  auto buffered = decode(decoder, ipfix(7, {{256, payload}}, 17), dh);
  REQUIRE(not buffered.error);
  auto definition = template_record(256, {{82, 0xffff}});
  auto replayed = decode(decoder, ipfix(7, {{2, definition}}, 18), dh);
  REQUIRE(not replayed.error);
  CHECK(replayed.records.empty());
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message,
              "discarded malformed buffered NetFlow data set");
}

TEST("bounds narrow buffered replay after restoring a snapshot") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto payload = bytes(2048, std::byte{6});
  for (auto index = uint32_t{0}; index < 33; ++index) {
    auto buffered
      = decode(decoder, ipfix(7, {{256, payload}}, index * 2048), dh);
    REQUIRE(not buffered.error);
    CHECK(buffered.records.empty());
  }
  auto buffer = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{buffer};
  REQUIRE(serializer.begin_object(caf::invalid_type_id, ""));
  auto saving = Serde{serializer};
  decoder.snapshot(saving);
  REQUIRE(serializer.end_object());
  auto restored = Decoder{};
  auto deserializer = caf::binary_deserializer{buffer};
  REQUIRE(deserializer.begin_object(caf::invalid_type_id, ""));
  auto loading = Serde{deserializer};
  restored.snapshot(loading);
  REQUIRE(deserializer.end_object());
  auto definition = template_record(256, {{4, 1}});
  auto replayed = decode(restored, ipfix(7, {{2, definition}}, 33 * 2048), dh);
  REQUIRE(not replayed.error);
  CHECK_EQUAL(replayed.records.size(), size_t{64 * 1024});
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message,
              "discarded buffered NetFlow data after decoded work limit");
}

TEST("drops an oversized buffered replay group atomically") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto small = bytes(2048, std::byte{6});
  for (auto index = uint32_t{0}; index < 31; ++index) {
    REQUIRE(
      not decode(decoder, ipfix(7, {{256, small}}, index * 2048), dh).error);
  }
  auto large = bytes(4096, std::byte{6});
  REQUIRE(not decode(decoder, ipfix(7, {{256, large}}, 31 * 2048), dh).error);
  auto definition = template_record(256, {{4, 1}});
  auto replayed = decode(decoder, ipfix(7, {{2, definition}}, 33 * 2048), dh);
  REQUIRE(not replayed.error);
  CHECK_EQUAL(replayed.records.size(), size_t{31 * 2048});
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message,
              "discarded buffered NetFlow data after decoded work limit");
}

TEST("bounds decoded fields while replaying wide buffered records") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto payload = bytes(1024, std::byte{6});
  for (auto index = uint32_t{0}; index < 63; ++index) {
    REQUIRE(not decode(decoder, ipfix(7, {{256, payload}}, index), dh).error);
  }
  auto final_payload = bytes(2048, std::byte{6});
  REQUIRE(not decode(decoder, ipfix(7, {{256, final_payload}}, 63), dh).error);
  auto definition = bytes{};
  append_u16(definition, 256);
  append_u16(definition, 1024);
  for (auto index = size_t{0}; index < 1024; ++index) {
    append_u16(definition, 4);
    append_u16(definition, 1);
  }
  auto replayed = decode(decoder, ipfix(7, {{2, definition}}, 65), dh);
  REQUIRE(not replayed.error);
  CHECK_EQUAL(replayed.records.size(), size_t{63});
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message,
              "discarded buffered NetFlow data after decoded work limit");
}

TEST("rejects an IPFIX message without mutating template state") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto buffered
    = decode(decoder, ipfix(7, {{256, raw({10, 0, 0, 1})}}, 17), dh);
  REQUIRE(not buffered.error);
  auto malformed_definitions = template_record(256, {{8, 4}});
  append_u16(malformed_definitions, 257);
  append_u16(malformed_definitions, 1);
  auto rejected
    = decode(decoder, ipfix(7, {{2, malformed_definitions}}, 18), dh);
  REQUIRE(rejected.error);
  auto definition = template_record(256, {{8, 4}});
  auto replayed = decode(decoder, ipfix(7, {{2, definition}}, 19), dh);
  REQUIRE(not replayed.error);
  REQUIRE_EQUAL(replayed.records.size(), size_t{1});
  CHECK(dh.empty());
}

TEST("preserves an out-of-range IPFIX timestamp as a blob") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definition = template_record(256, {{153, 8}});
  REQUIRE(not decode(decoder, ipfix(7, {{2, definition}}), dh).error);
  auto result = decode(
    decoder,
    ipfix(7, {{256, raw({0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})}}),
    dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  CHECK_EQUAL(value(result.records.front(), "flow_end_milliseconds"),
              (blob{std::byte{0xff}, std::byte{0xff}, std::byte{0xff},
                    std::byte{0xff}, std::byte{0xff}, std::byte{0xff},
                    std::byte{0xff}, std::byte{0xff}}));
  CHECK(dh.empty());
}

TEST("preserves invalid UTF-8 information elements as blobs") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definition = template_record(256, {{82, 3}});
  REQUIRE(not decode(decoder, ipfix(7, {{2, definition}}), dh).error);
  auto result
    = decode(decoder, ipfix(7, {{256, raw({0xe2, 0x28, 0xa1})}}, 2), dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  CHECK_EQUAL(value(result.records.front(), "interface_name"),
              (blob{std::byte{0xe2}, std::byte{0x28}, std::byte{0xa1}}));
  CHECK(dh.empty());
}

TEST("unfolds post-2036 IPFIX NTP timestamps") {
  auto const reference = time{} + std::chrono::seconds{2'208'988'800};
  auto decoder = Decoder{reference};
  auto dh = collecting_diagnostic_handler{};
  auto definition = template_record(256, {{154, 8}, {156, 8}});
  REQUIRE(not decode(decoder, ipfix(7, {{2, definition}}), dh).error);
  auto timestamp = bytes{};
  append_u32(timestamp, 123'010'304);
  append_u32(timestamp, uint32_t{0x8000'0000});
  auto payload = timestamp;
  payload.insert(payload.end(), timestamp.begin(), timestamp.end());
  auto result = decode(
    decoder, ipfix(7, {{256, std::move(payload)}}, 2, 2'208'988'800), dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  auto const expected = time{} + std::chrono::seconds{2'208'988'800}
                        + std::chrono::milliseconds{500};
  CHECK_EQUAL(value(result.records.front(), "flow_start_microseconds"),
              expected);
  CHECK_EQUAL(value(result.records.front(), "flow_start_nanoseconds"),
              expected);
  CHECK(dh.empty());
}

TEST("ignores reserved IPFIX microsecond fraction bits") {
  auto decoder = Decoder{time{}};
  auto dh = collecting_diagnostic_handler{};
  auto definition = template_record(256, {{154, 8}, {155, 8}});
  REQUIRE(not decode(decoder, ipfix(7, {{2, definition}}, 1, 0), dh).error);
  auto payload = bytes{};
  append_u32(payload, 2'208'988'800);
  append_u32(payload, uint32_t{0x0000'1000});
  append_u32(payload, 2'208'988'800);
  append_u32(payload, uint32_t{0x0000'17ff});
  auto result
    = decode(decoder, ipfix(7, {{256, std::move(payload)}}, 2, 0), dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  CHECK_EQUAL(value(result.records.front(), "flow_start_microseconds"), time{});
  CHECK_EQUAL(value(result.records.front(), "flow_end_microseconds"), time{});
  CHECK(dh.empty());
}

TEST("unfolds IPFIX timestamps after the 2106 UNIX wrap") {
  auto const expected = time{} + std::chrono::seconds{4'294'967'296};
  auto decoder = Decoder{expected};
  auto dh = collecting_diagnostic_handler{};
  auto definition = template_record(256, {{150, 4}, {154, 8}});
  REQUIRE(not decode(decoder,
                     ipfix(7, {{2, definition}}, 1, uint32_t{0xffff'ffff}), dh)
                .error);
  auto buffer = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{buffer};
  REQUIRE(serializer.begin_object(caf::invalid_type_id, ""));
  auto saving = Serde{serializer};
  decoder.snapshot(saving);
  REQUIRE(serializer.end_object());
  auto restored = Decoder{time{}};
  auto deserializer = caf::binary_deserializer{buffer};
  REQUIRE(deserializer.begin_object(caf::invalid_type_id, ""));
  auto loading = Serde{deserializer};
  restored.snapshot(loading);
  REQUIRE(deserializer.end_object());
  auto payload = [](uint32_t seconds) {
    auto result = bytes{};
    append_u32(result, seconds);
    append_u32(result, 2'208'988'800 + seconds);
    append_u32(result, 0);
    return result;
  };
  auto result = decode(restored, ipfix(7, {{256, payload(0)}}, 2, 0), dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  CHECK_EQUAL(result.records.front().metadata.export_time, expected);
  CHECK_EQUAL(value(result.records.front(), "flow_start_seconds"), expected);
  CHECK_EQUAL(value(result.records.front(), "flow_start_microseconds"),
              expected);
  auto delayed = decode(restored,
                        ipfix(7, {{256, payload(uint32_t{0xffff'ffff})}}, 1,
                              uint32_t{0xffff'ffff}),
                        dh);
  REQUIRE(not delayed.error);
  CHECK(delayed.records.empty());
  auto following = decode(restored, ipfix(7, {{256, payload(1)}}, 3, 1), dh);
  REQUIRE(not following.error);
  REQUIRE_EQUAL(following.records.size(), size_t{1});
  auto const next = expected + std::chrono::seconds{1};
  CHECK_EQUAL(following.records.front().metadata.export_time, next);
  CHECK_EQUAL(value(following.records.front(), "flow_start_seconds"), next);
  CHECK_EQUAL(value(following.records.front(), "flow_start_microseconds"),
              next);
  auto other_exporter = decode(
    restored, ipfix(8, {{2, definition}, {256, payload(0)}}, 1, 0), dh);
  REQUIRE(not other_exporter.error);
  REQUIRE_EQUAL(other_exporter.records.size(), size_t{1});
  CHECK_EQUAL(other_exporter.records.front().metadata.export_time, expected);
  CHECK_EQUAL(value(other_exporter.records.front(), "flow_start_seconds"),
              expected);
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message,
              "discarded reordered IPFIX data set");
}

TEST("rejects unfolded timestamps outside the supported range") {
  auto const reference_seconds
    = std::chrono::duration_cast<std::chrono::seconds>(
        time::max().time_since_epoch())
        .count();
  auto const raw_reference = static_cast<uint32_t>(reference_seconds);
  auto decoder = Decoder{time::max()};
  auto dh = collecting_diagnostic_handler{};
  auto definition = template_record(256, {{156, 8}});
  REQUIRE(not decode(decoder, ipfix(7, {{2, definition}}, 1, raw_reference), dh)
                .error);
  auto payload = bytes{};
  append_u32(payload, static_cast<uint32_t>(reference_seconds + 2'208'988'800));
  append_u32(payload, uint32_t{0xffff'ffff});
  auto const expected_blob = blob{payload.begin(), payload.end()};
  auto result = decode(
    decoder, ipfix(7, {{256, std::move(payload)}}, 2, raw_reference), dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  CHECK_EQUAL(value(result.records.front(), "flow_start_nanoseconds"),
              expected_blob);
  auto rejected = decode(decoder, ipfix(7, {}, 3, raw_reference + 1), dh);
  REQUIRE(rejected.error);
  CHECK_EQUAL(rejected.error->message,
              "IPFIX export time is outside the supported range");
  CHECK(dh.empty());
}

TEST("decodes short IPFIX records and template-relative padding") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definition = template_record(256, {{4, 1}, {5, 1}});
  REQUIRE(not decode(decoder, ipfix(7, {{2, definition}}), dh).error);
  auto result = decode(decoder, ipfix(7, {{256, raw({6, 16, 0xff})}}), dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  CHECK_EQUAL(value(result.records.front(), "protocol_identifier"),
              uint64_t{6});
  CHECK_EQUAL(value(result.records.front(), "ip_class_of_service"),
              uint64_t{16});
  CHECK(dh.empty());
}

TEST("isolates IPFIX exporters by port") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto const first = Peer{ip::v4(0xc0000201), 1000};
  auto const second = Peer{ip::v4(0xc0000201), 2000};
  auto source_template = template_record(256, {{8, 4}});
  REQUIRE(
    not decode(decoder, ipfix(9, {{2, source_template}}), dh, first).error);
  auto missing
    = decode(decoder, ipfix(9, {{256, raw({10, 0, 0, 2})}}), dh, second);
  REQUIRE(not missing.error);
  CHECK(missing.records.empty());
  auto destination_template = template_record(256, {{12, 4}});
  auto replayed
    = decode(decoder, ipfix(9, {{2, destination_template}}), dh, second);
  REQUIRE(not replayed.error);
  REQUIRE_EQUAL(replayed.records.size(), size_t{1});
  CHECK(has_field(replayed.records.front(), "destination_ipv4_address"));
  CHECK(not has_field(replayed.records.front(), "source_ipv4_address"));
  auto isolated
    = decode(decoder, ipfix(9, {{256, raw({10, 0, 0, 1})}}, 2), dh, first);
  REQUIRE(not isolated.error);
  REQUIRE_EQUAL(isolated.records.size(), size_t{1});
  CHECK(has_field(isolated.records.front(), "source_ipv4_address"));
  CHECK(not has_field(isolated.records.front(), "destination_ipv4_address"));
  CHECK(dh.empty());
}

TEST("replaces and withdraws IPFIX templates") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto const peer = Peer{ip::v4(0xc6336401), 4739};
  auto source_template = template_record(300, {{8, 4}});
  REQUIRE(
    not decode(decoder, ipfix(1, {{2, source_template}}), dh, peer).error);
  auto first = decode(decoder, ipfix(1, {{300, raw({10, 0, 0, 1})}}), dh, peer);
  REQUIRE_EQUAL(first.records.size(), size_t{1});
  CHECK(has_field(first.records.front(), "source_ipv4_address"));
  auto destination_template = template_record(300, {{12, 4}});
  REQUIRE(
    not decode(decoder, ipfix(1, {{2, destination_template}}), dh, peer).error);
  auto second
    = decode(decoder, ipfix(1, {{300, raw({10, 0, 0, 2})}}), dh, peer);
  REQUIRE_EQUAL(second.records.size(), size_t{1});
  CHECK(has_field(second.records.front(), "destination_ipv4_address"));
  auto missing
    = decode(decoder, ipfix(1, {{301, raw({10, 0, 0, 3})}}), dh, peer);
  REQUIRE(not missing.error);
  CHECK(missing.records.empty());
  auto missing_withdrawal = template_record(301, {});
  REQUIRE(
    not decode(decoder, ipfix(1, {{2, missing_withdrawal}}), dh, peer).error);
  auto missing_definition = template_record(301, {{8, 4}});
  auto discarded
    = decode(decoder, ipfix(1, {{2, missing_definition}}), dh, peer);
  REQUIRE(not discarded.error);
  CHECK(discarded.records.empty());
  auto withdrawal = template_record(300, {});
  REQUIRE(not decode(decoder, ipfix(1, {{2, withdrawal}}), dh, peer).error);
  auto after_withdrawal
    = decode(decoder, ipfix(1, {{300, raw({10, 0, 0, 3})}}), dh, peer);
  REQUIRE(not after_withdrawal.error);
  CHECK(after_withdrawal.records.empty());
  auto replayed
    = decode(decoder, ipfix(1, {{2, destination_template}}), dh, peer);
  REQUIRE(not replayed.error);
  REQUIRE_EQUAL(replayed.records.size(), size_t{1});
  CHECK(has_field(replayed.records.front(), "destination_ipv4_address"));
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message,
              "discarded buffered NetFlow data set");
}

TEST("warns when an implicit peer redefines a template") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto source_template = template_record(300, {{8, 4}});
  REQUIRE(not decode(decoder, ipfix(1, {{2, source_template}}, 1), dh).error);
  auto destination_template = template_record(300, {{12, 4}});
  REQUIRE(
    not decode(decoder, ipfix(1, {{2, destination_template}}, 2), dh).error);
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message, "redefined NetFlow template");
  CHECK_EQUAL(diagnostics.front().notes.size(), size_t{2});
}

TEST("stays quiet when a known peer redefines a template") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto const peer = Peer{ip::v4(0xc6336401), 4739};
  auto source_template = template_record(300, {{8, 4}});
  REQUIRE(
    not decode(decoder, ipfix(1, {{2, source_template}}, 1), dh, peer).error);
  auto destination_template = template_record(300, {{12, 4}});
  REQUIRE(
    not decode(decoder, ipfix(1, {{2, destination_template}}, 2), dh, peer)
          .error);
  CHECK(dh.empty());
}

TEST("detects IPFIX exporter restarts") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto const peer = Peer{ip::v4(0xc6336401), 4739};
  auto source_template = template_record(300, {{8, 4}});
  REQUIRE(
    not decode(decoder,
               ipfix(1, {{2, source_template}}, 3'000'000'000, 1'700'000'000),
               dh, peer)
          .error);
  auto restarted = decode(
    decoder, ipfix(1, {{300, raw({10, 0, 0, 1})}}, 1, 1'700'000'001), dh, peer);
  REQUIRE(not restarted.error);
  CHECK(restarted.records.empty());
  auto destination_template = template_record(300, {{12, 4}});
  auto replayed = decode(
    decoder, ipfix(1, {{2, destination_template}}, 2, 1'700'000'002), dh, peer);
  REQUIRE(not replayed.error);
  REQUIRE_EQUAL(replayed.records.size(), size_t{1});
  CHECK(has_field(replayed.records.front(), "destination_ipv4_address"));
  CHECK(not has_field(replayed.records.front(), "source_ipv4_address"));
  CHECK(dh.empty());
}

TEST("detects IPFIX exporter restarts within one export second") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto const peer = Peer{ip::v4(0xc6336401), 4739};
  auto source_template = template_record(300, {{8, 4}});
  REQUIRE(
    not decode(decoder,
               ipfix(1, {{2, source_template}}, 3'000'000'000, 1'700'000'000),
               dh, peer)
          .error);
  auto restarted = decode(
    decoder, ipfix(1, {{300, raw({10, 0, 0, 1})}}, 1, 1'700'000'000), dh, peer);
  REQUIRE(not restarted.error);
  CHECK(restarted.records.empty());
  auto destination_template = template_record(300, {{12, 4}});
  auto replayed = decode(
    decoder, ipfix(1, {{2, destination_template}}, 2, 1'700'000'000), dh, peer);
  REQUIRE(not replayed.error);
  REQUIRE_EQUAL(replayed.records.size(), size_t{1});
  CHECK_EQUAL(replayed.records.front().metadata.sequence_number, uint32_t{1});
  CHECK_EQUAL(replayed.records.front().metadata.export_time,
              time{} + std::chrono::seconds{1'700'000'000});
  CHECK(has_field(replayed.records.front(), "destination_ipv4_address"));
  CHECK(not has_field(replayed.records.front(), "source_ipv4_address"));
  CHECK(dh.empty());
}

TEST("preserves current templates across reordered IPFIX messages") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto const peer = Peer{ip::v4(0xc6336401), 4739};
  auto source_template = template_record(300, {{8, 4}});
  REQUIRE(not decode(decoder,
                     ipfix(1, {{2, source_template}}, 100, 1'700'000'000), dh,
                     peer)
                .error);
  auto destination_template = template_record(300, {{12, 4}});
  REQUIRE(not decode(decoder,
                     ipfix(1, {{2, destination_template}}, 102, 1'700'000'002),
                     dh, peer)
                .error);
  auto delayed
    = decode(decoder,
             ipfix(1, {{2, source_template}, {300, raw({10, 0, 0, 1})}}, 101,
                   1'700'000'001),
             dh, peer);
  REQUIRE(not delayed.error);
  REQUIRE_EQUAL(delayed.records.size(), size_t{1});
  CHECK(has_field(delayed.records.front(), "source_ipv4_address"));
  CHECK(not has_field(delayed.records.front(), "destination_ipv4_address"));
  auto current
    = decode(decoder, ipfix(1, {{300, raw({10, 0, 0, 2})}}, 103, 1'700'000'003),
             dh, peer);
  REQUIRE(not current.error);
  REQUIRE_EQUAL(current.records.size(), size_t{1});
  CHECK(has_field(current.records.front(), "destination_ipv4_address"));
  CHECK(not has_field(current.records.front(), "source_ipv4_address"));
  CHECK(dh.empty());
}

TEST("discards unresolved IPFIX data after withdraw-all") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto buffered = decode(decoder, ipfix(1, {{300, raw({10, 0, 0, 1})}}), dh);
  REQUIRE(not buffered.error);
  CHECK(buffered.records.empty());
  auto withdraw_all = template_record(2, {});
  REQUIRE(not decode(decoder, ipfix(1, {{2, withdraw_all}}), dh).error);
  auto destination_template = template_record(300, {{12, 4}});
  auto defined = decode(decoder, ipfix(1, {{2, destination_template}}), dh);
  REQUIRE(not defined.error);
  CHECK(defined.records.empty());
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message,
              "discarded buffered NetFlow data set");
}

TEST("orders NetFlow v9 messages and detects restarts") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto const first = Peer{ip::v4(0xcb007101), 1000};
  auto const second = Peer{ip::v4(0xcb007101), 2000};
  auto source_template = template_record(256, {{8, 4}});
  REQUIRE(not decode(decoder, netflow_v9(1'000, 7, {{0, source_template}}, 100),
                     dh, first)
                .error);
  auto shared = decode(decoder,
                       netflow_v9(1'100, 7, {{256, raw({10, 0, 0, 1})}}, 101,
                                  None{}, 1'700'000'001),
                       dh, second);
  REQUIRE_EQUAL(shared.records.size(), size_t{1});
  REQUIRE(shared.records.front().metadata.exporter);
  CHECK_EQUAL(shared.records.front().metadata.exporter->port, uint16_t{2000});
  auto reordered = decode(decoder,
                          netflow_v9(900, 7, {{256, raw({10, 0, 0, 2})}}, 99,
                                     None{}, 1'699'999'999),
                          dh, second);
  REQUIRE(not reordered.error);
  CHECK(reordered.records.empty());
  auto destination_template = template_record(256, {{12, 4}});
  REQUIRE(not decode(decoder,
                     netflow_v9(1'200, 7, {{0, destination_template}}, 102,
                                None{}, 1'700'000'002),
                     dh, first)
                .error);
  auto stale = decode(decoder,
                      netflow_v9(1'050, 7, {{256, raw({10, 0, 0, 8})}}, 100,
                                 None{}, 1'700'000'001),
                      dh, second);
  REQUIRE(not stale.error);
  CHECK(stale.records.empty());
  auto delayed = decode(
    decoder,
    netflow_v9(1'050, 7, {{0, source_template}, {256, raw({10, 0, 0, 9})}}, 100,
               uint16_t{2}, 1'700'000'001),
    dh, second);
  REQUIRE(not delayed.error);
  REQUIRE_EQUAL(delayed.records.size(), size_t{1});
  CHECK(has_field(delayed.records.front(), "source_ipv4_address"));
  CHECK(not has_field(delayed.records.front(), "destination_ipv4_address"));
  auto after_reordering
    = decode(decoder,
             netflow_v9(1'300, 7, {{256, raw({10, 0, 0, 3})}}, 103, None{},
                        1'700'000'003),
             dh, second);
  REQUIRE_EQUAL(after_reordering.records.size(), size_t{1});
  CHECK(
    has_field(after_reordering.records.front(), "destination_ipv4_address"));
  auto restarted = decode(decoder,
                          netflow_v9(100, 7, {{256, raw({10, 0, 0, 4})}}, 1,
                                     None{}, 1'700'000'004),
                          dh, second);
  REQUIRE(not restarted.error);
  CHECK(restarted.records.empty());
  auto replayed = decode(decoder,
                         netflow_v9(150, 7, {{0, destination_template}}, 2,
                                    None{}, 1'700'000'005),
                         dh, first);
  REQUIRE_EQUAL(replayed.records.size(), size_t{1});
  CHECK(has_field(replayed.records.front(), "destination_ipv4_address"));
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{2});
  CHECK_EQUAL(diagnostics.front().message,
              "discarded reordered NetFlow v9 data set");
  CHECK_EQUAL(diagnostics.back().message,
              "discarded reordered NetFlow v9 data set");
}

TEST("handles NetFlow v9 sysUptime rollover") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definition = template_record(256, {{8, 4}});
  REQUIRE(not decode(decoder,
                     netflow_v9(uint32_t{0xffff'ff00}, 7, {{0, definition}}, 1),
                     dh)
                .error);
  auto result = decode(decoder,
                       netflow_v9(256, 7, {{256, raw({10, 0, 0, 1})}}, 2,
                                  None{}, 1'700'000'001),
                       dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  CHECK(dh.empty());
}

TEST("detects NetFlow v9 restarts after long uptimes") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto source_template = template_record(256, {{8, 4}});
  REQUIRE(not decode(decoder,
                     netflow_v9(3'000'000'000, 7, {{0, source_template}},
                                100'000, None{}, 1'700'000'000),
                     dh)
                .error);
  auto restarted = decode(decoder,
                          netflow_v9(100, 7, {{256, raw({10, 0, 0, 1})}}, 1,
                                     None{}, 1'700'000'001),
                          dh);
  REQUIRE(not restarted.error);
  CHECK(restarted.records.empty());
  auto destination_template = template_record(256, {{12, 4}});
  auto replayed = decode(decoder,
                         netflow_v9(200, 7, {{0, destination_template}}, 2,
                                    None{}, 1'700'000'002),
                         dh);
  REQUIRE(not replayed.error);
  REQUIRE_EQUAL(replayed.records.size(), size_t{1});
  CHECK(has_field(replayed.records.front(), "destination_ipv4_address"));
  CHECK(dh.empty());
}

TEST("decodes NetFlow v9 options templates with enterprise fields") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto options = bytes{};
  append_u16(options, 400);
  append_u16(options, 4);
  append_u16(options, 8);
  append_u16(options, 1);
  append_u16(options, 4);
  append_u16(options, uint16_t{0x8000 | 110});
  append_u16(options, 4);
  append_u32(options, 3054);
  auto defined = decode(decoder, netflow_v9(100, 1, {{1, options}}), dh);
  REQUIRE(not defined.error);
  auto result = decode(
    decoder, netflow_v9(101, 1, {{400, raw({0, 0, 0, 1, 0, 0, 0, 2})}}), dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  CHECK_EQUAL(result.records.front().metadata.record_kind, RecordKind::options);
  CHECK_EQUAL(value(result.records.front(), "scope_system"), uint64_t{1});
  CHECK_EQUAL(value(result.records.front(), "ixia_l7_application_id"),
              uint64_t{2});
}

TEST("decodes NetFlow v9 options templates without scope fields") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definition = bytes{};
  append_u16(definition, 400);
  append_u16(definition, 0);
  append_u16(definition, 4);
  append_u16(definition, 1);
  append_u16(definition, 4);
  auto message = netflow_v9(100, 1, {{1, definition}});
  auto framed = decoder.frame(message, false);
  REQUIRE_EQUAL(framed.status, FrameStatus::ready);
  CHECK_EQUAL(framed.size, message.size());
  auto defined = decode(decoder, message, dh);
  REQUIRE(not defined.error);
  auto result
    = decode(decoder, netflow_v9(101, 1, {{400, raw({0, 0, 0, 2})}}), dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  CHECK_EQUAL(result.records.front().metadata.record_kind, RecordKind::options);
  CHECK_EQUAL(value(result.records.front(), "octet_delta_count"), uint64_t{2});
  CHECK(dh.empty());
}

TEST("withdraws an IPFIX options template without a scope count") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto initial = bytes{};
  append_u16(initial, 400);
  append_u16(initial, 1);
  append_u16(initial, 1);
  append_u16(initial, 4);
  append_u16(initial, 1);
  REQUIRE(not decode(decoder, ipfix(1, {{3, initial}}), dh).error);
  auto replacement = bytes{};
  append_u16(replacement, 400);
  append_u16(replacement, 0);
  append_u16(replacement, 401);
  append_u16(replacement, 1);
  append_u16(replacement, 1);
  append_u16(replacement, 4);
  append_u16(replacement, 1);
  REQUIRE(not decode(decoder, ipfix(1, {{3, replacement}}), dh).error);
  auto withdrawn = decode(decoder, ipfix(1, {{400, raw({6})}}, 2), dh);
  REQUIRE(not withdrawn.error);
  CHECK(withdrawn.records.empty());
  auto restored = decode(decoder, ipfix(1, {{3, initial}}, 3), dh);
  REQUIRE(not restored.error);
  REQUIRE_EQUAL(restored.records.size(), size_t{1});
  CHECK_EQUAL(restored.records.front().metadata.record_kind,
              RecordKind::options);
  CHECK_EQUAL(value(restored.records.front(), "protocol_identifier"),
              uint64_t{6});
  auto result = decode(decoder, ipfix(1, {{401, raw({6})}}, 4), dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  CHECK_EQUAL(result.records.front().metadata.record_kind, RecordKind::options);
  CHECK_EQUAL(value(result.records.front(), "protocol_identifier"),
              uint64_t{6});
  CHECK(dh.empty());
}

TEST("rejects IPFIX options templates without scope fields") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definition = bytes{};
  append_u16(definition, 400);
  append_u16(definition, 1);
  append_u16(definition, 0);
  append_u16(definition, 4);
  append_u16(definition, 1);
  auto result = decode(decoder, ipfix(1, {{3, definition}}), dh);
  REQUIRE(result.error);
  CHECK_EQUAL(result.error->message,
              "IPFIX options template has no scope fields");
  auto pending = decode(decoder, ipfix(1, {{400, raw({6})}}, 2), dh);
  REQUIRE(not pending.error);
  CHECK(pending.records.empty());
  auto valid_definition = template_record(400, {{4, 1}});
  auto replayed = decode(decoder, ipfix(1, {{2, valid_definition}}, 3), dh);
  REQUIRE(not replayed.error);
  REQUIRE_EQUAL(replayed.records.size(), size_t{1});
  CHECK_EQUAL(value(replayed.records.front(), "protocol_identifier"),
              uint64_t{6});
  CHECK(dh.empty());
}

TEST("bounds repeated buffered definitions for checkpointing") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definition = bytes{};
  append_u16(definition, 256);
  append_u16(definition, 1024);
  for (auto index = size_t{0}; index < 1024; ++index) {
    append_u16(definition, 4);
    append_u16(definition, 1);
  }
  auto message = netflow_v9(100, 1, {}, 1, uint16_t{1}, 1'700'000'000);
  append_set(message, Set{0, std::move(definition)});
  for (auto index = size_t{0}; index < 257; ++index) {
    append_set(message, Set{256, {}});
  }
  append_set(message, Set{257, {}});
  auto result = decode(decoder, message, dh);
  REQUIRE(not result.error);
  CHECK(result.records.empty());
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message,
              "discarded NetFlow data sets without templates");
}

TEST("decodes a message-local template after capacity eviction") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definitions = bytes{};
  for (auto id = uint16_t{256}; id <= uint16_t{4352}; ++id) {
    append_u16(definitions, id);
    append_u16(definitions, 1);
    append_u16(definitions, 4);
    append_u16(definitions, 1);
  }
  auto result = decode(
    decoder,
    netflow_v9(100, 1, {{0, std::move(definitions)}, {256, raw({6, 0, 0, 0})}},
               1, uint16_t{4098}),
    dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  CHECK_EQUAL(value(result.records.front(), "protocol_identifier"),
              uint64_t{6});
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message, "evicted inactive NetFlow template");
}

TEST("snapshots templates and buffered data sets") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definition = template_record(256, {{4, 1}});
  REQUIRE(
    not decode(decoder, netflow_v9(100, 1, {{0, definition}}, 1), dh).error);
  auto mixed = decode(
    decoder,
    netflow_v9(101, 1, {{257, raw({192, 0, 2, 1})}, {256, raw({6, 0, 0, 0})}},
               2, uint16_t{2}),
    dh);
  REQUIRE(not mixed.error);
  auto buffer = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{buffer};
  REQUIRE(serializer.begin_object(caf::invalid_type_id, ""));
  auto saving = Serde{serializer};
  decoder.snapshot(saving);
  REQUIRE(serializer.end_object());
  auto restored = Decoder{};
  auto deserializer = caf::binary_deserializer{buffer};
  REQUIRE(deserializer.begin_object(caf::invalid_type_id, ""));
  auto loading = Serde{deserializer};
  restored.snapshot(loading);
  REQUIRE(deserializer.end_object());
  auto buffered_definition = template_record(257, {{8, 4}});
  auto replayed
    = decode(restored, netflow_v9(102, 1, {{0, buffered_definition}}, 3), dh);
  REQUIRE(not replayed.error);
  REQUIRE_EQUAL(replayed.records.size(), size_t{2});
  CHECK_EQUAL(value(replayed.records.front(), "source_ipv4_address"),
              ip::v4(0xc0000201));
  CHECK_EQUAL(value(replayed.records.back(), "protocol_identifier"),
              uint64_t{6});
  auto result
    = decode(restored,
             netflow_v9(103, 1, {{256, raw({17, 0, 0, 0})}}, 4, uint16_t{1}),
             dh);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  CHECK_EQUAL(value(result.records.front(), "protocol_identifier"),
              uint64_t{17});
  CHECK(dh.empty());
}

TEST("frames a restored ambiguous prefix identically") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definition = template_record(256, {{7, 2}});
  REQUIRE(not decode(decoder, netflow_v9(100, 8, {{0, definition}}), dh).error);
  auto message
    = netflow_v9(101, 8, {{256, raw({0, 53, 0, 0})}}, 2, uint16_t{2});
  auto framed = decoder.frame(message, false);
  REQUIRE_EQUAL(framed.status, FrameStatus::ambiguous);
  auto buffer = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{buffer};
  REQUIRE(serializer.begin_object(caf::invalid_type_id, ""));
  auto saving = Serde{serializer};
  decoder.snapshot(saving);
  REQUIRE(serializer.end_object());
  auto restored = Decoder{};
  auto deserializer = caf::binary_deserializer{buffer};
  REQUIRE(deserializer.begin_object(caf::invalid_type_id, ""));
  auto loading = Serde{deserializer};
  restored.snapshot(loading);
  REQUIRE(deserializer.end_object());
  // Restoring drops v9 framing progress but keeps template state, so a
  // restored decoder must reproduce the framing decision for the same bytes.
  auto reframed = restored.frame(message, false);
  CHECK_EQUAL(reframed.status, FrameStatus::ambiguous);
  CHECK_EQUAL(reframed.size, framed.size);
  CHECK(dh.empty());
}

TEST("rejected input does not age buffered data sets") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto const peer = Peer{ip::v4(0xc0000201), 2055};
  auto pending
    = decode(decoder, ipfix(1, {{256, raw({10, 0, 0, 1})}}), dh, peer);
  REQUIRE(not pending.error);
  for (auto index = size_t{0}; index < 1024; ++index) {
    auto rejected = decode(decoder, raw({0, 6}), dh, peer);
    REQUIRE(rejected.error);
  }
  auto malformed_definition = template_record(256, {{8, 4}});
  append_u16(malformed_definition, 257);
  append_u16(malformed_definition, 1);
  auto malformed = ipfix(1, {{2, malformed_definition}}, 2);
  for (auto index = size_t{0}; index < 1024; ++index) {
    auto rejected = decode(decoder, malformed, dh, peer);
    REQUIRE(rejected.error);
  }
  auto definition = template_record(256, {{8, 4}});
  auto replayed = decode(decoder, ipfix(1, {{2, definition}}, 2), dh, peer);
  REQUIRE(not replayed.error);
  REQUIRE_EQUAL(replayed.records.size(), size_t{1});
  CHECK_EQUAL(value(replayed.records.front(), "source_ipv4_address"),
              ip::v4(0x0a000001));
  CHECK(dh.empty());
}

TEST("isolates accepted-message aging by exporter") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto const quiet = Peer{ip::v4(0xc0000201), 2055};
  auto const busy = Peer{ip::v4(0xc0000202), 2055};
  auto pending
    = decode(decoder, ipfix(1, {{256, raw({10, 0, 0, 1})}}), dh, quiet);
  REQUIRE(not pending.error);
  for (auto index = uint32_t{0}; index < 1024; ++index) {
    REQUIRE(not decode(decoder, ipfix(1, {}, index + 1), dh, busy).error);
  }
  auto definition = template_record(256, {{8, 4}});
  auto replayed = decode(decoder, ipfix(1, {{2, definition}}, 2), dh, quiet);
  REQUIRE(not replayed.error);
  REQUIRE_EQUAL(replayed.records.size(), size_t{1});
  CHECK(dh.empty());
}

TEST("retains quiet exporters under unrelated traffic") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto const quiet = Peer{ip::v4(0xc0000201), 2055};
  auto const busy = Peer{ip::v4(0xc0000202), 2055};
  auto definition = template_record(256, {{8, 4}});
  REQUIRE(not decode(decoder, ipfix(1, {{2, definition}}), dh, quiet).error);
  for (auto index = uint32_t{0}; index < 100'000; ++index) {
    REQUIRE(not decode(decoder, ipfix(1, {}, index + 1), dh, busy).error);
  }
  auto result
    = decode(decoder, ipfix(1, {{256, raw({10, 0, 0, 1})}}, 2), dh, quiet);
  REQUIRE(not result.error);
  REQUIRE_EQUAL(result.records.size(), size_t{1});
  CHECK_EQUAL(value(result.records.front(), "source_ipv4_address"),
              ip::v4(0x0a000001));
  CHECK(dh.empty());
}

TEST("bounds exporter state") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definition = template_record(256, {{4, 1}});
  for (auto port = uint16_t{0}; port < uint16_t{1025}; ++port) {
    auto peer = Peer{ip::v4(0xc0000201), port};
    REQUIRE(not decode(decoder, ipfix(1, {{2, definition}}), dh, peer).error);
  }
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message,
              "evicted inactive NetFlow exporter state");
  REQUIRE_EQUAL(diagnostics.front().notes.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().notes.front().message,
              "the decoder retains at most 1024 exporters");
  auto retained_dh = collecting_diagnostic_handler{};
  auto const retained_peer = Peer{ip::v4(0xc0000201), 1024};
  auto retained = decode(decoder, ipfix(1, {{256, raw({6})}}, 2), retained_dh,
                         retained_peer);
  REQUIRE(not retained.error);
  REQUIRE_EQUAL(retained.records.size(), size_t{1});
  auto const evicted_peer = Peer{ip::v4(0xc0000201), 0};
  auto evicted = decode(decoder, ipfix(1, {{256, raw({6})}}, 2), retained_dh,
                        evicted_peer);
  REQUIRE(not evicted.error);
  CHECK(evicted.records.empty());
  auto followup_diagnostics = std::move(retained_dh).collect();
  REQUIRE_EQUAL(followup_diagnostics.size(), size_t{1});
  CHECK_EQUAL(followup_diagnostics.front().message,
              "evicted inactive NetFlow exporter state");
}

TEST("bounds template state per exporter") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definitions = bytes{};
  for (auto id = uint16_t{256}; id <= uint16_t{4352}; ++id) {
    auto record = template_record(id, {{4, 1}});
    definitions.insert(definitions.end(), record.begin(), record.end());
  }
  REQUIRE(not decode(decoder, ipfix(2, {{2, definitions}}), dh).error);
  auto retained = decode(decoder, ipfix(2, {{4352, raw({6})}}, 2), dh);
  REQUIRE(not retained.error);
  REQUIRE_EQUAL(retained.records.size(), size_t{1});
  auto evicted = decode(decoder, ipfix(2, {{256, raw({6})}}, 3), dh);
  REQUIRE(not evicted.error);
  CHECK(evicted.records.empty());
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message, "evicted inactive NetFlow template");
  REQUIRE_EQUAL(diagnostics.front().notes.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().notes.front().message,
              "template 256 was evicted; each exporter retains at most 4096 "
              "templates");
}

TEST("bounds template state across exporters") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto definitions = bytes{};
  for (auto id = uint16_t{256}; id < uint16_t{4352}; ++id) {
    auto record = template_record(id, {{4, 1}});
    definitions.insert(definitions.end(), record.begin(), record.end());
  }
  for (auto port = uint16_t{1}; port <= uint16_t{4}; ++port) {
    auto peer = Peer{ip::v4(0xc0000202), port};
    REQUIRE(not decode(decoder, ipfix(2, {{2, definitions}}), dh, peer).error);
  }
  auto definition = template_record(256, {{4, 1}});
  auto const newest_peer = Peer{ip::v4(0xc0000202), 5};
  REQUIRE(
    not decode(decoder, ipfix(2, {{2, definition}}), dh, newest_peer).error);
  auto retained
    = decode(decoder, ipfix(2, {{256, raw({6})}}, 2), dh, newest_peer);
  REQUIRE(not retained.error);
  REQUIRE_EQUAL(retained.records.size(), size_t{1});
  auto const oldest_peer = Peer{ip::v4(0xc0000202), 1};
  auto evicted
    = decode(decoder, ipfix(2, {{256, raw({6})}}, 2), dh, oldest_peer);
  REQUIRE(not evicted.error);
  CHECK(evicted.records.empty());
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message,
              "evicted inactive NetFlow template state");
  REQUIRE_EQUAL(diagnostics.front().notes.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().notes.front().message,
              "evicted 1 template; the decoder retains at most 16384 "
              "templates and 262144 template fields");
}

TEST("bounds buffered data sets") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  auto const first = Peer{ip::v4(0xc0000201), 1000};
  auto const second = Peer{ip::v4(0xc0000201), 2000};
  for (auto index = size_t{0}; index < 1025; ++index) {
    auto const& peer = index % 2 == 0 ? first : second;
    auto const sequence_number = static_cast<uint32_t>(index / 2 + 1);
    REQUIRE(not decode(decoder, ipfix(3, {{256, raw({1})}}, sequence_number),
                       dh, peer)
                  .error);
  }
  auto definition = template_record(256, {{4, 1}});
  auto first_replayed
    = decode(decoder, ipfix(3, {{2, definition}}, 514), dh, first);
  REQUIRE(not first_replayed.error);
  REQUIRE_EQUAL(first_replayed.records.size(), size_t{512});
  auto second_replayed
    = decode(decoder, ipfix(3, {{2, definition}}, 513), dh, second);
  REQUIRE(not second_replayed.error);
  REQUIRE_EQUAL(second_replayed.records.size(), size_t{512});
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message, "evicted buffered NetFlow data set");
  REQUIRE_EQUAL(diagnostics.front().notes.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().notes.front().message,
              "template 256 data was evicted before its template arrived");
}

TEST("expires buffered data sets by accepted message count") {
  auto decoder = Decoder{};
  auto dh = collecting_diagnostic_handler{};
  REQUIRE(not decode(decoder, ipfix(4, {{256, raw({1})}}), dh).error);
  for (auto index = uint32_t{0}; index < 1024; ++index) {
    REQUIRE(not decode(decoder, ipfix(4, {}, index + 2), dh).error);
  }
  auto definition = template_record(256, {{4, 1}});
  auto replayed = decode(decoder, ipfix(4, {{2, definition}}, 1026), dh);
  REQUIRE(not replayed.error);
  CHECK(replayed.records.empty());
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().message,
              "discarded buffered NetFlow data set");
  REQUIRE_EQUAL(diagnostics.front().notes.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().notes.front().message,
              "template 256 did not arrive before the data set expired");
}

} // namespace tenzir::netflow
