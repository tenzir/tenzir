//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pcapng.hpp"

#include "tenzir/test/test.hpp"

#include <limits>

using namespace tenzir;

TEST("PCAPNG block padding") {
  CHECK_EQUAL(pcapng::padded_size(0), 0u);
  CHECK_EQUAL(pcapng::padded_size(1), 4u);
  CHECK_EQUAL(pcapng::padded_size(3), 4u);
  CHECK_EQUAL(pcapng::padded_size(4), 4u);
  CHECK_EQUAL(pcapng::padded_size(5), 8u);
  CHECK_EQUAL(pcapng::padded_size(std::numeric_limits<uint32_t>::max()),
              uint64_t{1} << 32);
}

TEST("decode PCAPNG timestamps") {
  auto expected = tenzir::time{duration{1'500'000'000}};
  CHECK_EQUAL(pcapng::decode_timestamp(1'500'000, {}), expected);
  CHECK_EQUAL(pcapng::decode_timestamp(
                1'500'000'000,
                {.resolution = pcapng::nanosecond_timestamp_resolution}),
              expected);
  CHECK_EQUAL(pcapng::decode_timestamp(
                1'536,
                {.resolution = pcapng::binary_resolution_flag | uint8_t{10}}),
              expected);
  CHECK_EQUAL(pcapng::decode_timestamp(1'500'000, {.offset_seconds = 2}),
              tenzir::time{duration{3'500'000'000}});
  CHECK_EQUAL(pcapng::decode_timestamp(
                std::numeric_limits<uint64_t>::max(),
                {.resolution = pcapng::nanosecond_timestamp_resolution}),
              None{});
}

TEST("encode PCAPNG timestamps") {
  auto input = tenzir::time{duration{1'500'000'001}};
  CHECK_EQUAL(pcapng::encode_timestamp(input, {}), uint64_t{1'500'000});
  CHECK_EQUAL(pcapng::encode_timestamp(
                input, {.resolution = pcapng::nanosecond_timestamp_resolution}),
              uint64_t{1'500'000'001});
  CHECK_EQUAL(pcapng::encode_timestamp(
                tenzir::time{duration{1'500'000'000}},
                {.resolution = pcapng::binary_resolution_flag | uint8_t{10}}),
              uint64_t{1'536});
  CHECK_EQUAL(pcapng::encode_timestamp(tenzir::time{duration{3'500'000'000}},
                                       {.offset_seconds = 2}),
              uint64_t{1'500'000});
  CHECK_EQUAL(pcapng::encode_timestamp(tenzir::time{duration{-1}}, {}), None{});
  CHECK_EQUAL(pcapng::encode_timestamp(
                tenzir::time::max(),
                {.resolution = pcapng::binary_resolution_flag | uint8_t{127},
                 .offset_seconds = std::numeric_limits<int64_t>::min()}),
              None{});
}
