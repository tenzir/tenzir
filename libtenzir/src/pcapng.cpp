//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pcapng.hpp"

#include "tenzir/checked_math.hpp"

#include <limits>

namespace tenzir::pcapng {

namespace {

constexpr auto nanoseconds_per_second = uint64_t{1'000'000'000};

} // namespace

auto decode_timestamp(uint64_t value, TimestampFormat format) -> Option<time> {
  auto nanoseconds = __int128{};
  auto exponent
    = static_cast<uint8_t>(format.resolution & resolution_exponent_mask);
  if ((format.resolution & binary_resolution_flag) == 0) {
    if (exponent <= nanosecond_timestamp_resolution) {
      auto factor = checked_pow(
        uint64_t{10},
        static_cast<uint8_t>(nanosecond_timestamp_resolution - exponent));
      if (not factor) {
        return None{};
      }
      nanoseconds = static_cast<__int128>(value) * *factor;
    } else {
      auto divisor = checked_pow(
        uint64_t{10},
        static_cast<uint8_t>(exponent - nanosecond_timestamp_resolution));
      nanoseconds = divisor ? value / *divisor : 0;
    }
  } else {
    auto numerator = static_cast<__uint128_t>(value) * nanoseconds_per_second;
    auto denominator = __uint128_t{1} << exponent;
    nanoseconds = static_cast<__int128>(numerator / denominator);
  }
  nanoseconds
    += static_cast<__int128>(format.offset_seconds) * nanoseconds_per_second;
  if (nanoseconds < std::numeric_limits<int64_t>::min()
      or nanoseconds > std::numeric_limits<int64_t>::max()) {
    return None{};
  }
  return time{duration{static_cast<int64_t>(nanoseconds)}};
}

auto encode_timestamp(time value, TimestampFormat format) -> Option<uint64_t> {
  auto nanoseconds
    = static_cast<__int128>(value.time_since_epoch().count())
      - static_cast<__int128>(format.offset_seconds) * nanoseconds_per_second;
  if (nanoseconds < 0) {
    return None{};
  }
  auto input = static_cast<__uint128_t>(nanoseconds);
  auto max = static_cast<__uint128_t>(std::numeric_limits<uint64_t>::max());
  auto exponent
    = static_cast<uint8_t>(format.resolution & resolution_exponent_mask);
  auto raw = __uint128_t{};
  if ((format.resolution & binary_resolution_flag) == 0) {
    if (exponent <= nanosecond_timestamp_resolution) {
      auto divisor = checked_pow(
        uint64_t{10},
        static_cast<uint8_t>(nanosecond_timestamp_resolution - exponent));
      if (not divisor) {
        return None{};
      }
      raw = input / *divisor;
    } else {
      auto factor = checked_pow(
        uint64_t{10},
        static_cast<uint8_t>(exponent - nanosecond_timestamp_resolution));
      if (not factor) {
        return input == 0 ? Option<uint64_t>{0} : None{};
      }
      if (input != 0 and *factor > max / input) {
        return None{};
      }
      raw = input * *factor;
    }
  } else {
    auto scale = __uint128_t{1} << exponent;
    auto limit = (max + 1) * nanoseconds_per_second;
    if (input != 0 and scale > (limit - 1) / input) {
      return None{};
    }
    raw = input * scale / nanoseconds_per_second;
  }
  if (raw > max) {
    return None{};
  }
  return static_cast<uint64_t>(raw);
}

} // namespace tenzir::pcapng
