//    _   _____   __________
//   | | / / _ | / __/_  __/    Visibility
//   | |/ / __ |_\ \  / /        Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

// This file provides a compatibility layer for Boost UUID generators
// that were introduced in Boost 1.86. It can be removed once we
// upgrade our minimum Boost version to 1.86 or later.
//
// To remove this patch:
// 1. Delete this file
// 2. Remove the #include <tenzir/detail/boost_uuid_generators.hpp> from
//    uuid.cpp
// 3. Remove the #if BOOST_VERSION < 108600 guard around the include
//
// For testing on systems with Boost >= 1.86:
// Option 1: Compile with -DTENZIR_FORCE_BOOST_UUID_COMPAT
//   cmake --build build --target tenzir --
//   CXXFLAGS="-DTENZIR_FORCE_BOOST_UUID_COMPAT"
//
// Option 2: Add to CMake configuration:
//   cmake -DCMAKE_CXX_FLAGS="-DTENZIR_FORCE_BOOST_UUID_COMPAT" ..
//
// This will use the compatibility implementations in the tenzir::detail
// namespace to avoid conflicts with the real Boost 1.86+ implementations.

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <random>

namespace tenzir::detail {

// Minimal uuid_clock implementation
class uuid_clock {
public:
  using rep = std::int64_t;
  using period = std::ratio<1, 10000000>; // 100ns
  using duration = std::chrono::duration<rep, period>;
  using time_point = std::chrono::time_point<uuid_clock, duration>;

  static auto now() noexcept -> time_point {
    using days = std::chrono::duration<std::int32_t, std::ratio<86400>>;
    // Days between 1582-10-15 and 1970-01-01
    constexpr auto epoch_diff = days(141427);
    auto sys_now = std::chrono::system_clock::now();
    auto sys_duration = sys_now.time_since_epoch();
    auto uuid_duration
      = std::chrono::duration_cast<duration>(sys_duration) + epoch_diff;
    return time_point(uuid_duration);
  }

  static auto to_timestamp(time_point const& tp) noexcept {
    return static_cast<std::uint64_t>(tp.time_since_epoch().count());
  }
};

// time_generator_v1
class time_generator_v1 {
public:
  struct state_type {
    std::uint64_t timestamp;
    std::uint16_t clock_seq;
  };

  using result_type = boost::uuids::uuid;

  time_generator_v1() {
    // Generate random node identifier
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<std::uint8_t> dist(0, 255);
    for (size_t i = 0; i < 6; ++i) {
      node_[i] = dist(gen);
    }
    node_[0] |= 0x01; // mark as multicast
    std::uniform_int_distribution<std::uint16_t> clock_dist(0, 0x3FFF);
    state_.clock_seq = clock_dist(gen);
    state_.timestamp = 0;
  }

  auto operator()() noexcept -> result_type {
    state_ = get_new_state(state_);
    boost::uuids::uuid result;
    std::uint32_t time_low = static_cast<std::uint32_t>(state_.timestamp);
    result.data[0] = (time_low >> 24) & 0xFF;
    result.data[1] = (time_low >> 16) & 0xFF;
    result.data[2] = (time_low >> 8) & 0xFF;
    result.data[3] = time_low & 0xFF;
    std::uint16_t time_mid = static_cast<std::uint16_t>(state_.timestamp >> 32);
    result.data[4] = (time_mid >> 8) & 0xFF;
    result.data[5] = time_mid & 0xFF;
    std::uint16_t time_hi = static_cast<std::uint16_t>(state_.timestamp >> 48);
    time_hi = (time_hi & 0x0FFF) | 0x1000; // Version 1
    result.data[6] = (time_hi >> 8) & 0xFF;
    result.data[7] = time_hi & 0xFF;
    result.data[8] = ((state_.clock_seq >> 8) & 0x3F) | 0x80;
    result.data[9] = state_.clock_seq & 0xFF;
    std::memcpy(result.data + 10, node_.data(), 6);
    return result;
  }

private:
  std::array<std::uint8_t, 6> node_;
  state_type state_;

  static auto get_new_state(state_type const& oldst) noexcept -> state_type {
    state_type newst(oldst);
    std::uint64_t timestamp = uuid_clock::to_timestamp(uuid_clock::now());
    if (timestamp <= newst.timestamp) {
      newst.clock_seq = (newst.clock_seq + 1) & 0x3FFF;
    }
    newst.timestamp = timestamp;
    return newst;
  }
};

// time_generator_v6 - reorders v1 fields for better database performance
class time_generator_v6 : public time_generator_v1 {
public:
  using result_type = boost::uuids::uuid;
  using time_generator_v1::time_generator_v1;

  auto operator()() noexcept -> result_type {
    boost::uuids::uuid result = time_generator_v1::operator()();
    std::uint32_t time_low = (result.data[0] << 24) | (result.data[1] << 16)
                             | (result.data[2] << 8) | result.data[3];
    std::uint16_t time_mid = (result.data[4] << 8) | result.data[5];
    std::uint16_t time_hi = ((result.data[6] << 8) | result.data[7]) & 0x0FFF;
    std::uint64_t timestamp = time_low
                              | (static_cast<std::uint64_t>(time_mid) << 32)
                              | (static_cast<std::uint64_t>(time_hi) << 48);
    std::uint32_t time_high = static_cast<std::uint32_t>(timestamp >> 28);
    result.data[0] = (time_high >> 24) & 0xFF;
    result.data[1] = (time_high >> 16) & 0xFF;
    result.data[2] = (time_high >> 8) & 0xFF;
    result.data[3] = time_high & 0xFF;
    time_mid = static_cast<std::uint16_t>(timestamp >> 12);
    result.data[4] = (time_mid >> 8) & 0xFF;
    result.data[5] = time_mid & 0xFF;
    std::uint16_t time_low_and_version
      = (static_cast<std::uint16_t>(timestamp & 0xFFF)) | 0x6000;
    result.data[6] = (time_low_and_version >> 8) & 0xFF;
    result.data[7] = time_low_and_version & 0xFF;
    return result;
  }
};

// time_generator_v7 - Unix timestamp with millisecond precision
class time_generator_v7 {
public:
  using result_type = boost::uuids::uuid;

  time_generator_v7() : state_(0), rng_(std::random_device{}()) {
  }

  auto operator()() noexcept -> result_type {
    boost::uuids::uuid result;
    state_ = get_new_state(state_);
    std::uint64_t time_ms = state_ >> 16;
    std::uint64_t timestamp
      = (time_ms << 16) | 0x7000 | ((state_ & 0xFFFF) >> 6);
    result.data[0] = (timestamp >> 56) & 0xFF;
    result.data[1] = (timestamp >> 48) & 0xFF;
    result.data[2] = (timestamp >> 40) & 0xFF;
    result.data[3] = (timestamp >> 32) & 0xFF;
    result.data[4] = (timestamp >> 24) & 0xFF;
    result.data[5] = (timestamp >> 16) & 0xFF;
    result.data[6] = (timestamp >> 8) & 0xFF;
    result.data[7] = timestamp & 0xFF;
    result.data[8] = 0x80 | (state_ & 0x3F);
    std::uniform_int_distribution<std::uint8_t> dist(0, 255);
    for (int i = 9; i < 16; ++i) {
      result.data[i] = dist(rng_);
    }
    return result;
  }

private:
  std::uint64_t state_;
  std::mt19937 rng_;

  static auto get_new_state(std::uint64_t const& oldst) noexcept
    -> std::uint64_t {
    auto now = std::chrono::system_clock::now();
    auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto now_us = std::chrono::time_point_cast<std::chrono::microseconds>(now);
    std::uint64_t time_ms = now_ms.time_since_epoch().count();
    std::uint64_t time_us = (now_us.time_since_epoch().count()) % 1000;
    std::uint64_t newst = (time_ms << 16) | (time_us << 6);
    if (newst > oldst) {
      return newst;
    }
    if (time_ms < (oldst >> 16)) {
      return newst;
    }
    return oldst + 1;
  }
};

} // namespace tenzir::detail

namespace boost::uuids {
using tenzir::detail::time_generator_v1;
using tenzir::detail::time_generator_v6;
using tenzir::detail::time_generator_v7;
} // namespace boost::uuids
