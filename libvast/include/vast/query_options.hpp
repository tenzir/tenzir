//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include <cstdint>

namespace vast {

/// Stores query options.
enum class query_options : uint32_t {
  none = 0x00,
  historical = 0x01,
  continuous = 0x02,
  preserve_ids = 0x04,
  low_priority = 0x08
};

/// Concatenates two query options.
constexpr query_options
operator+(const query_options& lhs, const query_options& rhs) {
  return static_cast<query_options>(static_cast<uint32_t>(lhs)
                                    | static_cast<uint32_t>(rhs));
}

constexpr query_options no_query_options = query_options::none;
constexpr query_options historical = query_options::historical;
constexpr query_options continuous = query_options::continuous;
constexpr query_options unified = historical + continuous;
constexpr query_options preserve_ids = query_options::preserve_ids;
constexpr query_options low_priority = query_options::low_priority;

constexpr bool has_query_option(query_options haystack, query_options needle) {
  return (static_cast<uint32_t>(haystack) & static_cast<uint32_t>(needle)) != 0;
}

constexpr bool has_historical_option(query_options opts) {
  return has_query_option(opts, historical);
}

constexpr bool has_continuous_option(query_options opts) {
  return has_query_option(opts, continuous);
}

constexpr bool has_unified_option(query_options opts) {
  return has_query_option(opts, historical)
         && has_query_option(opts, continuous);
}

constexpr bool has_preserve_ids_option(query_options opts) {
  return has_query_option(opts, preserve_ids);
}

constexpr bool has_low_priority_option(query_options opts) {
  return has_query_option(opts, low_priority);
}

} // namespace vast
