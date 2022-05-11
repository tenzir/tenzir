//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 Tenzir GmbH

#pragma once

#include <tsl/robin_map.h>
#include <tsl/robin_set.h>

#include <string>
#include <string_view>
#include <unordered_map>

namespace vast::detail {

struct heterogeneous_string_equal {
  using is_transparent = void;

  bool operator()(const std::string& lhs, const std::string& rhs) const {
    return lhs == rhs;
  }

  bool operator()(const std::string& lhs, std::string_view sv) const {
    return lhs == sv;
  }

  bool operator()(const std::string& lhs, const char* sv) const {
    return lhs == sv;
  }
};

struct heterogeneous_string_hash {
  using is_transparent = void;

  [[nodiscard]] size_t operator()(const char* s) const {
    return std::hash<std::string_view>{}(s);
  }

  [[nodiscard]] size_t operator()(std::string_view s) const {
    return std::hash<std::string_view>{}(s);
  }

  [[nodiscard]] size_t operator()(const std::string& s) const {
    return std::hash<std::string>{}(s);
  }
};

/// A map from std::string to `Value` allowing for heterogeneous lookups.
//  Note that we can't use the C++20 heterogeneous unordered_map yet,
//  because we still want to support GCC 10 on Debian. (and of course,
//  there's a good chance that robin_map would be faster anyways)
template <typename Value>
using heterogeneous_string_hashmap
  = tsl::robin_map<std::string, Value, heterogeneous_string_hash,
                   vast::detail::heterogeneous_string_equal>;

/// A set of `std::string`s allowing for heterogeneous lookups.
using heterogeneous_string_hashset
  = tsl::robin_set<std::string, heterogeneous_string_hash,
                   vast::detail::heterogeneous_string_equal>;

} // namespace vast::detail
