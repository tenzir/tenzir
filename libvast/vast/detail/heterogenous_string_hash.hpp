//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 Tenzir GmbH

#pragma once

#include <string>
#include <unordered_map>

namespace vast::detail {

struct heterogenous_string_hash {
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

/// A map from std::string to `Value` allowing for C++20 heterogenous
/// lookups.
template <typename Value>
using heterogenous_string_hashmap
  = std::unordered_map<std::string, Value, heterogenous_string_hash,
                       std::equal_to<>>;

} // namespace vast::detail
