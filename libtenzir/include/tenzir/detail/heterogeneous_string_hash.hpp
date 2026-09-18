//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2022 Tenzir GmbH

#pragma once

#include <tsl/robin_map.h>
#include <tsl/robin_set.h>

#include <string>
#include <string_view>
#include <unordered_map>

namespace tenzir::detail {

struct heterogeneous_string_equal {
  using is_transparent = void;

  template <class LhsAllocator, class RhsAllocator>
  auto operator()(
    const std::basic_string<char, std::char_traits<char>, LhsAllocator>& lhs,
    const std::basic_string<char, std::char_traits<char>, RhsAllocator>& rhs)
    const noexcept -> bool {
    return std::string_view{lhs} == std::string_view{rhs};
  }

  template <class Allocator>
  auto operator()(
    const std::basic_string<char, std::char_traits<char>, Allocator>& lhs,
    std::string_view rhs) const noexcept -> bool {
    return std::string_view{lhs} == rhs;
  }

  template <class Allocator>
  auto
  operator()(std::string_view lhs,
             const std::basic_string<char, std::char_traits<char>, Allocator>&
               rhs) const noexcept -> bool {
    return lhs == std::string_view{rhs};
  }

  template <class Allocator>
  auto operator()(
    const std::basic_string<char, std::char_traits<char>, Allocator>& lhs,
    const char* rhs) const noexcept -> bool {
    return std::string_view{lhs} == rhs;
  }
};

struct heterogeneous_string_hash {
  using is_transparent = void;

  [[nodiscard]] auto operator()(const char* s) const noexcept -> size_t {
    return std::hash<std::string_view>{}(s);
  }

  [[nodiscard]] auto operator()(std::string_view s) const noexcept -> size_t {
    return std::hash<std::string_view>{}(s);
  }

  template <class Allocator>
  [[nodiscard]] auto
  operator()(const std::basic_string<char, std::char_traits<char>, Allocator>&
               s) const noexcept -> size_t {
    return std::hash<std::string_view>{}(s);
  }
};

/// A map from std::string to `Value` allowing for heterogeneous lookups.
//  Note that we can't use the C++20 heterogeneous unordered_map yet,
//  because we still want to support GCC 10 on Debian. (and of course,
//  there's a good chance that robin_map would be faster anyways)
template <typename Value>
using heterogeneous_string_hashmap
  = tsl::robin_map<std::string, Value, heterogeneous_string_hash,
                   tenzir::detail::heterogeneous_string_equal>;

/// A set of `std::string`s allowing for heterogeneous lookups.
using heterogeneous_string_hashset
  = tsl::robin_set<std::string, heterogeneous_string_hash,
                   tenzir::detail::heterogeneous_string_equal>;

} // namespace tenzir::detail
