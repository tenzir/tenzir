//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 Tenzir GmbH

#pragma once

#include "vast/type.hpp"
#include "vast/view.hpp"

#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

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

  bool operator()(std::string_view lhs, const std::string& rhs) const {
    return lhs == rhs;
  }

  bool operator()(const char* lhs, const std::string& rhs) const {
    return lhs == rhs;
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

template <type_or_concrete_type Type = type>
struct heterogeneous_data_hash {
  using is_transparent = void;

  [[nodiscard]] size_t operator()(view<type_to_data_t<Type>> value) const {
    return hash(value);
  }

  [[nodiscard]] size_t operator()(const type_to_data_t<Type>& value) const
    requires(!std::is_same_v<view<type_to_data_t<Type>>, type_to_data_t<Type>>)
  {
    return hash(make_view(value));
  }
};

template <type_or_concrete_type Type = type>
struct heterogeneous_data_equal {
  using is_transparent = void;

  [[nodiscard]] bool operator()(const type_to_data_t<Type>& lhs,
                                const type_to_data_t<Type>& rhs) const {
    return lhs == rhs;
  }

  [[nodiscard]] bool operator()(const type_to_data_t<Type>& lhs,
                                view<type_to_data_t<Type>> rhs) const
    requires(!std::is_same_v<view<type_to_data_t<Type>>, type_to_data_t<Type>>)
  {
    return make_view(lhs) == rhs;
  }

  [[nodiscard]] bool operator()(view<type_to_data_t<Type>> lhs,
                                const type_to_data_t<Type>& rhs) const
    requires(!std::is_same_v<view<type_to_data_t<Type>>, type_to_data_t<Type>>)
  {
    return lhs == make_view(rhs);
  }
};

/// A map from std::string to `Value` allowing for heterogeneous lookups.
template <class Value>
using heterogeneous_string_hashmap
  = boost::unordered_flat_map<std::string, Value, heterogeneous_string_hash,
                              heterogeneous_string_equal>;

/// A set of `std::string` allowing for heterogeneous lookups.
using heterogeneous_string_hashset
  = boost::unordered_flat_set<std::string, heterogeneous_string_hash,
                              heterogeneous_string_equal>;

/// A map from data to `Value` allowing for heterogeneous lookups.
template <class Value, type_or_concrete_type KeyType = type>
using heterogeneous_data_hashmap
  = boost::unordered_flat_map<type_to_data_t<KeyType>, Value,
                              heterogeneous_data_hash<KeyType>,
                              heterogeneous_data_equal<KeyType>>;

/// A set of `data` allowing for heterogeneous lookups.
template <type_or_concrete_type Type = type>
using heterogeneous_data_hashset
  = boost::unordered_flat_set<type_to_data_t<Type>,
                              heterogeneous_data_hash<Type>,
                              heterogeneous_data_equal<Type>>;

} // namespace vast::detail
