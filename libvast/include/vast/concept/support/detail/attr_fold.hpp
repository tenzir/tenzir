//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

namespace vast {

struct unused_type;

namespace detail {

template <class Attribute>
struct attr_fold : std::decay<Attribute> {};

template <>
struct attr_fold<std::vector<char>> : std::decay<std::string> {};

template <>
struct attr_fold<unused_type> : std::decay<unused_type> {};

template <>
struct attr_fold<std::vector<unused_type>> : std::decay<unused_type> {};

template <>
struct attr_fold<std::tuple<char, char>> : std::decay<std::string> {};

template <>
struct attr_fold<std::tuple<char, std::string>> : std::decay<std::string> {};

template <>
struct attr_fold<std::tuple<std::string, char>> : std::decay<std::string> {};

template <class T>
using attr_fold_t = typename attr_fold<T>::type;

} // namespace detail
} // namespace vast
