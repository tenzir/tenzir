//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <utility>

namespace vast::detail {

/// Creates a set of overloaded functions. This utility struct allows for
/// writing inline visitors without having to result to inversion of control.
template <class... Ts>
struct overload : Ts... {
  using Ts::operator()...;
};

} // namespace vast::detail
