//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <type_traits>
#include <utility>

namespace vast {

template <class From, class To, class... Opts>
concept convertible = requires(From from, To to, Opts&&... opts) {
  convert(from, to, std::forward<Opts>(opts)...);
};

} // namespace vast
