//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/core/rule.hpp"
#include "vast/concept/parseable/numeric/bool.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/numeric/real.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/concept/parseable/string/quoted_string.hpp"
#include "vast/detail/narrow.hpp"

namespace vast {

namespace parsers {

// clang-format off
static auto const json_boolean
  = parsers::boolean;
static auto const json_int
  = parsers::i64;
static auto const json_count
  = (parsers::hex_prefix >> parsers::hex64)
  | parsers::u64;
static auto const json_number
  = (parsers::hex_prefix >> parsers::hex64 ->* [](uint64_t x) {
        return detail::narrow_cast<double>(x); })
  | parsers::real;
// clang-format on

} // namespace parsers
} // namespace vast
