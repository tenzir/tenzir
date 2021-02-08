/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

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
        return detail::narrow_cast<vast::real>(x); })
  | parsers::real_opt_dot;
// clang-format on

} // namespace parsers
} // namespace vast
