//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/choice.hpp"
#include "vast/concept/parseable/core/operators.hpp"
#include "vast/concept/parseable/core/plus.hpp"
#include "vast/concept/parseable/string/char.hpp"
#include "vast/concept/parseable/string/char_class.hpp"

namespace vast::parsers {

constexpr inline auto identifier_char = (alnum | ch<'_'> | ch<'.'>);
constexpr inline auto identifier = +identifier_char;

constexpr inline auto plugin_name_char = alnum | chr{'-'} | chr{'_'};
constexpr inline auto plugin_name = +plugin_name_char;

} // namespace vast::parsers
