//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/choice.hpp"
#include "tenzir/concept/parseable/core/operators.hpp"
#include "tenzir/concept/parseable/core/plus.hpp"
#include "tenzir/concept/parseable/string/char.hpp"
#include "tenzir/concept/parseable/string/char_class.hpp"

namespace tenzir::parsers {

constexpr inline auto identifier_char = (alnum | ch<'_'> | ch<'.'>);
constexpr inline auto identifier = +identifier_char;

constexpr inline auto plugin_name_char = alnum | chr{'-'} | chr{'_'};
constexpr inline auto plugin_name = +plugin_name_char;

} // namespace tenzir::parsers
