//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/operators.hpp"
#include "tenzir/concept/parseable/string/char.hpp"
#include "tenzir/concept/parseable/string/char_class.hpp"

namespace tenzir::parsers {

namespace detail {

const inline auto key = *(printable - '=');
const inline auto value = *(printable - ',');

} // namespace detail

const inline auto kvp = detail::key >> '=' >> detail::value;
const inline auto kvp_list = kvp % ',';

} // namespace tenzir::parsers
