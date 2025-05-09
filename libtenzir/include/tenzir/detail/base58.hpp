//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/expected.hpp>

#include <string>
#include <string_view>

namespace tenzir::detail::base58 {

auto encode(const std::string_view input) -> std::string;

auto decode(const std::string_view input) -> caf::expected<std::string>;

} // namespace tenzir::detail::base58
