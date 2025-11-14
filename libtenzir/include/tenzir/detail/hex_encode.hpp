//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace tenzir::detail::hex {

auto encode(const std::string_view input) -> std::string;

auto decode(const std::string_view input) -> std::optional<std::string>;

} // namespace tenzir::detail::hex
