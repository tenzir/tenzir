//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <cstdint>
#include <optional>
#include <string_view>

namespace tenzir {

auto ocsf_class_name(int64_t id) -> std::optional<std::string_view>;
auto ocsf_class_uid(std::string_view name) -> std::optional<int64_t>;
auto ocsf_category_name(int64_t id) -> std::optional<std::string_view>;
auto ocsf_category_uid(std::string_view name) -> std::optional<int64_t>;

} // namespace tenzir
