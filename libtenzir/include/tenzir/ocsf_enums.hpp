//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <boost/unordered/unordered_flat_map.hpp>

namespace tenzir {

auto get_ocsf_int_to_string(std::string_view enum_id)
  -> std::optional<std::reference_wrapper<
    const boost::unordered_flat_map<int64_t, std::string_view>>>;

auto get_ocsf_string_to_int(std::string_view enum_id)
  -> std::optional<std::reference_wrapper<
    const boost::unordered_flat_map<std::string_view, int64_t>>>;

} // namespace tenzir
