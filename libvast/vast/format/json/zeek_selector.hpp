//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/format/json/field_selector.hpp"

namespace vast::format::json {

struct zeek_selector_specification {
  // static constexpr auto category = defaults::import::zeek_json::category;
  static constexpr auto name = "zeek-reader";
  static constexpr auto field = "_path";
  static constexpr auto prefix = "zeek";
};

using zeek_selector = field_selector<zeek_selector_specification>;

} // namespace vast::format::json
