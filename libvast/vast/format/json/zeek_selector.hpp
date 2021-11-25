//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

namespace vast::format::json {

struct zeek_selector {
  static constexpr auto field_name = "_path";
  static constexpr auto type_prefix = "zeek";
};

} // namespace vast::format::json
