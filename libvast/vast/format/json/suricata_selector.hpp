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

struct suricata_selector_specification {
  static constexpr auto field = "event_type";
  static constexpr auto prefix = "suricata";
};

using suricata_selector = field_selector<suricata_selector_specification>;

} // namespace vast::format::json
