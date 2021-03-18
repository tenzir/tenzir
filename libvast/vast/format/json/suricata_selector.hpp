// SPDX-FileCopyrightText: (c) 2021 Tenzir GmbH <info@tenzir.com>
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
