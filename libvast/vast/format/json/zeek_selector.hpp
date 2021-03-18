// SPDX-FileCopyrightText: (c) 2021 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/format/json/field_selector.hpp"

namespace vast::format::json {

struct zeek_selector_specification {
  static constexpr auto field = "_path";
  static constexpr auto prefix = "zeek";
};

using zeek_selector = field_selector<zeek_selector_specification>;

} // namespace vast::format::json
