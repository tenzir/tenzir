//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/entity_id.hpp"

namespace tenzir::tql2 {

auto entity_id::debug_inspect(debug_writer& dbg) const -> bool {
  if (not resolved()) {
    return dbg.fmt_value("<unresolved>");
  }
  return dbg.apply(id);
}

} // namespace tenzir::tql2
