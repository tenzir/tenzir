//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/entity_id.hpp"

#include "tenzir/tql2/registry.hpp"

namespace tenzir::tql2 {

auto entity_id::debug_inspect(debug_writer& dbg) const -> bool {
  if (not resolved()) {
    return dbg.fmt_value("<unresolved>");
  }
  auto reg = thread_local_registry();
  if (not reg) {
    return dbg.apply(id);
  }
  return reg->get(*this).match(
    [&](const std::unique_ptr<operator_def>& op) {
      return dbg.fmt_value("<{}> (0)", op->name(), id);
    },
    [&](const function_def& def) {
      return dbg.apply(def.test);
    });
}

} // namespace tenzir::tql2
