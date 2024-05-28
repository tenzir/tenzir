//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include "tenzir/series_builder.hpp"

namespace tenzir::tql2 {

auto aggregation_function_plugin::eval(invocation inv,
                                       diagnostic_handler& dh) const -> series {
  if (inv.args.size() != 1) {
    diagnostic::error("function `{}` expects exactly one argument", name())
      .primary(inv.self.get_location())
      .emit(dh);
    return series::null(null_type{}, inv.length);
  }
  auto& arg = inv.args[0];
  auto list = dynamic_cast<arrow::ListArray*>(&*arg.array);
  if (not list) {
    diagnostic::warning("function `{}` expects a list, got {}", name(),
                        arg.type.kind())
      .primary(inv.self.args[0].get_location())
      .emit(dh);
    return series::null(null_type{}, inv.length);
  }
  auto inner_type = caf::get<list_type>(arg.type).value_type();
  auto b = series_builder{};
  for (auto i = int64_t{0}; i < list->length(); ++i) {
    if (list->IsNull(i)) {
      // TODO: What do we want to do here?
      b.null();
      continue;
    }
    auto instance = make_aggregation();
    TENZIR_ASSERT(instance);
    instance->add(
      aggregation_instance::add_info{
        inv.self.fn, located{series{inner_type, list->value_slice(i)},
                             inv.self.args[0].get_location()}},
      dh);
    b.data(instance->finish());
  }
  auto result = b.finish();
  // TODO: Can this happen? If so, what do we want to do? Do we need to adjust
  // the plugin interface?
  if (result.size() != 1) {
    diagnostic::warning("internal error in `{}`: unexpected array count: {}",
                        result.size(), name())
      .primary(inv.self.fn.get_location())
      .emit(dh);
    return series::null(null_type{}, inv.length);
  }
  return std::move(result[0]);
}

} // namespace tenzir::tql2
