//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include "tenzir/tql2/eval_impl.hpp"

namespace tenzir::tql2 {

// auto aggregation_function_plugin::eval(invocation inv,
//                                        diagnostic_handler& dh) const ->
//                                        series {
//   if (inv.args.size() != 1) {
//     diagnostic::error("function `{}` expects exactly one argument", name())
//       .primary(inv.self.get_location())
//       .emit(dh);
//     return series::null(null_type{}, inv.length);
//   }
//   auto arg = std::get_if<positional_argument>(&inv.args[0]);
//   if (not arg) {
//     diagnostic::error("function `{}` expects a positional argument", name())
//       .primary(inv.self.get_location())
//       .emit(dh);
//     return series::null(null_type{}, inv.length);
//   }
//   auto& array = arg->inner.array;
//   auto& type = arg->inner.type;
//   auto list = dynamic_cast<arrow::ListArray*>(&*array);
//   if (not list) {
//     diagnostic::warning("function `{}` expects a list, got {}", name(),
//                         type.kind())
//       .primary(inv.self.fn.get_location())
//       .emit(dh);
//     return series::null(null_type{}, inv.length);
//   }
//   auto inner_type = caf::get<list_type>(type).value_type();
//   auto b = series_builder{};
//   for (auto i = int64_t{0}; i < list->length(); ++i) {
//     if (list->IsNull(i)) {
//       // TODO: What do we want to do here?
//       b.null();
//       continue;
//     }
//     auto instance = make_aggregation();
//     TENZIR_ASSERT(instance);
//     instance->add(
//       aggregation_instance::add_info{
//         inv.self.fn, located{series{inner_type, list->value_slice(i)},
//                              inv.self.args[0].get_location()}},
//       dh);
//     b.data(instance->finish());
//   }
//   auto result = b.finish();
//   // TODO: Can this happen? If so, what do we want to do? Do we need to
//   adjust
//   // the plugin interface?
//   if (result.size() != 1) {
//     diagnostic::warning("internal error in `{}`: unexpected array count: {}",
//                         result.size(), name())
//       .primary(inv.self.fn.get_location())
//       .emit(dh);
//     return series::null(null_type{}, inv.length);
//   }
//   return std::move(result[0]);
// }

} // namespace tenzir::tql2

namespace tenzir {

auto function_use::evaluator::length() const -> int64_t {
  return static_cast<tenzir::evaluator*>(self_)->length();
}

auto function_use::evaluator::operator()(const ast::expression& expr) const
  -> series {
  return static_cast<tenzir::evaluator*>(self_)->eval(expr);
}

auto aggregation_plugin::make_function(invocation inv, session ctx) const
  -> std::unique_ptr<function_use> {
  // TODO: Consider making this pure-virtual or provide a default implementation.
  diagnostic::error("this function can only be used as an aggregation function")
    .primary(inv.call.fn)
    .emit(ctx);
  return nullptr;
}

auto function_use::make(
  std::function<auto(evaluator eval, session ctx)->series> f)
  -> std::unique_ptr<function_use> {
  class result final : public function_use {
  public:
    explicit result(std::function<auto(evaluator eval, session ctx)->series> f)
      : f_{std::move(f)} {
    }

    auto run(evaluator eval, session ctx) const -> series override {
      return f_(std::move(eval), ctx);
    }

  private:
    std::function<auto(evaluator eval, session ctx)->series> f_;
  };
  return std::make_unique<result>(std::move(f));
}

} // namespace tenzir
