//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include "tenzir/series_builder.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"

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

auto function_argument_parser::parse(tql2::function_plugin::invocation& inv,
                                     diagnostic_handler& dh) -> bool {
  // TODO: This is called once per batch at the moment. Perhaps something more
  // like the operator API is better.
  auto it = positional_.begin();
  for (auto& arg : inv.args) {
    auto success = arg.match(
      [&](tql2::function_plugin::positional_argument& arg) {
        if (it == positional_.end()) {
          diagnostic::error("unexpected positional argument")
            .primary(arg.source)
            .emit(dh);
          return false;
        }
        auto success = it->set(arg, dh);
        ++it;
        return success;
      },
      [&](tql2::function_plugin::named_argument& arg) {
        diagnostic::error("unexpected named argument")
          .primary(arg.selector.get_location())
          .emit(dh);
        return false;
      });
    if (not success) {
      return false;
    }
  }
  if (it != positional_.end()) {
    // TODO: Better location?
    diagnostic::error("missing positional argument")
      .primary(inv.self.get_location())
      .emit(dh);
    return false;
  }
  return true;
}

template <type_or_concrete_type Type>
auto function_argument_parser::try_cast(series x)
  -> std::optional<basic_series<Type>> {
  if (auto cast = x.as<Type>()) {
    return std::move(*cast);
  }
  if (x.type.kind().is<null_type>()) {
    auto ty = std::optional<Type>{};
    if constexpr (std::is_default_constructible_v<Type>) {
      ty = Type{};
    } else if constexpr (std::same_as<Type, list_type>) {
      ty = Type{null_type{}};
    } else {
      // We cannot pick a type for those here.
      // TODO: For record, we could pick the empty record.
      static_assert(caf::detail::is_one_of<Type, map_type, enumeration_type,
                                           record_type>::value);
    }
    if (ty) {
      return basic_series<Type>::null(*ty, x.array->length());
    }
  }
  return std::nullopt;
}

template <type_or_concrete_type Type>
auto function_argument_parser::add(basic_series<Type>& x, std::string meta)
  -> function_argument_parser& {
  positional_.emplace_back(
    [&x](located<series> y, diagnostic_handler& dh) {
      if constexpr (std::same_as<Type, type>) {
        x = std::move(y.inner);
        return true;
      } else {
        if (auto cast = try_cast<Type>(y.inner)) {
          x = std::move(*cast);
          return true;
        }
        diagnostic::warning("expected {} but got {}", type_kind::of<Type>,
                            y.inner.type.kind())
          .primary(y.source)
          .emit(dh);
        return false;
      }
    },
    std::move(meta));
  return *this;
}

template <type_or_concrete_type Type>
auto function_argument_parser::add(located<basic_series<Type>>& x,
                                   std::string meta)
  -> function_argument_parser& {
  positional_.emplace_back(
    [&x](located<series> y, diagnostic_handler& dh) {
      if constexpr (std::same_as<Type, type>) {
        x = std::move(y);
        return true;
      } else {
        if (auto cast = try_cast<Type>(y.inner)) {
          x = located{std::move(*cast), y.source};
          return true;
        }
        diagnostic::warning("expected {} but got {}", type_kind::of<Type>,
                            y.inner.type.kind())
          .primary(y.source)
          .emit(dh);
        return false;
      }
    },
    std::move(meta));
  return *this;
}

template <std::monostate>
struct function_argument_parser::instantiate {
  template <class T>
  using add
    = auto (function_argument_parser::*)(basic_series<T>& x, std::string meta)
      -> function_argument_parser&;

  template <class T>
  using add_located
    = auto (function_argument_parser::*)(located<basic_series<T>>& x,
                                         std::string meta)
      -> function_argument_parser&;

  template <class... T>
  struct inner {
    static constexpr auto value = std::tuple{
      &function_argument_parser::try_cast<T>...,
      static_cast<add<T>>(&function_argument_parser::add)...,
      static_cast<add_located<T>>(&function_argument_parser::add)...,
    };
  };

  static constexpr auto value
    = detail::tl_apply_t<caf::detail::tl_cons_t<concrete_types, type>,
                         inner>::value;
};

template struct function_argument_parser::instantiate<std::monostate{}>;

auto aggregation_plugin::eval(invocation inv, diagnostic_handler& dh) const
  -> series {
  // TODO: Consider making this pure-virtual or provide a default implementation.
  diagnostic::error("this function can only be used as an aggregation function")
    .primary(inv.self.fn.get_location())
    .emit(dh);
  return series::null(null_type{}, inv.length);
}

} // namespace tenzir
