//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::int_ {

namespace {

template <bool Signed>
class int_uint final : public function_plugin {
public:
  using Type = std::conditional_t<Signed, int64_type, uint64_type>;
  using Array = type_to_arrow_array_t<Type>;
  using Builder = type_to_arrow_builder_t<Type>;
  using Data = type_to_data_t<Type>;

  auto name() const -> std::string override {
    return Signed ? "int" : "uint";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .add(expr, "<string|number>")
          .parse(inv, ctx));
    return function_use::make([expr = std::move(expr),
                               this](auto eval, session ctx) -> series {
      auto value = eval(expr);
      // TODO: Add overload for `int -> uint` conversion.
      auto f = detail::overload{
        [](const arrow::NullArray& arg) {
          auto b = Builder{};
          check(b.AppendNulls(arg.length()));
          return finish(b);
        },
        [](const Array& arg) {
          return std::make_shared<Array>(arg.data());
        },
        [&]<class T>(const T& arg)
          requires integral_type<type_from_arrow_t<T>>
        {
          auto b = Builder{};
          check(b.Reserve(arg.length()));
          auto overflow = false;
          for (auto i = int64_t{0}; i < arg.length(); ++i) {
            if (arg.IsNull(i)) {
              check(b.AppendNull());
              continue;
            }
            auto val = arg.Value(i);
            if (not std::in_range<Data>(val)) {
              check(b.AppendNull());
              overflow = true;
              continue;
            }
            check(b.Append(static_cast<Data>(val)));
          }
          if (overflow) {
            diagnostic::warning("integer overflow in `{}`", name())
              .primary(expr)
              .emit(ctx);
          }
          return finish(b);
        },
        [&](const arrow::DoubleArray& arg) {
          auto b = Builder{};
          check(b.Reserve(arg.length()));
          auto overflow = false;
          for (auto i = int64_t{0}; i < arg.length(); ++i) {
            if (arg.IsNull(i)) {
              check(b.AppendNull());
              continue;
            }
            auto val = arg.Value(i);
            // TODO: Make sure that the following does what we want.
            auto min
              = static_cast<double>(std::numeric_limits<Data>::lowest()) - 1.0;
            auto max
              = static_cast<double>(std::numeric_limits<Data>::max()) + 1.0;
            if (not(val > min) || not(val < max)) {
              check(b.AppendNull());
              overflow = true;
              continue;
            }
            check(b.Append(static_cast<Data>(val)));
          }
          if (overflow) {
            diagnostic::warning("integer overflow in `{}`", name())
              .primary(expr)
              .emit(ctx);
          }
          return finish(b);
        },
        [&](const arrow::StringArray& arg) {
          auto report = false;
          auto b = Builder{};
          check(b.Reserve(value.length()));
          for (auto row = int64_t{0}; row < value.length(); ++row) {
            if (arg.IsNull(row)) {
              // TODO: Do we want to report this? Probably not.
              check(b.AppendNull());
            } else {
              auto p = std::invoke([] {
                if constexpr (Signed) {
                  return parsers::i64;
                } else {
                  return parsers::u64;
                }
              });
              auto q = ignore(*parsers::space) >> p >> ignore(*parsers::space);
              auto result = Data{};
              if (q(arg.GetView(row), result)) {
                check(b.Append(result));
              } else {
                check(b.AppendNull());
                report = true;
              }
            }
          }
          if (report) {
            // TODO: It would be helpful to know what string, but then
            // deduplication doesn't work.
            diagnostic::warning("`{}` failed to convert some string", name())
              .primary(expr)
              .emit(ctx);
          }
          return finish(b);
        },
        [&](const auto&) -> std::shared_ptr<Array> {
          diagnostic::warning("`{}` currently expects `number` or `string`, "
                              "got `{}`",
                              name(), value.type.kind())
            .primary(expr)
            .emit(ctx);
          auto b = Builder{};
          check(b.AppendNulls(value.length()));
          return finish(b);
        },
        };
      return series{Type{}, caf::visit(f, *value.array)};
    });
  }
};

} // namespace

} // namespace tenzir::plugins::int_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::int_::int_uint<true>)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::int_::int_uint<false>)
