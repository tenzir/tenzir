//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/eval_kernel.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/nova/type_system.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

using namespace tenzir::nova;

namespace tenzir::plugins::int_ {

namespace {

struct IntArgs {
  ValueArgument x;
  located<uint64_t> base = {10, location::unknown};
  location call;
};

template <bool Signed>
class IntUintFunction final {
public:
  using Type = std::conditional_t<Signed, int64_type, uint64_type>;
  using Data = type_to_data_t<Type>;

  static auto eval(IntArgs const& args, EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto const name = Signed ? "int" : "uint";
    using NovaTag = std::conditional_t<Signed, nova::Int, nova::UInt>;
    auto warn_overflow = nova::WarnOnce{};
    auto warn_convert = nova::WarnOnce{};
    return apply_kernel<1>(
      frame, name, {args.x}, args.call,
      detail::overload{
        [](diagnostic_handler&, nova::Null) -> Option<NovaTag> {
          return None{};
        },
        [](diagnostic_handler&, NovaTag v) -> Option<NovaTag> {
          return v;
        },
        []<class T>(diagnostic_handler&, T v) -> Option<NovaTag>
          requires(std::same_as<T, nova::Int> or std::same_as<T, nova::UInt>)
        {
          if (not std::in_range<Data>(v)) {
            return None{};
          }
          return static_cast<Data>(v);
        },
        [](diagnostic_handler&, nova::Bool v) -> Option<NovaTag> {
          return static_cast<Data>(v);
        },
        [&args, name, &warn_overflow](diagnostic_handler& dh,
                                      nova::Float v) -> Option<NovaTag> {
          auto min
            = static_cast<double>(std::numeric_limits<Data>::lowest()) - 1.0;
          auto max
            = static_cast<double>(std::numeric_limits<Data>::max()) + 1.0;
          if (not(v > min) or not(v < max)) {
            warn_overflow(dh,
                          diagnostic::warning("integer overflow in `{}`", name)
                            .primary(args.x.source));
            return None{};
          }
          return static_cast<Data>(v);
        },
        [&args, name, &warn_convert, base = args.base.inner](
          diagnostic_handler& dh, std::string_view v) -> Option<NovaTag> {
          constexpr auto p = std::invoke([] {
            if constexpr (Signed) {
              return parsers::i64;
            } else {
              return parsers::u64;
            }
          });
          constexpr auto q
            = ignore(*parsers::space) >> p >> ignore(*parsers::space);
          constexpr auto px = std::invoke([] {
            if constexpr (Signed) {
              return parsers::ix64;
            } else {
              return parsers::ux64;
            }
          });
          constexpr auto qx
            = ignore(*parsers::space) >> px >> ignore(*parsers::space);
          auto result = Data{};
          switch (base) {
            case 10:
              if (q(v, result)) {
                return result;
              }
              break;
            case 16:
              if (qx(v, result)) {
                return result;
              }
              break;
            default:
              TENZIR_UNREACHABLE();
          }
          warn_convert(
            dh, diagnostic::warning("`{}` failed to convert some string", name)
                  .primary(args.x.source)
                  .note("tried to convert: {}", v));
          return None{};
        },
        });
  }
};

template <bool Signed>
class int_uint final : public FunctionPlugin {
public:
  using Type = std::conditional_t<Signed, int64_type, uint64_type>;
  using Array = type_to_arrow_array_t<Type>;
  using Builder = type_to_arrow_builder_t<Type>;
  using Data = type_to_data_t<Type>;

  auto name() const -> std::string override {
    return Signed ? "int" : "uint";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> FunctionDescription override {
    auto d = FunctionDescriber<IntArgs, IntUintFunction<Signed>>{};
    d.positional("x", &IntArgs::x, "number|string");
    d.named_optional("base", &IntArgs::base);
    d.call_location(&IntArgs::call);
    d.validate([](IntArgs& args, diagnostic_handler& dh) -> failure_or<void> {
      if (args.base.inner != 10 and args.base.inner != 16) {
        diagnostic::error("`base` must be 10 or 16").primary(args.base).emit(dh);
        return failure::promise();
      }
      return {};
    });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto base = located<uint64_t>{10, location::unknown};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "number|string")
          .named_optional("base", base)
          .parse(inv, ctx));
    if (base.inner != 10 and base.inner != 16) {
      diagnostic::error("`base` must be 10 or 16").primary(base).emit(ctx);
      return failure::promise();
    }
    return function_use::make([this, expr = std::move(expr),
                               base = base.inner](auto eval, session ctx) {
      auto failed = Option<std::string>{};
      auto result = map_series(eval(expr), [&](series value) {
        auto f = detail::overload{
          [](const arrow::NullArray& arg) {
            auto b = Builder{tenzir::arrow_memory_pool()};
            check(b.AppendNulls(arg.length()));
            return finish(b);
          },
          [](const Array& arg) {
            return std::make_shared<Array>(arg.data());
          },
          [&]<class T>(const T& arg)
            requires integral_type<type_from_arrow_t<T>>
          {
            auto b = Builder{tenzir::arrow_memory_pool()};
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
            auto b = Builder{tenzir::arrow_memory_pool()};
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
                = static_cast<double>(std::numeric_limits<Data>::lowest())
                  - 1.0;
              auto max
                = static_cast<double>(std::numeric_limits<Data>::max()) + 1.0;
              if (not(val > min) or not(val < max)) {
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
            auto b = Builder{tenzir::arrow_memory_pool()};
            check(b.Reserve(value.length()));
            constexpr auto p = std::invoke([] {
              if constexpr (Signed) {
                return parsers::i64;
              } else {
                return parsers::u64;
              }
            });
            constexpr auto q
              = ignore(*parsers::space) >> p >> ignore(*parsers::space);
            constexpr auto px = std::invoke([] {
              if constexpr (Signed) {
                return parsers::ix64;
              } else {
                return parsers::ux64;
              }
            });
            constexpr auto qx
              = ignore(*parsers::space) >> px >> ignore(*parsers::space);
            for (auto row = int64_t{0}; row < value.length(); ++row) {
              if (arg.IsNull(row)) {
                // TODO: Do we want to report this? Probably not.
                check(b.AppendNull());
              } else {
                auto result = Data{};
                switch (base) {
                  case 10:
                    if (q(arg.GetView(row), result)) {
                      check(b.Append(result));
                      continue;
                    }
                    break;
                  case 16:
                    if (qx(arg.GetView(row), result)) {
                      check(b.Append(result));
                      continue;
                    }
                    break;
                  default:
                    TENZIR_UNREACHABLE();
                }
                check(b.AppendNull());
                if (not failed) {
                  failed = std::string{arg.GetView(row)};
                }
              }
            }
            return finish(b);
          },
          [&](const auto&) -> std::shared_ptr<Array> {
            diagnostic::warning("`{}` currently expects `number` or `string`, "
                                "got `{}`",
                                name(), value.type.kind())
              .primary(expr)
              .emit(ctx);
            auto b = Builder{tenzir::arrow_memory_pool()};
            check(b.AppendNulls(value.length()));
            return finish(b);
          },
          };
        return series{Type{}, match(*value.array, f)};
      });
      if (failed) {
        diagnostic::warning("`{}` failed to convert some string", name())
          .primary(expr)
          .note(fmt::format("tried to convert: {}", *failed))
          .emit(ctx);
      }
      return result;
    });
  }
};

} // namespace

} // namespace tenzir::plugins::int_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::int_::int_uint<true>)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::int_::int_uint<false>)
