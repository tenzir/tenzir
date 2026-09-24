//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/concepts.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/ocsf.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::ocsf {

namespace {

template <class In, class Out>
using mapping_function = auto (*)(ocsf_version, In) -> Option<Out>;

template <class In, class Out>
struct MappingArgs {
  nova::ValueArgument x;
  location call;
  // Filled in by `validate` from the plugin instance.
  mapping_function<In, Out> function = nullptr;
  std::string_view name;
  std::string_view warning_text;
};

template <class In, class Out>
struct MappingFunction {
  auto eval(MappingArgs<In, Out> const& args, nova::EvalFrame frame) const
    -> nova::Array<nova::Data> {
    using OutView = std::conditional_t<std::same_as<Out, int64_t>, nova::Int,
                                       std::string_view>;
    auto warn_invalid = nova::WarnOnce{};
    auto map = [&](diagnostic_handler& dh, In in,
                   auto const& original) -> Option<OutView> {
      // TODO: Because some of the values depend on the OCSF version, this
      // function should actually also require the desired OCSF version as an
      // argument.
      if (auto out = args.function(ocsf_version::v1_5_0, in)) {
        return *out;
      }
      warn_invalid(dh, diagnostic::warning("invalid {}", args.warning_text)
                         .note("got `{}`", original)
                         .primary(args.x.source));
      return None{};
    };
    return nova::apply_kernel<1>(
      frame, args.name, {args.x}, args.call,
      detail::overload{
        [](diagnostic_handler&, nova::Null) -> Option<OutView> {
          return None{};
        },
        // Constrained to exact types, since `bool` converts implicitly.
        [&]<class T>(diagnostic_handler& dh, T v) -> Option<OutView>
          requires(std::same_as<In, std::string_view>
                   and std::same_as<T, std::string_view>)
                    or (std::same_as<In, int64_t>
                        and concepts::one_of<T, nova::Int, nova::UInt>)
        {
          if constexpr (std::same_as<T, nova::UInt>) {
            if (not std::in_range<int64_t>(v)) {
              warn_invalid(dh,
                           diagnostic::warning("invalid {}", args.warning_text)
                             .note("got `{}`", v)
                             .primary(args.x.source));
              return None{};
            }
            return map(dh, static_cast<int64_t>(v), v);
          } else {
            return map(dh, v, v);
          }
        },
        });
  }
};

/// Generic mapping plugin that supports `In -> Out` conversion.
template <class In, class Out>
class generic_mapping_plugin final : public nova::FunctionPlugin {
public:
  using function = mapping_function<In, Out>;

  generic_mapping_plugin(std::string name, std::string input_meta,
                         function function, std::string warning_text)
    : name_{std::move(name)},
      input_meta_{std::move(input_meta)},
      function_{function},
      warning_text_{std::move(warning_text)} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto describe() const -> nova::FunctionDescription override {
    using Args = MappingArgs<In, Out>;
    auto d = nova::FunctionDescriber<Args, MappingFunction<In, Out>>{};
    d.positional("x", &Args::x, input_meta_);
    d.call_location(&Args::call);
    d.validate([this](Args& args, diagnostic_handler&) -> failure_or<void> {
      args.function = function_;
      args.name = name_;
      args.warning_text = warning_text_;
      return {};
    });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name_)
          .positional("x", expr, input_meta_)
          .parse(inv, ctx));
    return function_use::make(
      [&, expr = std::move(expr)](evaluator eval, session ctx) -> series {
        using InTy = data_to_type_t<materialize_t<In>>;
        using OutTy = data_to_type_t<materialize_t<Out>>;
        auto b = type_to_arrow_builder_t<OutTy>{tenzir::arrow_memory_pool()};
        check(b.Reserve(eval.length()));
        for (auto& arg : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::NullArray& arg) {
              check(b.AppendNulls(arg.length()));
            },
            [&]<class Array>(const Array& arg)
              requires(std::same_as<Array, type_to_arrow_array_t<InTy>>
                       or (std::same_as<InTy, int64_t>
                           and std::same_as<Array, arrow::UInt64Array>))
            {
              using input_data_type = type_to_data_t<type_from_arrow_t<Array>>;
              auto warn_value = Option<input_data_type>{};
              for (auto i = int64_t{0}; i < arg.length(); ++i) {
                if (arg.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                auto in = arg.GetView(i);
                if constexpr (std::same_as<input_data_type, uint64_type>) {
                  if (in > static_cast<uint64_t>(
                        std::numeric_limits<int64_t>::max())) {
                    warn_value = in;
                    check(b.AppendNull());
                    continue;
                  }
                }
                // TODO: Because the some of the values depend on the OCSF
                // version, this function should actually also require the
                // desired OCSF version as an argument.
                auto out = function_(ocsf_version::v1_5_0, in);
                if (out) {
                  check(b.Append(*out));
                } else {
                  if (not warn_value) {
                    warn_value = in;
                  }
                  check(b.AppendNull());
                }
              }
              if (warn_value) {
                diagnostic::warning("invalid {}", warning_text_)
                  .note("got `{}`", *warn_value)
                  .primary(expr)
                  .emit(ctx);
              }
            },
            [&](const auto&) {
              diagnostic::warning("expected `{}`, but got `{}`", input_meta_,
                                  arg.type.kind())
                .primary(expr)
                .emit(ctx);
              check(b.AppendNulls(arg.length()));
            },
            };
          match(*arg.array, f);
        }
        return series{OutTy{}, finish(b)};
      });
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

private:
  std::string name_;
  std::string input_meta_;
  function function_;
  std::string warning_text_;
};

using name_to_id_plugin = generic_mapping_plugin<std::string_view, int64_t>;
using id_to_name_plugin = generic_mapping_plugin<int64_t, std::string_view>;

} // namespace

} // namespace tenzir::plugins::ocsf

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::name_to_id_plugin{
  "ocsf_category_uid", "string", tenzir::ocsf_category_uid,
  "OCSF category name"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::id_to_name_plugin{
  "ocsf_category_name", "int", tenzir::ocsf_category_name, "OCSF category ID"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::name_to_id_plugin{
  "ocsf_class_uid", "string", tenzir::ocsf_class_uid, "OCSF class name"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::id_to_name_plugin{
  "ocsf_class_name", "int", tenzir::ocsf_class_name, "OCSF class ID"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::name_to_id_plugin{
  "ocsf_type_uid", "string", tenzir::ocsf_type_uid, "OCSF type name"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::id_to_name_plugin{
  "ocsf_type_name", "int", tenzir::ocsf_type_name, "OCSF type ID"})
