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
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/ocsf.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::ocsf {

namespace {

/// Generic mapping plugin that supports `In -> Out` conversion.
template <class In, class Out>
class generic_mapping_plugin final : public function_plugin {
public:
  using function = std::function<auto(ocsf_version, In)->std::optional<Out>>;

  generic_mapping_plugin(std::string name, std::string input_meta,
                         function function, std::string warning_text)
    : name_{std::move(name)},
      input_meta_{std::move(input_meta)},
      function_{std::move(function)},
      warning_text_{std::move(warning_text)} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto make_function(invocation inv, session ctx) const
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
              auto warn_value = std::optional<input_data_type>{};
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
  "ocsf::category_uid", "string", tenzir::ocsf_category_uid,
  "OCSF category name"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::id_to_name_plugin{
  "ocsf::category_name", "int", tenzir::ocsf_category_name, "OCSF category ID"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::name_to_id_plugin{
  "ocsf::class_uid", "string", tenzir::ocsf_class_uid, "OCSF class name"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::id_to_name_plugin{
  "ocsf::class_name", "int", tenzir::ocsf_class_name, "OCSF class ID"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::name_to_id_plugin{
  "ocsf::type_uid", "string", tenzir::ocsf_type_uid, "OCSF type name"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::id_to_name_plugin{
  "ocsf::type_name", "int", tenzir::ocsf_type_name, "OCSF type ID"})
