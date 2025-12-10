//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::get_identity_token {

namespace {

class get_identity_token final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.get_identity_token";
  }

  auto is_deterministic() const -> bool final {
    return true;
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto audience = ast::expression{};
    TRY(argument_parser2::function("get_identity_token")
          .positional("audience", audience, "string")
          .parse(inv, ctx));
    return function_use::make([expr = std::move(audience)](
                                evaluator eval, session ctx) -> series {
      auto b = secret_type::builder_type{};
      check(b.Reserve(eval.length()));
      for (auto& value : eval(expr)) {
        auto f = detail::overload{
          [&](const arrow::StringArray& array) {
            for (auto i = int64_t{0}; i < array.length(); ++i) {
              if (array.IsNull(i)) {
                check(b.AppendNull());
                continue;
              }
              auto secret_name = fmt::format(
                "__tenzir_workload_identity?aud={}", array.GetView(i));
              check(append_builder(
                secret_type{}, b, ::tenzir::secret::make_managed(secret_name)));
            }
          },
          [&](const arrow::NullArray&) {
            check(b.AppendNulls(value.length()));
          },
          [&](const auto&) {
            diagnostic::warning("expected `string`, got `{}`",
                                value.type.kind())
              .primary(expr)
              .emit(ctx);
            check(b.AppendNulls(value.length()));
          },
        };
        match(*value.array, f);
      }
      return series{secret_type{}, finish(b)};
    });
  }
};

} // namespace

} // namespace tenzir::plugins::get_identity_token

TENZIR_REGISTER_PLUGIN(tenzir::plugins::get_identity_token::get_identity_token)
