//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_utils.hpp"
#include "tenzir/curl.hpp"

#include <tenzir/detail/url.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type_fwd.h>

namespace tenzir::plugins::url {

namespace {

enum class mode { encode, decode };

template <mode Mode>
class plugin final : public function_plugin {
  using Type = std::conditional_t<Mode == mode::encode, string_type, blob_type>;
  auto name() const -> std::string override {
    return Mode == mode::encode ? "encode_url" : "decode_url";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("value", expr, "blob|string")
          .parse(inv, ctx));
    return function_use::make([expr
                               = std::move(expr)](evaluator eval, session ctx) {
      return map_series(eval(expr), [&](series value) {
        const auto f = detail::overload{
          [&](const arrow::NullArray& array) -> series {
            return series::null(Type{}, array.length());
          },
          [&](
            const concepts::one_of<arrow::BinaryArray, arrow::StringArray> auto&
              array) -> series {
            auto b = Type::make_arrow_builder(arrow_memory_pool());
            check(b->Reserve(array.length()));
            for (auto i = int64_t{}; i < array.length(); ++i) {
              if (array.IsNull(i)) {
                check(b->AppendNull());
                continue;
              }
              if constexpr (Mode == mode::encode) {
                check(b->Append(curl::escape(array.Value(i))));
              } else {
                const auto decoded = curl::try_unescape(array.Value(i));
                if (not decoded) {
                  diagnostic::warning("invalid url encoding")
                    .primary(expr)
                    .emit(ctx);
                  check(b->AppendNull());
                  continue;
                }
                check(b->Append(decoded.value()));
              }
            }
            return series{Type{}, finish(*b)};
          },
          [&](const auto&) -> series {
            diagnostic::warning("expected `blob` or `string`, got `{}`",
                                value.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(Type{}, value.length());
          }};
        return match(*value.array, f);
      });
    });
  }
};

using encode_url = plugin<mode::encode>;
using decode_url = plugin<mode::decode>;

} // namespace

} // namespace tenzir::plugins::url

TENZIR_REGISTER_PLUGIN(tenzir::plugins::url::encode_url)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::url::decode_url)
