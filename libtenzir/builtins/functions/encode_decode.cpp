//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_utils.hpp"
#include "tenzir/curl.hpp"
#include "tenzir/detail/base58.hpp"
#include "tenzir/detail/hex_encode.hpp"
#include "tenzir/view3.hpp"

#include <tenzir/detail/base64.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type_fwd.h>

namespace tenzir::plugins::encode_decode {

namespace {

template <detail::string_literal Name, bool encode, auto F,
          fbs::data::SecretTransformations tag>
class plugin final : public function_plugin {
  using Type = std::conditional_t<encode, string_type, blob_type>;
  auto name() const -> std::string override {
    return (encode ? "encode_" : "decode_") + std::string{Name};
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
              if constexpr (encode) {
                check(b->Append(F(array.Value(i))));
              } else {
                const auto decoded = F(array.Value(i));
                if (not decoded) {
                  diagnostic::warning("invalid {} encoding", Name)
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
          [&](const type_to_arrow_array_t<secret_type>& array) -> series {
            auto b = type_to_arrow_builder_t<secret_type>{};
            check(b.Reserve(array.length()));
            for (auto s : values3(array)) {
              if (not s) {
                check(b.AppendNull());
                continue;
              }
              check(append_builder(secret_type{}, b, s->with_operation(tag)));
            }
            return series{secret_type{}, finish(b)};
          },
          [&](const auto&) -> series {
            diagnostic::warning("expected `blob` or `string`, got "
                                "`{}`",
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

using encode_base64 = plugin<"base64", true,
                             static_cast<std::string (*)(std::string_view)>(
                               detail::base64::encode),
                             fbs::data::SecretTransformations::encode_base64>;
using decode_base64
  = plugin<"base64", false, detail::base64::try_decode<std::string>,
           fbs::data::SecretTransformations::decode_base64>;

using encode_url
  = plugin<"url", true,
           static_cast<std::string (*)(std::string_view)>(curl::escape),
           fbs::data::SecretTransformations::encode_url>;
using decode_url = plugin<"url", false, curl::try_unescape,
                          fbs::data::SecretTransformations::decode_url>;

using encode_base58 = plugin<"base58", true, detail::base58::encode,
                             fbs::data::SecretTransformations::encode_url>;
using decode_base68 = plugin<"base58", false, detail::base58::decode,
                             fbs::data::SecretTransformations::decode_url>;

using encode_hex = plugin<"hex", true, detail::hex::encode,
                          fbs::data::SecretTransformations::encode_hex>;
using decode_hex = plugin<"hex", false, detail::hex::decode,
                          fbs::data::SecretTransformations::decode_hex>;

} // namespace

} // namespace tenzir::plugins::encode_decode

TENZIR_REGISTER_PLUGIN(tenzir::plugins::encode_decode::encode_base64)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encode_decode::decode_base64)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encode_decode::encode_url)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encode_decode::decode_url)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encode_decode::encode_base58)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encode_decode::decode_base68)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encode_decode::encode_hex)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encode_decode::decode_hex)
