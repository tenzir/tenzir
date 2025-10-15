//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_utils.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <boost/algorithm/hex.hpp>

namespace tenzir::plugins::hex {

namespace {

enum class mode { encode, decode };

template <mode Mode>
class plugin final : public function_plugin {
  using return_type
    = std::conditional_t<Mode == mode::encode, string_type, blob_type>;

  auto name() const -> std::string override {
    return Mode == mode::encode ? "encode_hex" : "decode_hex";
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
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) {
        return map_series(eval(expr), [&](series arg) {
          const auto f = detail::overload{
            [](const arrow::NullArray& array) {
              return series::null(return_type{}, array.length());
            },
            [&](const concepts::one_of<arrow::BinaryArray,
                                       arrow::StringArray> auto& array) {
              auto b = return_type::make_arrow_builder(arrow_memory_pool());
              check(b->Reserve(array.length()));
              for (auto i = int64_t{}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b->AppendNull());
                  continue;
                }
                const auto val = array.Value(i);
                auto str = std::string{};
                if constexpr (Mode == mode::encode) {
                  str.reserve(val.length() * 2);
                  boost::algorithm::hex(val, std::back_inserter(str));
                } else {
                  str.reserve(val.length() / 2);
                  try {
                    boost::algorithm::unhex(val, std::back_inserter(str));
                  } catch (boost::algorithm::hex_decode_error& ex) {
                    check(b->AppendNull());
                    diagnostic::warning("failed to decode hex: {}", ex.what())
                      .primary(expr)
                      .emit(ctx);
                    continue;
                  }
                }
                check(b->Append(str));
              }
              return series{return_type{}, finish(*b)};
            },
            [&](const auto&) {
              diagnostic::warning("expected `blob` or `string`, got `{}`",
                                  arg.type.kind())
                .primary(expr)
                .emit(ctx);
              return series::null(return_type{}, arg.length());
            },
          };
          return match(*arg.array, f);
        });
      });
  }
};

using encode_hex = plugin<mode::encode>;
using decode_hex = plugin<mode::decode>;

} // namespace

} // namespace tenzir::plugins::hex

TENZIR_REGISTER_PLUGIN(tenzir::plugins::hex::encode_hex)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hex::decode_hex)
