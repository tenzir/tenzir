//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/cryptopan.hpp"

#include "tenzir/multi_series.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type_fwd.h>

#include <algorithm>
#include <cstdlib>
#include <string>

namespace tenzir::plugins::cryptopan {

namespace {

enum class Mode {
  encrypt,
  decrypt,
};

template <Mode Value>
auto transform_cryptopan(ip const& value, cryptopan_seed const& seed,
                         [[maybe_unused]] Option<ip::family> decrypt_family)
  -> ip {
  if constexpr (Value == Mode::decrypt) {
    return decrypt_family
             ? tenzir::decrypt_cryptopan(value, seed, *decrypt_family)
             : tenzir::decrypt_cryptopan(value, seed);
  } else {
    return tenzir::encrypt_cryptopan(value, seed);
  }
}

template <Mode Value>
class cryptopan_function : public virtual function_plugin {
  auto name() const -> std::string override {
    if constexpr (Value == Mode::decrypt) {
      return "decrypt_cryptopan";
    } else {
      return "encrypt_cryptopan";
    }
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto seed = Option<std::string>{};
    auto parser = argument_parser2::function(name());
    parser.positional("x", expr, "ip").named("seed", seed);
    auto decrypt_family = Option<ip::family>{};
    if constexpr (Value == Mode::decrypt) {
      auto family = Option<located<std::string>>{};
      parser.named("family", family, "string");
      TRY(parser.parse(inv, ctx));
      if (family) {
        if (family->inner == "ipv4") {
          decrypt_family = ip::ipv4;
        } else if (family->inner == "ipv6") {
          decrypt_family = ip::ipv6;
        } else {
          diagnostic::error("`family` must be one of `ipv4`, `ipv6`")
            .primary(*family)
            .emit(ctx);
          return failure::promise();
        }
      }
    } else {
      TRY(parser.parse(inv, ctx));
    }
    auto seed_bytes = cryptopan_seed{};
    if (seed) {
      auto max_seed_size = std::min(cryptopan_seed_size * 2, seed->size());
      for (auto i = size_t{0}; (i * 2) < max_seed_size; ++i) {
        auto byte_string_pos = i * 2;
        auto byte_size = (byte_string_pos + 2 > seed->size()) ? 1 : 2;
        auto byte = seed->substr(byte_string_pos, byte_size);
        if (byte_size == 1) {
          byte.append("0");
        }
        TENZIR_ASSERT(i < seed_bytes.size());
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
        seed_bytes[i]
          = static_cast<std::byte>(std::strtoul(byte.c_str(), nullptr, 16));
      }
    }
    return function_use::make([expr = std::move(expr), seed = seed_bytes,
                               decrypt_family](evaluator eval, session ctx) {
      return map_series(eval(expr), [&](series s) {
        return match(
          *s.array,
          [&](arrow::NullArray const& array) {
            return series::null(ip_type{}, array.length());
          },
          [&](ip_type::array_type const& array) {
            auto b = ip_type::make_arrow_builder(arrow_memory_pool());
            for (auto const& value : values(ip_type{}, array)) {
              if (not value) {
                check(b->AppendNull());
                continue;
              }
              auto result
                = transform_cryptopan<Value>(*value, seed, decrypt_family);
              check(append_builder(ip_type{}, *b, result));
            }
            return series{ip_type{}, finish(*b)};
          },
          [&](auto const&) {
            diagnostic::warning("expected type `ip`, got `{}`", s.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(ip_type{}, s.length());
          });
      });
    });
  }
};

using encrypt_cryptopan = cryptopan_function<Mode::encrypt>;
using decrypt_cryptopan = cryptopan_function<Mode::decrypt>;

} // namespace
} // namespace tenzir::plugins::cryptopan

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cryptopan::encrypt_cryptopan)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::cryptopan::decrypt_cryptopan)
