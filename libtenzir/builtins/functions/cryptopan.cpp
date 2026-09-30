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
#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/nova/function_plugin.hpp>
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

auto parse_seed(Option<std::string> const& seed) -> cryptopan_seed {
  auto result = cryptopan_seed{};
  if (not seed) {
    return result;
  }
  auto max_size = std::min(cryptopan_seed_size * 2, seed->size());
  for (auto i = size_t{0}; i * 2 < max_size; ++i) {
    auto byte = seed->substr(i * 2, 2);
    if (byte.size() == 1) {
      byte += '0';
    }
    result[i] = static_cast<std::byte>(std::strtoul(byte.c_str(), nullptr, 16));
  }
  return result;
}

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

struct CryptopanArgs {
  nova::ValueArgument x;
  Option<std::string> seed;
  Option<located<std::string>> family;
  cryptopan_seed seed_bytes{};
  Option<ip::family> decrypt_family;
};

template <Mode Value>
struct CryptopanFunction {
  static auto eval(CryptopanArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova::apply_kernel<1>(
      frame, Value == Mode::encrypt ? "encrypt_cryptopan" : "decrypt_cryptopan",
      {args.x}, args.x.source,
      detail::overload{[](diagnostic_handler&, nova::Null) -> Option<ip> {
                         return None{};
                       },
                       [&](diagnostic_handler&, ip value) -> Option<ip> {
                         return transform_cryptopan<Value>(
                           value, args.seed_bytes, args.decrypt_family);
                       }});
  }
};

template <Mode Value>
class cryptopan_function : public virtual nova::FunctionPlugin {
  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<CryptopanArgs, CryptopanFunction<Value>>{};
    d.positional("x", &CryptopanArgs::x, "ip");
    d.named("seed", &CryptopanArgs::seed);
    if constexpr (Value == Mode::decrypt) {
      d.named("family", &CryptopanArgs::family, "string");
    }
    d.validate(
      [](CryptopanArgs& args, diagnostic_handler& dh) -> failure_or<void> {
        args.seed_bytes = parse_seed(args.seed);
        if (args.family) {
          if (args.family->inner == "ipv4") {
            args.decrypt_family = ip::ipv4;
          } else if (args.family->inner == "ipv6") {
            args.decrypt_family = ip::ipv6;
          } else {
            diagnostic::error("`family` must be one of `ipv4`, `ipv6`")
              .primary(*args.family)
              .emit(dh);
            return failure::promise();
          }
        }
        return {};
      });
    return std::move(d).finish();
  }

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
    auto seed_bytes = parse_seed(seed);
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
