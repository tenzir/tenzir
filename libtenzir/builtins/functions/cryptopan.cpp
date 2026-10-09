//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/cryptopan.hpp"

#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/nova/secret.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <algorithm>
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

struct CryptopanArgs {
  nova::ValueArgument x;
  /// The key. Without it, the functions use a key of zeros.
  Option<located<nova::Secret>> seed;
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
        if (args.seed) {
          auto const& bytes = args.seed->inner.data;
          if (bytes.size() != cryptopan_seed_size) {
            diagnostic::error("`seed` must have {} bytes, but has {} bytes",
                              cryptopan_seed_size, bytes.size())
              .primary(args.seed->source)
              .hint("decode a hex or Base64 key with `decode_hex` or "
                    "`decode_base64`, for example "
                    "`secret(\"cryptopan-key\").decode_hex()`")
              .emit(dh);
            return failure::promise();
          }
          std::ranges::copy(bytes, args.seed_bytes.begin());
        }
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
    diagnostic::error("`{}` requires `--nova`", name())
      .primary(inv.call)
      .emit(ctx);
    return failure::promise();
  }
};

using encrypt_cryptopan = cryptopan_function<Mode::encrypt>;
using decrypt_cryptopan = cryptopan_function<Mode::decrypt>;

} // namespace
} // namespace tenzir::plugins::cryptopan

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cryptopan::encrypt_cryptopan)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::cryptopan::decrypt_cryptopan)
