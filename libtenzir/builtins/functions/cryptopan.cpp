//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/multi_series.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/api.h>
#include <arrow/type_fwd.h>

namespace tenzir::plugins::cryptopan {

namespace {

class encrypt_cryptopan : public virtual function_plugin {
  auto name() const -> std::string override {
    return "encrypt_cryptopan";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto seed = std::optional<std::string>{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "ip")
          .named("seed", seed)
          .parse(inv, ctx));
    auto seed_bytes
      = std::array<ip::byte_type,
                   tenzir::ip::pseudonymization_seed_array_size>{};
    if (seed.has_value()) {
      auto max_seed_size = std::min(
        tenzir::ip::pseudonymization_seed_array_size * 2, seed->size());
      for (auto i = size_t{0}; (i * 2) < max_seed_size; ++i) {
        auto byte_string_pos = i * 2;
        auto byte_size = (byte_string_pos + 2 > seed->size()) ? 1 : 2;
        auto byte = seed->substr(byte_string_pos, byte_size);
        if (byte_size == 1) {
          byte.append("0");
        }
        TENZIR_ASSERT(i < seed_bytes.size());
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
        seed_bytes[i] = std::strtoul(byte.c_str(), nullptr, 16);
      }
    }
    return function_use::make([expr = std::move(expr),
                               seed = seed_bytes](evaluator eval, session ctx) {
      return map_series(eval(expr), [&](series s) {
        if (is<null_type>(s.type)) {
          return series::null(ip_type{}, s.length());
        }
        auto typed_series = s.as<ip_type>();
        if (not typed_series) {
          diagnostic::warning("expected type `ip`, got `{}`", s.type.kind())
            .primary(expr)
            .emit(ctx);
          return series::null(ip_type{}, s.length());
        }
        auto b = ip_type::make_arrow_builder(arrow_memory_pool());
        for (const auto& value : typed_series->values()) {
          if (not value) {
            check(b->AppendNull());
            continue;
          }
          check(append_builder(ip_type{}, *b,
                               tenzir::ip::pseudonymize(value.value(), seed)));
        }
        return series{ip_type{}, finish(*b)};
      });
    });
  }
};

} // namespace
} // namespace tenzir::plugins::cryptopan

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cryptopan::encrypt_cryptopan)
