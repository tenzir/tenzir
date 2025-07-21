//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/modules.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/detail/byteswap.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::compile_schemas {

namespace {

class compile_schemas_operator final
  : public crtp_operator<compile_schemas_operator> {
public:
  auto operator()() const -> generator<chunk_ptr> {
    const auto schemas = modules::expensive_get_all_schemas();
    for (const auto& schema : schemas) {
      const auto bytes = as_bytes(schema);
      const auto uncompressed_size
        = detail::to_network_order(uint64_t{bytes.size()});
      co_yield chunk::copy(&uncompressed_size, sizeof(uncompressed_size));
      auto compressed = check(chunk::compress(bytes));
      const auto compressed_size
        = detail::to_network_order(uint64_t{compressed->size()});
      co_yield chunk::copy(&compressed_size, sizeof(compressed_size));
      co_yield std::move(compressed);
    }
  }

  auto name() const -> std::string override {
    return "_compile_schemas";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter);
    TENZIR_UNUSED(order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, compile_schemas_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin2<compile_schemas_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(argument_parser2::operator_("compile_schemas").parse(inv, ctx));
    return std::make_unique<compile_schemas_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::compile_schemas

TENZIR_REGISTER_PLUGIN(tenzir::plugins::compile_schemas::plugin)
