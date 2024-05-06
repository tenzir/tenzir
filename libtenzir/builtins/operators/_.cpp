//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pipeline.hpp"
#include "tenzir/plugin.hpp"

#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::_ {

namespace ast = tql2::ast;

namespace {

class read_json final : public crtp_operator<read_json> {
public:
  read_json() = default;

  auto name() const -> std::string override {
    return "tql2.read_json";
  }

  auto operator()(generator<chunk_ptr> input) const -> generator<table_slice> {
    co_return;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, read_json& x) -> bool {
    return f.object(x).fields();
  }
};

class read_json_plugin final
  : public virtual operator_inspection_plugin<read_json>,
    public virtual tql2::operator_factory_plugin {
public:
  auto make_operator(ast::entity self, std::vector<ast::expression> args,
                     tql2::context& ctx) const -> operator_ptr override {
    // --schema
    // > schema="foo"
    // --selector="event_type:suricata"
    // > schema="suricata" + event_type
    // --unnest-separator
    // ? This is perhaps here because of schema definitions?
    // --no-infer
    // > no_extra_fields=true
    // --ndjson
    // > separator="\n" (maybe?)
    // --gelf
    // > separator="\0"
    // --precise
    // precise=true
    // --raw
    // raw=true
    // --arrays-of-objects
    // array_of_objects=true

    // Modes:
    // 1) Schema (with selector)
    // 2) Precise based on type
    // 3) Infer-Almost-Same

    // Typing later:
    // 1) Put all in one.
    // 2) Put in builder based on whole shape.
    // 3) Put in builder based on expression value.

    // read json [--schema <schema>] [--selector <field[:prefix]>]
    //           [--unnest-separator <string>] [--no-infer] [--ndjson]
    //           [--precise] [--raw] [--arrays-of-objects]
    diagnostic::error("ok").primary(self.get_location()).emit(ctx);
    return std::make_unique<read_json>();
  }
};

} // namespace

} // namespace tenzir::plugins::_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::_::read_json_plugin)
