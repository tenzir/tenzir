//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/error.hpp>
#include <tenzir/location.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/resolve.hpp>

namespace tenzir::plugins::top_rare {

namespace {

enum class mode {
  top,
  rare,
};

template <mode Mode>
class top_rare_plugin final : public virtual operator_factory_plugin {
  auto name() const -> std::string override {
    return Mode == mode::top ? "top" : "rare";
  }

  auto make(operator_factory_plugin::invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto selector = ast::field_path{};
    const auto loc = inv.self.get_location();
    TRY(argument_parser2::operator_(name())
          .positional("x", selector)
          .parse(inv, ctx));
    const auto* summarize
      = plugins::find<operator_factory_plugin>("tql2.summarize");
    const auto* sort = plugins::find<operator_factory_plugin>("tql2.sort");
    TENZIR_ASSERT(summarize);
    TENZIR_ASSERT(sort);
    auto ident = ast::identifier{"count", loc};
    auto call = ast::function_call{ast::entity{{ident}}, {}, loc, false};
    auto out = ast::field_path::try_from(ast::root_field{std::move(ident)});
    TENZIR_ASSERT(out);
    auto summarize_args = ast::assignment{out.value(), loc, call};
    TENZIR_ASSERT(resolve_entities(summarize_args.right, ctx));
    auto summarized = summarize->make(
      {
        inv.self,
        {
          std::move(selector).unwrap(),
          summarize_args,
        },
      },
      ctx);
    const auto sort_args = [&]() {
      if constexpr (Mode == mode::top) {
        return ast::unary_expr{{ast::unary_op::neg, loc},
                               std::move(out).value().unwrap()};
      }
      if constexpr (Mode == mode::rare) {
        return std::move(out).value().unwrap();
      }
      TENZIR_UNREACHABLE();
    };
    auto sorted = sort->make({inv.self, {sort_args()}}, ctx);
    TENZIR_ASSERT(summarized);
    TENZIR_ASSERT(sorted);
    auto p = std::make_unique<pipeline>();
    p->append(std::move(summarized).unwrap());
    p->append(std::move(sorted).unwrap());
    return p;
  }

private:
  static constexpr auto default_count_field = "count";
};

using top_plugin = top_rare_plugin<mode::top>;
using rare_plugin = top_rare_plugin<mode::rare>;

} // namespace

} // namespace tenzir::plugins::top_rare

TENZIR_REGISTER_PLUGIN(tenzir::plugins::top_rare::top_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::top_rare::rare_plugin)
