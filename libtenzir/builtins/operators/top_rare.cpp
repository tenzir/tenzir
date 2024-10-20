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

enum class mode { top, rare };

class top_rare_instance final : public aggregation_instance {
public:
  explicit top_rare_instance(enum mode mode, ast::expression expr)
    : mode_{mode}, expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    auto arg = eval(expr_, input, ctx);
    if (caf::holds_alternative<null_type>(arg.type)) {
      return;
    }
    for (int64_t i = 0; i < arg.array->length(); ++i) {
      if (arg.array->IsValid(i)) {
        const auto& view = value_at(arg.type, *arg.array, i);
        auto it = counts_.find(view);
        if (it == counts_.end()) {
          counts_.emplace_hint(it, materialize(view), 0);
          continue;
        }
        ++it.value();
        return;
      }
    }
  }

  auto finish() -> data override {
    const auto comp = [](const auto& lhs, const auto& rhs) {
      return lhs.second < rhs.second;
    };
    const auto it = mode_ == mode::top
                      ? std::ranges::max_element(counts_, comp)
                      : std::ranges::min_element(counts_, comp);
    if (it == counts_.end()) {
      return {};
    }
    return it->first;
  }

private:
  const mode mode_ = {};
  const ast::expression expr_ = {};
  tsl::robin_map<data, int64_t> counts_ = {};
};

template <mode Mode>
class top_rare_plugin final : public virtual operator_parser_plugin,
                              public virtual operator_factory_plugin,
                              public virtual aggregation_plugin {
  auto name() const -> std::string override {
    return Mode == mode::top ? "top" : "rare";
  }

  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/operators/{}", name())};
    auto field = located<std::string>{};
    auto count_field = std::optional<located<std::string>>{};
    parser.add(field, "<str>");
    parser.add("-c,--count-field", count_field, "<str>");
    parser.parse(p);
    if (count_field) {
      if (count_field->inner.empty()) {
        diagnostic::error("`--count-field` must not be empty")
          .primary(count_field->source)
          .throw_();
      }
      if (count_field->inner == field.inner) {
        diagnostic::error("invalid duplicate field value `{}` for count and "
                          "value fields",
                          field.inner)
          .primary(field.source)
          .primary(count_field->source)
          .throw_();
      }
    } else {
      if (field.inner == default_count_field) {
        diagnostic::error("invalid duplicate field value `{}` for count and "
                          "value fields",
                          field.inner)
          .primary(field.source)
          .throw_();
      } else {
        count_field.emplace();
        count_field->inner = default_count_field;
      }
    }
    // TODO: Replace this textual parsing with a subpipeline to improve
    // diagnostics for this operator.
    auto repr = fmt::format("summarize {0}=count(.) by {1} | sort {0} {2}",
                            count_field->inner, field.inner,
                            Mode == mode::top ? "desc" : "asc");
    auto parsed = pipeline::internal_parse_as_operator(repr);
    if (not parsed) {
      // TODO: Improve error message.
      diagnostic::error(parsed.error()).throw_();
    }
    return std::move(*parsed);
  }

  auto make(operator_factory_plugin::invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto selector = ast::simple_selector{};
    const auto loc = inv.self.get_location();
    TRY(argument_parser2::operator_(name())
          .add(selector, "<field>")
          .parse(inv, ctx));
    const auto* summarize
      = plugins::find<operator_factory_plugin>("tql2.summarize");
    const auto* sort = plugins::find<operator_factory_plugin>("tql2.sort");
    TENZIR_ASSERT(summarize);
    TENZIR_ASSERT(sort);
    auto ident = ast::identifier{"count", loc};
    auto call = ast::function_call{std::nullopt, ast::entity{{ident}}, {}, loc};
    auto out
      = ast::simple_selector::try_from(ast::root_field{std::move(ident)});
    TENZIR_ASSERT(out);
    auto summarize_args = ast::assignment{out.value(), loc, call};
    TENZIR_ASSERT(resolve_entities(summarize_args.right, ctx));
    auto summarized = summarize->make(
      {
        inv.self,
        {summarize_args, std::move(selector).unwrap()},
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

  auto make_aggregation(aggregation_plugin::invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name()).add(expr, "<expr>").parse(inv, ctx));
    return std::make_unique<top_rare_instance>(Mode, std::move(expr));
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
