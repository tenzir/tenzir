//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/convertible/data.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>

#include <arrow/type.h>
#include <fmt/format.h>

namespace tenzir::plugins::drop {

namespace {

/// The configuration of a project pipeline operator.
struct configuration {
  /// The key suffixes of the fields to drop.
  std::vector<std::string> fields = {};

  /// The key suffixes of the schemas to drop.
  std::vector<std::string> schemas = {};

  /// Support type inspection for easy parsing with convertible.
  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return detail::apply_all(f, x.fields, x.schemas);
  }

  /// Enable parsing from a record via convertible.
  static inline const record_type& schema() noexcept {
    static auto result = record_type{
      {"fields", list_type{string_type{}}},
      {"schemas", list_type{string_type{}}},
    };
    return result;
  }
};

/// Drops the specifed fields from the input.
class drop_operator final
  : public schematic_operator<
      drop_operator, std::optional<std::vector<indexed_transformation>>> {
public:
  drop_operator() = default;

  explicit drop_operator(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  auto initialize(const type& schema, exec_ctx) const
    -> caf::expected<state_type> override {
    // Determine whether we want to drop the entire batch first.
    const auto drop_schema
      = std::any_of(config_.schemas.begin(), config_.schemas.end(),
                    [&](const auto& dropped_schema) {
                      return dropped_schema == schema.name();
                    });
    if (drop_schema)
      return std::nullopt;

    // Apply the transformation.
    auto transform_fn
      = [&](struct record_type::field, std::shared_ptr<arrow::Array>) noexcept
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
      return {};
    };
    auto transformations = std::vector<indexed_transformation>{};
    for (const auto& field : config_.fields) {
      for (auto index : schema.resolve(field)) {
        transformations.push_back({std::move(index), transform_fn});
      }
    }
    // transform_columns requires the transformations to be sorted, and that may
    // not necessarily be true if we have multiple fields configured, so we sort
    // again in that case.
    if (config_.fields.size() > 1)
      std::sort(transformations.begin(), transformations.end());
    transformations.erase(std::unique(transformations.begin(),
                                      transformations.end()),
                          transformations.end());
    return transformations;
  }

  /// Processes a single slice with the corresponding schema-specific state.
  auto process(table_slice slice, state_type& state) const
    -> output_type override {
    if (state) {
      return transform_columns(slice, *state);
    }
    return {};
  }

  auto name() const -> std::string override {
    return "drop";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, drop_operator& x) -> bool {
    return f.apply(x.config_);
  }

private:
  /// The underlying configuration of the transformation.
  configuration config_;
};

class plugin final : public virtual operator_plugin<drop_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::extractor_list;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = required_ws_or_comment >> extractor_list
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto config = configuration{};
    if (!p(f, l, config.fields)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse drop "
                                                      "operator: '{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<drop_operator>(std::move(config)),
    };
  }
};

class drop_operator2 final : public crtp_operator<drop_operator2> {
public:
  drop_operator2() = default;

  explicit drop_operator2(std::vector<ast::simple_selector> selectors)
    : selectors_{std::move(selectors)} {
  }

  auto name() const -> std::string override {
    return "tql2.drop";
  }

  auto operator()(generator<table_slice> input, exec_ctx ctx) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto transformations = std::vector<indexed_transformation>{};
      for (auto& sel : selectors_) {
        auto resolved = resolve(sel, slice.schema());
        std::move(resolved).match(
          [&](offset off) {
            transformations.emplace_back(
              std::move(off),
              [](struct record_type::field, std::shared_ptr<arrow::Array>) {
                return indexed_transformation::result_type{};
              });
          },
          [&](resolve_error err) {
            err.reason.match(
              [&](resolve_error::field_not_found&) {
                diagnostic::warning("could not find field `{}`", err.ident.name)
                  .primary(err.ident)
                  .emit(ctrl.diagnostics());
              },
              [&](resolve_error::field_of_non_record& reason) {
                diagnostic::warning("type `{}` has no field field `{}`",
                                    reason.type.kind(), err.ident.name)
                  .primary(err.ident)
                  .emit(ctrl.diagnostics());
              });
          });
      }
      std::ranges::sort(transformations);
      co_yield transform_columns(slice, transformations);
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, drop_operator2& x) -> bool {
    return f.apply(x.selectors_);
  }

private:
  std::vector<ast::simple_selector> selectors_;
};

class plugin2 final : public virtual operator_plugin2<drop_operator2> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_("drop");
    auto selectors = std::vector<ast::simple_selector>{};
    for (auto& arg : inv.args) {
      auto selector = ast::simple_selector::try_from(arg);
      if (selector) {
        selectors.push_back(std::move(*selector));
      } else {
        // TODO: Improve error message.
        diagnostic::error("expected simple selector")
          .primary(arg)
          .usage(parser.usage())
          .docs(parser.docs())
          .emit(ctx.dh());
      }
    }
    return std::make_unique<drop_operator2>(std::move(selectors));
  }
};

} // namespace

} // namespace tenzir::plugins::drop

TENZIR_REGISTER_PLUGIN(tenzir::plugins::drop::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::drop::plugin2)
