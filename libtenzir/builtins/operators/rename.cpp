//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/cast.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/convertible/data.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>

#include <arrow/table.h>

#include <algorithm>
#include <string_view>
#include <unordered_map>

namespace tenzir::plugins::rename {

/// The configuration of the rename pipeline operator.
struct configuration {
  struct name_mapping {
    std::string from = {};
    std::string to = {};

    template <class Inspector>
    friend auto inspect(Inspector& f, name_mapping& x) {
      return detail::apply_all(f, x.from, x.to);
    }

    static inline const record_type& schema() noexcept {
      static auto result = record_type{
        {"from", string_type{}},
        {"to", string_type{}},
      };
      return result;
    }
  };

  std::vector<name_mapping> schemas = {};
  std::vector<name_mapping> fields = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return detail::apply_all(f, x.schemas, x.fields);
  }

  static inline const record_type& schema() noexcept {
    // schemas:
    //   - from: zeek.conn
    //     to: zeek.aggregated_conn
    //   - from: suricata.flow
    //     to: suricata.aggregated_flow
    // fields:
    //   - from: resp_h
    //     to: response_h
    static auto result = record_type{
      {"schemas", list_type{name_mapping::schema()}},
      {"fields", list_type{name_mapping::schema()}},
    };
    return result;
  }
};

struct state_t {
  std::vector<indexed_transformation> field_transformations;
  std::optional<type> renamed_schema;
};

class rename_operator final
  : public schematic_operator<rename_operator, state_t> {
public:
  rename_operator() = default;

  rename_operator(configuration config) : config_{std::move(config)} {
    // nop
  }

  auto initialize(const type& schema, operator_control_plane&) const
    -> caf::expected<state_type> override {
    // Step 1: Adjust field names.
    auto field_transformations = std::vector<indexed_transformation>{};
    if (! config_.fields.empty()) {
      for (const auto& field : config_.fields) {
        if (auto index = schema.resolve_key_or_concept_once(field.from)) {
          auto transformation
            = [&](struct record_type::field old_field,
                  std::shared_ptr<arrow::Array> array) noexcept
            -> std::vector<std::pair<struct record_type::field,
                                     std::shared_ptr<arrow::Array>>> {
            return {
              {{field.to, old_field.type}, array},
            };
          };
          field_transformations.push_back(
            {std::move(*index), std::move(transformation)});
        }
      }
      std::sort(field_transformations.begin(), field_transformations.end());
    }
    // Step 2: Adjust schema names.
    std::optional<type> renamed_schema;
    if (! config_.schemas.empty()) {
      const auto schema_mapping
        = std::find_if(config_.schemas.begin(), config_.schemas.end(),
                       [&](const auto& name_mapping) noexcept {
                         return name_mapping.from == schema.name();
                       });
      if (schema_mapping != config_.schemas.end()) {
        auto rename_schema = [&](const concrete_type auto& pruned_schema) {
          TENZIR_ASSERT(! schema.has_attributes());
          return type{schema_mapping->to, pruned_schema};
        };
        renamed_schema = match(schema, rename_schema);
      }
    }
    return state_t{std::move(field_transformations), std::move(renamed_schema)};
  }

  auto process(table_slice slice, state_type& state) const
    -> output_type override {
    slice = transform_columns(slice, state.field_transformations);
    if (state.renamed_schema) {
      slice = cast(std::move(slice), *state.renamed_schema);
    }
    return slice;
  }

  auto name() const -> std::string override {
    return "rename";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, rename_operator& x) -> bool {
    return f.apply(x.config_);
  }

private:
  /// Step-specific configuration, including the schema name mapping.
  configuration config_ = {};
};

struct RenameMapping {
  std::string From = {};
  std::string To = {};

  friend auto inspect(auto& f, RenameMapping& x) -> bool {
    return f.object(x).fields(f.field("from", x.From), f.field("to", x.To));
  }
};

struct RenameState {
  std::vector<indexed_transformation> FieldTransformations = {};
};

auto ToExtractor(const ast::field_path& FieldPath)
  -> std::optional<std::string> {
  if (FieldPath.path().empty()) {
    return std::nullopt;
  }
  auto Result = std::string{};
  for (const auto& Segment : FieldPath.path()) {
    if (Segment.has_question_mark) {
      return std::nullopt;
    }
    if (not Result.empty()) {
      Result += ".";
    }
    Result += Segment.id.name;
  }
  return Result;
}

auto TryMakeFieldMapping(const ast::expression& Expression)
  -> std::optional<RenameMapping> {
  const auto* Assignment = try_as<ast::assignment>(Expression);
  if (not Assignment) {
    return std::nullopt;
  }
  const auto* ToField = try_as<ast::field_path>(Assignment->left);
  auto FromField = ast::field_path::try_from(Assignment->right);
  if (not ToField or not FromField) {
    return std::nullopt;
  }
  auto To = ToExtractor(*ToField);
  auto From = ToExtractor(*FromField);
  if (not To or not From) {
    return std::nullopt;
  }
  return RenameMapping{
    .From = std::move(*From),
    .To = std::move(*To),
  };
}

class Rename final : public Operator<table_slice, table_slice> {
public:
  explicit Rename(std::vector<RenameMapping> FieldMappings)
    : FieldMappings_{std::move(FieldMappings)} {
  }

  auto process(table_slice Input, Push<table_slice>& Push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    auto Iter = StateBySchema_.find(Input.schema());
    if (Iter == StateBySchema_.end()) {
      Iter = StateBySchema_.emplace(Input.schema(), MakeState(Input.schema()))
               .first;
    }
    auto Output
      = transform_columns(std::move(Input), Iter->second.FieldTransformations);
    co_await Push(std::move(Output));
  }

private:
  auto MakeState(const type& Schema) const -> RenameState {
    auto FieldTransformations = std::vector<indexed_transformation>{};
    FieldTransformations.reserve(FieldMappings_.size());
    for (const auto& Mapping : FieldMappings_) {
      auto Index = Schema.resolve_key_or_concept_once(Mapping.From);
      if (not Index) {
        continue;
      }
      auto Transformation
        = [To = Mapping.To](struct record_type::field OldField,
                            std::shared_ptr<arrow::Array> Array) noexcept
        -> std::vector<
          std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
        return {
          {{To, OldField.type}, std::move(Array)},
        };
      };
      FieldTransformations.push_back(
        {std::move(*Index), std::move(Transformation)});
    }
    std::sort(FieldTransformations.begin(), FieldTransformations.end());
    return RenameState{
      .FieldTransformations = std::move(FieldTransformations),
    };
  }

  std::vector<RenameMapping> FieldMappings_ = {};
  std::unordered_map<type, RenameState> StateBySchema_ = {};
};

class RenameIr final : public ir::Operator {
public:
  RenameIr() = default;

  RenameIr(std::vector<RenameMapping> FieldMappings, location Self)
    : FieldMappings_{std::move(FieldMappings)}, Self_{Self} {
  }

  auto name() const -> std::string override {
    return "RenameIr";
  }

  auto main_location() const -> location override {
    return Self_;
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TENZIR_UNUSED(ctx, instantiate);
    return {};
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    if (not input.is<table_slice>()) {
      diagnostic::error("`rename` expects events as input")
        .primary(main_location())
        .emit(dh);
      return failure::promise();
    }
    return tag_v<table_slice>;
  }

  auto spawn(element_type_tag input) && -> AnyOperator override {
    TENZIR_ASSERT(input.is<table_slice>());
    return Rename{std::move(FieldMappings_)};
  }

  friend auto inspect(auto& f, RenameIr& x) -> bool {
    return f.object(x).fields(f.field("field_mappings", x.FieldMappings_),
                              f.field("self", x.Self_));
  }

private:
  std::vector<RenameMapping> FieldMappings_ = {};
  location Self_ = location::unknown;
};

auto ParseFieldMappings(const std::vector<ast::expression>& Args,
                        const ast::entity& Self, diagnostic_handler& Dh)
  -> failure_or<std::vector<RenameMapping>> {
  auto result = std::vector<RenameMapping>{};
  result.reserve(Args.size());
  auto has_error = false;
  for (const auto& arg : Args) {
    auto mapping = TryMakeFieldMapping(arg);
    if (not mapping) {
      diagnostic::error("expected field assignment in the form `<to>=<from>`")
        .primary(arg)
        .emit(Dh);
      has_error = true;
      continue;
    }
    result.push_back(std::move(*mapping));
  }
  if (has_error) {
    return failure::promise();
  }
  if (result.empty()) {
    diagnostic::error("expected at least one field assignment")
      .primary(Self)
      .emit(Dh);
    return failure::promise();
  }
  return result;
}

auto ToConfiguration(std::vector<RenameMapping> mappings) -> configuration {
  auto config = configuration{};
  config.fields.reserve(mappings.size());
  for (auto& mapping : mappings) {
    config.fields.push_back(configuration::name_mapping{std::move(mapping.From),
                                                        std::move(mapping.To)});
  }
  return config;
}

// -- plugin ------------------------------------------------------------------

class plugin final : public virtual operator_plugin<rename_operator>,
                     public virtual operator_factory_plugin,
                     public virtual operator_compiler_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto initialize(const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    // We don't use any plugin-specific configuration under
    // tenzir.plugins.rename, so nothing is needed here.
    if (plugin_config.empty()) {
      return caf::none;
    }
    return caf::make_error(ec::invalid_configuration, "expected empty "
                                                      "configuration under "
                                                      "tenzir.plugins.rename");
  }

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::extractor_assignment_list;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = required_ws_or_comment >> extractor_assignment_list
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    std::vector<std::tuple<std::string, std::string>> parsed_assignments;
    if (! p(f, l, parsed_assignments)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse rename "
                                                      "operator: '{}'",
                                                      pipeline)),
      };
    }
    auto config = configuration{};
    for (const auto& [to, from] : parsed_assignments) {
      if (from.starts_with(':')) {
        config.schemas.push_back({from.substr(1), to});
      } else {
        config.fields.push_back({from, to});
      }
    }
    return {
      std::string_view{f, l},
      std::make_unique<rename_operator>(std::move(config)),
    };
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(auto mappings, ParseFieldMappings(inv.args, inv.self, ctx.dh()));
    return std::make_unique<rename_operator>(
      ToConfiguration(std::move(mappings)));
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<Box<ir::Operator>> override {
    for (auto& arg : inv.args) {
      TRY(arg.bind(ctx));
    }
    auto& dh = static_cast<diagnostic_handler&>(ctx);
    auto self = inv.op;
    TRY(auto mappings, ParseFieldMappings(inv.args, inv.op, dh));
    return RenameIr{std::move(mappings), self.get_location()};
  }
};

} // namespace tenzir::plugins::rename

TENZIR_REGISTER_PLUGIN(tenzir::plugins::rename::plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::ir::Operator,
                            tenzir::plugins::rename::RenameIr>);
