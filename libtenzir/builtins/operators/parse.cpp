//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/api.h>

#include <memory>
#include <ranges>

namespace tenzir::plugins::parse {

namespace {

class parse_operator final : public crtp_operator<parse_operator> {
public:
  parse_operator() = default;

  explicit parse_operator(parser_interface& p) {
    auto parser = static_cast<const parser_parser_plugin*>(nullptr);
    try {
      auto input = p.accept_shell_arg();
      if (not input) {
        diagnostic::error("expected extractor")
          .primary(p.current_span())
          .throw_();
      }
      input_ = std::move(*input);
      auto parser_name_ = p.accept_shell_arg();
      if (not parser_name_) {
        diagnostic::error("expected parser name")
          .primary(p.current_span())
          .throw_();
      }
      parser = plugins::find<parser_parser_plugin>(parser_name_->inner);
      if (not parser) {
        diagnostic::error("parser `{}` was not found", parser_name_->inner)
          .primary(parser_name_->source)
          .hint("must be one of: {}",
                fmt::join(std::views::transform(
                            collect(plugins::get<parser_parser_plugin>()),
                            &parser_parser_plugin::name),
                          ", "))
          .throw_();
      }
    } catch (diagnostic& d) {
      std::move(d)
        .modify()
        .usage("parse <input> <parser> <args>...")
        .docs("https://docs.tenzir.com/operators/parse")
        .throw_();
    }
    TENZIR_ASSERT(parser);
    parser_ = parser->parse_parser(p);
    TENZIR_ASSERT(parser_);
  }

  auto operator()(generator<table_slice> input, exec_ctx ctx) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto batch = to_record_batch(slice);
      auto schema = caf::get<record_type>(slice.schema());
      auto index = slice.schema().resolve_key_or_concept_once(input_.inner);
      if (not index) {
        diagnostic::error("could not resolve `{}` for schema `{}`",
                          input_.inner, slice.schema())
          .primary(input_.source)
          .emit(ctrl.diagnostics());
        co_return;
      }
      auto [ty, array] = index->get(slice);
      if (ty.kind().is_not<string_type>()) {
        diagnostic::error("expected `string`, but got `{}` for schema `{}`",
                          ty.kind(), slice.schema())
          .primary(input_.source)
          .emit(ctrl.diagnostics());
        co_return;
      }
      auto string_array = std::dynamic_pointer_cast<arrow::StringArray>(array);
      TENZIR_ASSERT(string_array);
      auto results = parser_->parse_strings(string_array, ctrl);
      auto total = int64_t{0};
      for (auto& [result_ty, result] : results) {
        TENZIR_ASSERT(result_ty.to_arrow_type()->Equals(result->type()));
        total += result->length();
      }
      if (total == 0) {
        // TODO: Ideally, we should be able to recover from here a little more
        // gracefully.
        diagnostic::error("parsing failed")
          .primary(input_.source)
          .emit(ctrl.diagnostics());
        co_return;
      }
      TENZIR_ASSERT(total == string_array->length());
      const auto children
        = batch->ToStructArray().ValueOrDie()->Flatten().ValueOrDie();
      auto next = int64_t{0};
      for (auto& [result_ty, result] : results) {
        auto sub = subslice(slice, next, next + result->length());
        next += result->length();
        auto final_result = transform_columns(
          sub, {
                 {
                   *index,
                   [&](struct record_type::field field,
                       const std::shared_ptr<arrow::Array>&)
                     -> indexed_transformation::result_type {
                     field.type = result_ty;
                     return {{std::move(field), result}};
                   },
                 },
               });
        co_yield std::move(final_result);
      }
    }
  }

  auto name() const -> std::string override {
    return "parse";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, parse_operator& x) -> bool {
    // TODO: This could be easier, but `plugin_inspect` does not seem to play
    // well with the `.object()` DSL.
    return f.begin_object(caf::invalid_type_id, "parse_operator")
           && f.begin_field("input") && f.apply(x.input_) && f.end_field()
           && f.begin_field("parser_name") && f.apply(x.parser_name_)
           && f.end_field() && f.begin_field("parser")
           && plugin_inspect(f, x.parser_) && f.end_field() && f.end_object();
  }

private:
  located<std::string> input_;
  std::optional<located<std::string>> parser_name_;
  std::unique_ptr<plugin_parser> parser_;
};

class plugin final : public virtual operator_plugin<parse_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    return std::make_unique<class parse_operator>(p);
  }
};

} // namespace

} // namespace tenzir::plugins::parse

TENZIR_REGISTER_PLUGIN(tenzir::plugins::parse::plugin)
