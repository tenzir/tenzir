//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/aliases.hpp"
#include "tenzir/coder.hpp"
#include "tenzir/query_context.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/table_slice.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/operator_control_plane.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/extension_type.h>

#include <memory>
#include <ranges>
#include <simdjson.h>
#include <string>

namespace tenzir::plugins::print {

namespace {

class print_operator final : public crtp_operator<print_operator> {
public:
  print_operator() = default;

  explicit print_operator(parser_interface& p) {
    const auto* printer = static_cast<const printer_parser_plugin*>(nullptr);
    try {
      auto input = p.accept_shell_arg();
      if (not input) {
        diagnostic::error("expected extractor")
          .primary(p.current_span())
          .throw_();
      }
      input_ = std::move(*input);
      auto printer_name = p.accept_shell_arg();
      if (not printer_name) {
        diagnostic::error("expected parser name")
          .primary(p.current_span())
          .throw_();
      }
      printer_name_ = std::move(*printer_name);
      printer = plugins::find<printer_parser_plugin>(printer_name_->inner);
      if (not printer) {
        diagnostic::error("parser `{}` was not found", printer_name_->inner)
          .primary(printer_name_->source)
          .hint("must be one of: {}",
                fmt::join(std::views::transform(
                            collect(plugins::get<printer_parser_plugin>()),
                            &printer_parser_plugin::name),
                          ", "))
          .throw_();
      }
    } catch (diagnostic& d) {
      std::move(d)
        .modify()
        .usage("print <input> <parser> <args>...")
        .docs("https://docs.tenzir.com/operators/print")
        .throw_();
    }
    TENZIR_ASSERT(printer);
    printer_ = printer->parse_printer(p);
    TENZIR_ASSERT(printer_);
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    //////
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      for (size_t i = 0; i < slice.rows(); i++) {
        auto event = subslice(slice, i, i + 1);
        auto indices = collect(slice.schema().resolve(input_.inner));
        // cut slice into smaller piece
        auto target_index
          = slice.schema().resolve_key_or_concept_once(input_.inner);
        auto [ty, array] = target_index->get(slice);
        TENZIR_WARN(array->ToString());
        auto record_batch
          = arrow::RecordBatch::FromStructArray(array).MoveValueUnsafe();
        auto new_ty = type{"dummy_head", ty};
        TENZIR_WARN(new_ty.name());
        record_batch = record_batch->ReplaceSchema(new_ty.to_arrow_schema())
                         .MoveValueUnsafe(); // issue: only works on records,
                                             // not on raw data types
        auto event_target = table_slice(record_batch);
        auto printer_instance
          = printer_->instantiate(event_target.schema(), ctrl);
        auto slice_as_string = std::string{};
        for (auto&& chunk : printer_instance->get()->process(event_target)) {
          for (std::size_t i = 0; i < chunk->size(); ++i) {
            slice_as_string.push_back(static_cast<char>(chunk->data()[i]));
          }
        }

        ////////
        auto drop_fn = [&](struct record_type::field,
                           std::shared_ptr<arrow::Array>) noexcept
          -> std::vector<std::pair<struct record_type::field,
                                   std::shared_ptr<arrow::Array>>> {
          return {};
        };
        auto to_string
          = [slice_as_string](struct record_type::field field,
                              std::shared_ptr<arrow::Array>) noexcept
          -> std::vector<std::pair<struct record_type::field,
                                   std::shared_ptr<arrow::Array>>> {
          TENZIR_WARN(slice_as_string);
          arrow::StringBuilder builder{arrow::default_memory_pool()};
          auto x = builder.Append(slice_as_string);
          std::shared_ptr<arrow::Array> str_array;
          x = builder.Finish(&str_array);
          field.type = type{string_type{}};
          return {
            {field, str_array},
          };
        };

        ///////
        auto transformations = std::vector<indexed_transformation>{};
        for (auto&& index : indices) {
          if (index == target_index) {
            transformations.push_back({std::move(index), to_string});
          } else {
            transformations.push_back({std::move(index), drop_fn});
          }
        }
        co_yield transform_columns(event, transformations);
      }
    }
  }

  auto name() const -> std::string override {
    return "print";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, print_operator& x) -> bool {
    // TODO: This could be easier, but `plugin_inspect` does not seem to play
    // well with the `.object()` DSL.
    return f.begin_object(caf::invalid_type_id, "parse_operator")
           && f.begin_field("input") && f.apply(x.input_) && f.end_field()
           && f.begin_field("parser_name") && f.apply(x.printer_name_)
           && f.end_field() && f.begin_field("parser")
           && plugin_inspect(f, x.printer_) && f.end_field() && f.end_object();
  }

private:
  located<std::string> input_;
  std::optional<located<std::string>> printer_name_;
  std::unique_ptr<plugin_printer> printer_;
};

class plugin final : public virtual operator_plugin<print_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    return std::make_unique<class print_operator>(p);
  }
};

} // namespace

} // namespace tenzir::plugins::print

TENZIR_REGISTER_PLUGIN(tenzir::plugins::print::plugin)
