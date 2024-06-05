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
#include "tenzir/generator.hpp"
#include "tenzir/offset.hpp"
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

#include <cstddef>
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
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto target_index
        = slice.schema().resolve_key_or_concept_once(input_.inner);
      if (not target_index) {
        co_return diagnostic::error("could not resolve `{}` for schema `{}`",
                                    input_.inner, slice.schema())
          .primary(input_.source)
          .emit(ctrl.diagnostics());
      }
      auto event_target = indexed_table_slice(slice, *target_index);
      if (not event_target) {
        diagnostic::error(event_target.error()).emit(ctrl.diagnostics());
        co_return;
      }
      auto printer_instance
        = printer_->instantiate(event_target->schema(), ctrl);
      for (size_t i = 0; i < slice.rows(); i++) {
        auto sliced_event = subslice(slice, i, i + 1);
        ///
        auto event_arr_0
          = to_record_batch(slice)->ToStructArray().MoveValueUnsafe();
        auto event_as_rb_0
          = arrow::RecordBatch::FromStructArray(event_arr_0).MoveValueUnsafe();
        auto event_0 = table_slice(event_as_rb_0);
        ///
        event_target = indexed_table_slice(event_0, *target_index);
        arrow::StringBuilder builder{arrow::default_memory_pool()};
        for (auto&& chunk : printer_instance->get()->process(*event_target)) {
          auto builder_status
            = builder.Append(reinterpret_cast<const char*>(chunk->data()),
                             static_cast<size_t>(chunk->size()));
          if (not builder_status.ok()) {
            co_return diagnostic::error("could not add contents as string for "
                                        "schema {}",
                                        event_target->schema())
              .primary(input_.source)
              .emit(ctrl.diagnostics());
          }
        }
        std::shared_ptr<arrow::StringArray> str_array;
        auto finish_builder_status = builder.Finish(&str_array);
        if (not finish_builder_status.ok()) {
          co_return diagnostic::error("could not turn data into a string array "
                                      "for "
                                      "schema `{}`",
                                      event_target->schema())
            .primary(input_.source)
            .emit(ctrl.diagnostics());
        }
        auto utf8_status = str_array->ValidateUTF8();
        if (not utf8_status.ok()) {
          co_return diagnostic::error("The {} data format does not write valid "
                                      "UTF8 ",
                                      printer_name_->inner)
            .emit(ctrl.diagnostics());
        }
        auto drop_fn =
          [](struct record_type::field, std::shared_ptr<arrow::Array>) noexcept
          -> std::vector<std::pair<struct record_type::field,
                                   std::shared_ptr<arrow::Array>>> {
          return {};
        };
        auto to_string = [str_array](struct record_type::field field,
                                     std::shared_ptr<arrow::Array>) noexcept
          -> std::vector<std::pair<struct record_type::field,
                                   std::shared_ptr<arrow::Array>>> {
          field.type = type{string_type{}};
          return {
            {field, str_array},
          };
        };
        auto transformations = std::vector<indexed_transformation>{};
        auto indices = collect(slice.schema().resolve(input_.inner));
        for (auto&& index : indices) {
          if (index == target_index) {
            transformations.push_back({std::move(index), to_string});
          } else {
            transformations.push_back({std::move(index), drop_fn});
          }
        }

        co_yield transform_columns(sliced_event, transformations);
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
  static auto
  indexed_table_slice(const table_slice& slice, const offset& target_index)
    -> caf::expected<table_slice> {
    auto [arr_type, arr] = target_index.get(slice);
    const auto* ty = caf::get_if<record_type>(&arr_type);
    if (not ty) {
      return diagnostic::error("invalid record type: {}",
                               arr_type.to_arrow_type()->ToString())
        .to_error();
    }
    auto record_batch_result = arrow::RecordBatch::FromStructArray(arr);
    if (not record_batch_result.ok()) {
      return diagnostic::error("could not convert table slice array into a "
                               "record batch for schema `{}`",
                               slice.schema())
        .to_error();
    }
    auto target_batch = record_batch_result.MoveValueUnsafe();
    record_batch_result = target_batch->ReplaceSchema(
      type{"dummy_head", arr_type}.to_arrow_schema());
    if (not record_batch_result.ok()) {
      return diagnostic::error("could not replace schema {} with the a "
                               "the head {} ",
                               slice.schema(), type{"dummy_head", arr_type})
        .to_error();
    }
    target_batch = record_batch_result.MoveValueUnsafe();
    return table_slice(target_batch);
  }
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
