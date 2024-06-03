//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

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

#include <memory>
#include <ranges>
#include <simdjson.h>

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
      // auto original_batch = to_record_batch(slice);
      auto index = slice.schema().resolve_key_or_concept_once(input_.inner);
      if (not index) {
        diagnostic::error("could not resolve `{}` for schema `{}`",
                          input_.inner, slice.schema())
          .primary(input_.source)
          .emit(ctrl.diagnostics());
        co_return;
      }
      auto indices = collect(slice.schema().resolve(input_.inner));
      auto [ty, array] = index->get(slice);
      ty = type{"dummy_head", ty};
      auto record_batch_result = arrow::RecordBatch::FromStructArray(array);
      if (not record_batch_result.ok()) {
        diagnostic::error("could not create record batch for schema `{}`",
                          slice.schema())
          .primary(input_.source)
          .emit(ctrl.diagnostics());
        co_return;
      }
      auto record_batch = record_batch_result.MoveValueUnsafe();
      auto updated_record_batch
        = record_batch->ReplaceSchema(ty.to_arrow_schema());
      if (not updated_record_batch.ok()) {
        diagnostic::error("could not update schema of record batch `{}`",
                          slice.schema())
          .primary(input_.source)
          .emit(ctrl.diagnostics());
        co_return;
      }
      auto segmented_table_slice
        = table_slice{updated_record_batch.MoveValueUnsafe(), ty};
      auto printer_instance
        = printer_->instantiate(segmented_table_slice.schema(), ctrl);
      for (size_t i = 0; i < segmented_table_slice.rows(); i++) {
        auto event = subslice(segmented_table_slice, i, i + 1);
        auto slice_as_string = std::string{};
        for (auto&& chunk : printer_instance->get()->process(event)) {
          for (std::size_t i = 0; i < chunk->size(); ++i) {
            slice_as_string.push_back(static_cast<char>(chunk->data()[i]));
          }
        }
        // auto buffer = make_shared<arrow::Buffer>(slice_as_string);
        // // auto offsets =
        // arrow::Buffer{reinterpret_cast<char*>(index->data()),
        // //                              index->size()};
        // auto offsets = make_shared<arrow::Buffer>(
        //   std::string{index->begin(), index->end()});

        // auto string_array
        //   = arrow::StringArray(slice_as_string.length(), offsets, buffer);
        arrow::StringBuilder builder{};
        arrow::Status status = builder.Append(slice_as_string);
        if (not status.ok()) {
          diagnostic::error("dskjfposdfpsod `{}`", slice.schema())
            .primary(input_.source)
            .emit(ctrl.diagnostics());
          co_return;
        }
        // Finalize the builder to create the StringArray
        std::shared_ptr<arrow::Array> array;
        status = builder.Finish(&array);
        if (not status.ok()) {
          diagnostic::error("dssdsdsccc `{}`", slice.schema())
            .primary(input_.source)
            .emit(ctrl.diagnostics());
          co_return;
        }

        // auto utf8_validation = string_array.ValidateUTF8();
        // // TODO: improve error message
        // if (!utf8_validation.ok()) {
        //   diagnostic::error("the provided format does not produce utf8
        //   output")
        //     .primary(input_.source)
        //     .emit(ctrl.diagnostics());
        //   co_return;
        // }
        auto drop_fn = [&](struct record_type::field,
                           std::shared_ptr<arrow::Array>) noexcept
          -> std::vector<std::pair<struct record_type::field,
                                   std::shared_ptr<arrow::Array>>> {
          return {};
        };

        auto add_string_fn = [&](struct record_type::field field,
                                 std::shared_ptr<arrow::Array>) noexcept
          -> std::vector<std::pair<struct record_type::field,
                                   std::shared_ptr<arrow::Array>>> {
          static const auto options
            = arrow::compute::TrimOptions{" \t\n\v\f\r"};
          auto trimmed_array
            = arrow::compute::CallFunction("utf8_trim", {array}, &options);
          if (not trimmed_array.ok()) {
            diagnostic::error("{}", trimmed_array.status().ToString())
              .primary(input_.source)
              .throw_();
          }
          return {{field, trimmed_array.MoveValueUnsafe().make_array()}};
        };
        auto transformations = std::vector<indexed_transformation>{};
        int j = 0;
        for (auto index : indices) {
          // if (j == 0) {
          //   transformations.push_back({std::move(index), add_string_fn});
          //   j++;
          // } else {
          transformations.push_back({std::move(index), drop_fn});
          //}
        }
        // // transform_columns requires the transformations to be sorted, and
        // that
        // // may not necessarily be true if we have multiple fields configured,
        // so
        // // we sort again in that case.
        // if (config_.fields.size() > 1) {
        //   std::sort(transformations.begin(), transformations.end());
        // }
        // transformations.erase(std::unique(transformations.begin(),
        //                                   transformations.end()),
        //                       transformations.end());
        co_yield transform_columns(std::move(slice),
                                   std::move(transformations));
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
