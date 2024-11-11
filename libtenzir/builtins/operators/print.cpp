//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/argument_parser.hpp"
#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/collect.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/generator.hpp"
#include "tenzir/operator_control_plane.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/type.hpp"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/extension_type.h>
#include <arrow/util/utf8.h>
#include <caf/sum_type.hpp>

#include <cstddef>
#include <iterator>
#include <memory>
#include <ranges>
#include <string_view>

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
        diagnostic::error("expected printer name")
          .primary(p.current_span())
          .throw_();
      }
      printer_name_ = std::move(*printer_name);
      printer = plugins::find<printer_parser_plugin>(printer_name_->inner);
      if (not printer) {
        diagnostic::error("printer `{}` was not found", printer_name_->inner)
          .primary(printer_name_->source)
          .hint("must be one of: {}",
                fmt::join(std::views::transform(
                            collect(plugins::get<printer_parser_plugin>()),
                            &printer_parser_plugin::name),
                          ", "))
          .throw_();
      }
      TENZIR_ASSERT(printer);
      printer_ = printer->parse_printer(p);
      TENZIR_ASSERT(printer_);
      if (not printer_->prints_utf8()) {
        diagnostic::error("print operator does not support binary formats")
          .primary(printer_name_->source)
          .throw_();
      }
    } catch (diagnostic& d) {
      std::move(d)
        .modify()
        .usage("print <input> <printer> <args>...")
        .docs("https://docs.tenzir.com/operators/print")
        .throw_();
    }
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
        diagnostic::error("could not resolve `{}` for schema `{}`",
                          input_.inner, slice.schema())
          .primary(input_.source)
          .throw_();
      }
      auto transform = [&](struct record_type::field field,
                           std::shared_ptr<arrow::Array> array)
        -> std::vector<
          std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
        if (not caf::holds_alternative<record_type>(field.type)) {
          diagnostic::error("field {} is not of type record", field.name)
            .primary(input_.source)
            .throw_();
        }
        field.type = type{
          fmt::format("{}.{}", slice.schema().name(),
                      as<record_type>(slice.schema()).key(*target_index)),
          field.type,
        };
        auto rb = arrow::RecordBatch::Make(
          field.type.to_arrow_schema(), array->length(),
          static_cast<const arrow::StructArray&>(*array).Flatten().ValueOrDie());
        auto slice = table_slice{rb, field.type};
        auto builder = series_builder{type{string_type{}}};
        for (size_t i = 0; i < slice.rows(); i++) {
          auto row = subslice(slice, i, i + 1);
          auto chunks = std::vector<chunk_ptr>{};
          try {
            auto printer_instance = printer_->instantiate(field.type, ctrl);
            std::ranges::copy(printer_instance->get()->process(row),
                              std::back_inserter(chunks));
            std::ranges::copy(printer_instance->get()->finish(),
                              std::back_inserter(chunks));
            std::erase_if(chunks, [](const chunk_ptr& chunk) {
              return not chunk or chunk->size() == 0;
            });
          } catch (diagnostic diag) {
            std::move(diag)
              .modify()
              .severity(severity::warning)
              .emit(ctrl.diagnostics());
            builder.null();
            continue;
          }
          auto validate_and_add = [&](std::string_view str) {
            TENZIR_ASSERT_EXPENSIVE(arrow::util::ValidateUTF8(str));
            TENZIR_ASSERT(not str.empty());
            if (str.back() == '\n') {
              str.remove_suffix(1);
            }
            builder.data(str);
          };
          if (chunks.empty()) {
            builder.data("");
          } else if (chunks.size() == 1) {
            const auto* data = reinterpret_cast<const char*>(chunks[0]->data());
            validate_and_add({data, data + chunks[0]->size()});
          } else {
            auto buffer = std::vector<char>{};
            for (auto&& chunk : chunks) {
              const auto* data = reinterpret_cast<const char*>(chunk->data());
              buffer.insert(buffer.end(), data, data + chunk->size());
            }
            validate_and_add({buffer.data(), buffer.data() + buffer.size()});
          }
        }
        auto series = builder.finish_assert_one_array();
        return {{
          {field.name, series.type},
          series.array,
        }};
      };
      auto transformations = std::vector<indexed_transformation>{
        {*target_index, transform},
      };
      co_yield transform_columns(slice, transformations);
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
    return f.begin_object(caf::invalid_type_id, "print_operator")
           && f.begin_field("input") && f.apply(x.input_) && f.end_field()
           && f.begin_field("printer_name") && f.apply(x.printer_name_)
           && f.end_field() && f.begin_field("printer")
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
    return std::make_unique<print_operator>(p);
  }
};

} // namespace

} // namespace tenzir::plugins::print

TENZIR_REGISTER_PLUGIN(tenzir::plugins::print::plugin)
