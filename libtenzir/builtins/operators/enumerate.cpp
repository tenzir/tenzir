//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/option_set.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/escapers.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/error.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>

#include <arrow/type.h>
#include <arrow/type_fwd.h>

#include <unordered_set>

namespace tenzir::plugins::enumerate {

namespace {

class enumerate_operator final : public crtp_operator<enumerate_operator> {
  static constexpr auto default_field_name = "#";

public:
  enumerate_operator() = default;

  explicit enumerate_operator(std::string field) : field_{std::move(field)} {
    if (field_.empty()) {
      field_ = default_field_name;
    }
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto current_type = type{};
    std::unordered_map<type, uint64_t> offsets;
    std::unordered_set<type> skipped_schemas;
    // Create transformation to prepend  column to slice.
    auto transformations = std::vector<indexed_transformation>{};
    auto function = [&](struct record_type::field field,
                        std::shared_ptr<arrow::Array> array) mutable
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
      // Create a column with the RIDs.
      auto n = array->length();
      auto rid_type = uint64_type{};
      auto builder
        = uint64_type::make_arrow_builder(arrow::default_memory_pool());
      auto reserve_result = builder->Reserve(n);
      TENZIR_ASSERT(reserve_result.ok(), reserve_result.ToString().c_str());
      // Fill the column.
      auto& offset = offsets[current_type];
      for (uint64_t i = 0; i < detail::narrow_cast<uint64_t>(n); ++i) {
        auto append_result
          = append_builder(rid_type, *builder, view<uint64_t>{offset + i});
        TENZIR_ASSERT(append_result.ok(), append_result.ToString().c_str());
      }
      offset += n;
      // Replace first column with a pair of (RID, first).
      auto rid_array = builder->Finish().ValueOrDie();
      return {
        {{field_, rid_type}, rid_array},
        {field, array},
      };
    };
    transformations.push_back({offset{0}, std::move(function)});
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
      } else if (skipped_schemas.contains(slice.schema())) {
        co_yield slice;
      } else if (caf::get<record_type>(slice.schema())
                   .resolve_key(field_)
                   .has_value()) {
        diagnostic::warning("ignores schema {} with already existing "
                            "enumeration key {}",
                            slice.schema(), field_)
          .note("from `{}`", name())
          .emit(ctrl.diagnostics());
        skipped_schemas.insert(slice.schema());
        co_yield slice;
      } else {
        current_type = slice.schema();
        co_yield transform_columns(slice, transformations);
      }
    }
  }

  auto name() const -> std::string override {
    return "enumerate";
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return optimize_result{std::nullopt, event_order::ordered, copy()};
  }

private:
  friend auto inspect(auto& f, enumerate_operator& x) -> bool {
    return f.apply(x.field_);
  }

  std::string field_;
};

class enumerate_operator2 final : public crtp_operator<enumerate_operator2> {
public:
  enumerate_operator2() = default;

  explicit enumerate_operator2(ast::simple_selector selector)
    : selector_{std::move(selector)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto idx = int64_t{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto b = int64_type::make_arrow_builder(arrow::default_memory_pool());
      check(b->Reserve(slice.rows()));
      for (auto i = int64_t{}; i < detail::narrow<int64_t>(slice.rows()); ++i) {
        check(b->Append(idx++));
      }
      co_yield assign(selector_, series{int64_type{}, finish(*b)}, slice,
                      ctrl.diagnostics(), true);
    }
  }

  auto name() const -> std::string override {
    return "tql2.enumerate";
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return optimize_result{std::nullopt, event_order::ordered, copy()};
  }

private:
  friend auto inspect(auto& f, enumerate_operator2& x) -> bool {
    return f.apply(x.selector_);
  }

  ast::simple_selector selector_;
};

class plugin final : public virtual operator_plugin<enumerate_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::operator_arg;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = -(required_ws_or_comment >> operator_arg)
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    std::string field;
    if (!p(f, l, field)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "enumerate operator: "
                                                      "'{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<enumerate_operator>(std::move(field)),
    };
  }
};

class plugin2 final : public virtual operator_plugin2<enumerate_operator2> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto selector = ast::simple_selector::try_from(
      ast::root_field{ast::identifier{"#", inv.self.get_location()}});
    TENZIR_ASSERT(selector.has_value());
    TRY(argument_parser2::operator_("enumerate")
          .add(selector, "<selector>")
          .parse(inv, ctx));
    return std::make_unique<enumerate_operator2>(std::move(selector.value()));
  }
};

} // namespace

} // namespace tenzir::plugins::enumerate

TENZIR_REGISTER_PLUGIN(tenzir::plugins::enumerate::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::enumerate::plugin2)
