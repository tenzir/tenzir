//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/operator_control_plane.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir {

/// Creates a record that maps `path` to `value`.
///
/// # Examples
//
/// ["foo", "bar"] -> {"foo": {"bar": value}}
/// [] -> value
[[nodiscard]] auto consume_path(std::span<const ast::field_path::segment> path,
                                series value) -> series;

enum class assign_position {
  front,
  back,
};

[[nodiscard]] auto assign(std::span<const ast::field_path::segment> left,
                          series right, series input, diagnostic_handler& dh,
                          assign_position position) -> series;

[[nodiscard]] auto
assign(const ast::selector& left, series right, const table_slice& input,
       diagnostic_handler& dh, assign_position position = assign_position::back)
  -> std::vector<table_slice>;

[[nodiscard]] auto
assign(const ast::field_path& left, series right, const table_slice& input,
       diagnostic_handler& dh, assign_position position = assign_position::back)
  -> table_slice;

[[nodiscard]] auto assign(const ast::meta& left, const series& right,
                          const table_slice& input, diagnostic_handler& diag)
  -> std::vector<table_slice>;

[[nodiscard]] auto resolve_move_keyword(ast::assignment assignment)
  -> std::pair<ast::assignment, std::vector<ast::field_path>>;

[[nodiscard]] auto
drop(const table_slice& slice, std::span<const ast::field_path> fields,
     diagnostic_handler& dh, bool warn_for_duplicates) -> table_slice;

class set_operator final : public crtp_operator<set_operator> {
public:
  set_operator() = default;

  explicit set_operator(std::vector<ast::assignment> assignments)
    : assignments_{std::move(assignments)} {
    for (auto& assignment : assignments_) {
      auto [pruned_assignment, moved_fields]
        = resolve_move_keyword(std::move(assignment));
      assignment = std::move(pruned_assignment);
      std::ranges::move(moved_fields, std::back_inserter(moved_fields_));
    }
  }

  auto name() const -> std::string override {
    return "tql2.set";
  }

  auto operator()(generator<table_slice> input,
                  operator_control_plane& ctrl) const -> generator<table_slice>;

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter);
    auto replacement = std::make_unique<set_operator>(*this);
    replacement->order_ = order;
    return optimize_result{std::nullopt, order, std::move(replacement)};
  }

  friend auto inspect(auto& f, set_operator& x) -> bool {
    return f.object(x).fields(f.field("assignments", x.assignments_),
                              f.field("moved_fields", x.moved_fields_),
                              f.field("order", x.order_));
  }

private:
  std::vector<ast::assignment> assignments_;
  std::vector<ast::field_path> moved_fields_;
  event_order order_ = event_order::ordered;
};

} // namespace tenzir
