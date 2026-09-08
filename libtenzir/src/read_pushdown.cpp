//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/read_pushdown.hpp"

#include "tenzir/tql2/filter.hpp"

namespace tenzir {

namespace {

class ReadProjectionVisitor final : public ast::visitor<ReadProjectionVisitor> {
public:
  template <class T>
  void visit(T& x) {
    enter(x);
  }

  void visit(ast::function_call&) {
    // Function implementations can inspect their evaluation context as well
    // as explicit arguments. Retain the full schema for such predicates.
    full_schema = true;
  }

  bool full_schema = false;
};

} // namespace

auto read_projection(Option<ir::OptimizeProjection> projection,
                     ir::OptimizeFilter const& filter)
  -> Option<std::vector<std::string>> {
  for (auto const& expr : filter) {
    auto visitor = ReadProjectionVisitor{};
    auto copy = expr;
    visitor.visit(copy);
    if (visitor.full_schema) {
      return {};
    }
    ir::add_refs_to_projection(projection, expr);
  }
  if (not projection) {
    return {};
  }
  auto result = std::vector<std::string>{};
  for (auto const& field : *projection) {
    if (field.path().empty()) {
      return {};
    }
    auto name = field.path().front().id.name;
    if (std::ranges::find(result, name) == result.end()) {
      result.push_back(name);
    }
  }
  return result;
}

auto apply_read_pushdown(table_slice slice, ir::OptimizeFilter const& filter,
                         Option<uint64_t>& remaining, diagnostic_handler& dh)
  -> table_slice {
  for (auto const& expr : filter) {
    if (slice.rows() == 0) {
      break;
    }
    slice = filter2(slice, expr, dh, false);
  }
  if (remaining) {
    slice = head(std::move(slice), *remaining);
    *remaining -= slice.rows();
  }
  return slice;
}

} // namespace tenzir
