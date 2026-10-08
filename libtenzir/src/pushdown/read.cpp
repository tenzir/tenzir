//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/read.hpp"

#include "tenzir/nova/structured_storage.hpp"
#include "tenzir/tql2/filter.hpp"

#include <string_view>

namespace tenzir::pushdown {

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

auto read_projection_paths(Option<ir::OptimizeProjection> projection,
                           ir::OptimizeFilter const& filter)
  -> Option<ir::OptimizeProjection> {
  for (auto const& expr : filter) {
    auto visitor = ReadProjectionVisitor{};
    auto copy = expr;
    visitor.visit(copy);
    if (visitor.full_schema) {
      return {};
    }
    ir::add_refs_to_projection(projection, expr);
  }
  if (projection and std::ranges::any_of(*projection, [](auto const& field) {
        return field.path().empty();
      })) {
    return {};
  }
  return projection;
}

auto apply_read(table_slice slice, ir::OptimizeFilter const& filter,
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

auto apply_read(nova::Events events, std::span<nova::Evaluator> filters,
                Option<uint64_t>& remaining, diagnostic_handler& dh)
  -> nova::Events {
  for (auto& filter : filters) {
    if (not events.mask.any()) {
      break;
    }
    auto result = filter.eval(events, nova::EvalCtx{dh});
    auto predicate = result.get_alternative<nova::Bool>();
    auto present = predicate ? events.mask & predicate->present
                             : nova::storage::BitMap{events.length(), false};
    // A `null` predicate filters the row out without warning, like `where`:
    // `null` is an absent value, not a type error.
    auto null_predicate = result.get_alternative<nova::Null>();
    auto null_present = null_predicate
                          ? events.mask & null_predicate->present
                          : nova::storage::BitMap{events.length(), false};
    if (events.mask.and_not(present).and_not(null_present).any()) {
      diagnostic::warning("expected `bool`").primary(filter.location()).emit(dh);
    }
    events.mask = predicate
                    ? std::move(present)
                        & as<nova::storage::BitMap>(predicate->data.storage())
                    : nova::storage::BitMap{events.length(), false};
  }
  if (remaining) {
    auto count = detail::narrow<uint64_t>(events.active_count());
    if (count > *remaining) {
      events.mask
        = std::move(events.mask)
            .keep_first(detail::narrow<nova::storage::Index>(*remaining));
      count = *remaining;
    }
    *remaining -= count;
  }
  return events;
}

auto project(nova::Events events, ir::OptimizeProjection const& projection)
  -> nova::Events {
  if (std::ranges::any_of(projection, [](auto const& field) {
        return field.path().empty();
      })) {
    return events;
  }
  auto data = std::move(events.data).to_primary();
  auto const& storage = *as<nova::storage::RecordStorage>(data.storage());
  auto dropped = std::vector<std::string>{};
  for (auto const& [name, _] : storage.names) {
    auto const view = std::string_view{name};
    auto keep = std::ranges::any_of(projection, [&](auto const& field) {
      return field.path().front().id.name == view;
    });
    if (not keep) {
      dropped.emplace_back(view);
    }
  }
  if (not dropped.empty()) {
    auto names = std::vector<std::string_view>{dropped.begin(), dropped.end()};
    // Dropping the fields from every physical row, not only the active ones,
    // lets the record array forget them entirely.
    auto all_rows = nova::storage::BitMap{data.length(), true};
    data = std::move(data).without_fields(names, std::move(all_rows));
  }
  events.data = std::move(data);
  return events;
}

} // namespace tenzir::pushdown
