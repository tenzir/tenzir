#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/tql2/ast.hpp"

#include <algorithm>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::nova {

auto _::EvalRun::eval(const ast::record& x, EvalFrame frame) -> Array<Data> {
  auto const& mask = frame.mask();
  auto const length = frame.length();
  // Only a single spread is supported for now; a record literal with more
  // than one is rejected outright rather than defining a merge order across
  // several spread sources.
  auto const spread_count
    = std::ranges::count_if(x.items, [](auto const& item) {
        return is<ast::spread>(item);
      });
  if (spread_count > 1) {
    diagnostic::error("at most one spread is supported in a record literal")
      .primary(x)
      .emit(frame);
    return frame.null();
  }
  // Evaluate the (optional) spread first, establishing the base record and
  // its shape; literal fields are applied on top of it below.
  auto base = Array<Record>::make_empty(length);
  for (auto const& item : x.items) {
    auto const* spread = try_as<ast::spread>(item);
    if (not spread) {
      continue;
    }
    auto value = frame.eval(spread->expr);
    if (auto record = value.try_as<Record>()) {
      base = std::move(*record);
      continue;
    }
    if (auto* u = try_as<UnionArray>(value)) {
      auto alt = u->get_alternative<Record>();
      if (alt) {
        // The alternative spans every row, but only the rows in `present` are
        // actually records; the others must become empty records here.
        base = std::move(alt->data).empty_where(alt->present.make_inverted());
      }
      if (mask.and_not(u->alternative_mask<Record>())
            .and_not(u->alternative_mask<Null>())
            .any()) {
        diagnostic::warning("expected `record`")
          .primary(spread->expr)
          .emit(frame);
      }
      continue;
    }
    if (not value.try_as<Null>()) {
      diagnostic::warning("expected `record`").primary(spread->expr).emit(frame);
    }
  }
  // Apply literal fields on top of the (possibly spread-derived) base,
  // last-write-wins on duplicate names, matching the legacy evaluator.
  auto fields
    = std::vector<std::pair<std::string_view, MaskedArray<Array<Data>>>>{};
  for (auto const& item : x.items) {
    auto const* field = try_as<ast::record::field>(item);
    if (not field) {
      continue;
    }
    fields.emplace_back(field->name.name, MaskedArray<Array<Data>>{
                                            frame.eval(field->expr), mask});
  }
  auto result = std::move(base).with_fields(std::move(fields));
  return Array<Data>{std::move(result)};
}

} // namespace tenzir::nova
