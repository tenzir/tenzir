#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tql2/ast.hpp"

#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::nova {

namespace {

/// Evaluates the spread `expr` into the record array it contributes, or `None`
/// if it contributes nothing. Non-record rows of a union contribute no fields.
/// `null` contributes nothing silently, every other type with a warning.
auto eval_spread(EvalFrame frame, ast::expression const& expr)
  -> Option<Array<Record>> {
  auto const& mask = frame.mask();
  auto value = frame.eval(expr);
  if (auto record = value.try_as<Record>()) {
    return std::move(*record);
  }
  if (auto* u = try_as<UnionArray>(value)) {
    if (mask.and_not(u->alternative_mask<Record>())
          .and_not(u->alternative_mask<Null>())
          .any()) {
      diagnostic::warning("expected `record`").primary(expr).emit(frame);
    }
    auto alt = u->get_alternative<Record>();
    if (not alt) {
      return None{};
    }
    // The alternative spans every row, but only the rows in `present` are
    // actually records; the others must become empty records here.
    return std::move(alt->data).empty_where(alt->present.make_inverted());
  }
  if (not value.try_as<Null>()) {
    diagnostic::warning("expected `record`").primary(expr).emit(frame);
  }
  return None{};
}

} // namespace

auto _::EvalRun::eval(const ast::record& x, EvalFrame frame) -> Array<Data> {
  auto const& mask = frame.mask();
  // Items apply in order, like in the legacy evaluator: a later item
  // overwrites the value of an earlier field with the same name, which keeps
  // its position, and new fields are appended.
  auto result = Option<Array<Record>>{};
  auto contribute = [&](Array<Record> contribution) {
    // The first contribution is taken as is, sharing its storage.
    result = result ? std::move(*result).with_fields(contribution)
                    : std::move(contribution);
  };
  // Consecutive fields form one contribution.
  auto fields
    = std::vector<std::pair<std::string_view, MaskedArray<Array<Data>>>>{};
  auto apply_fields = [&] {
    if (fields.empty()) {
      return;
    }
    contribute(Array<Record>::from_fields(fields));
    fields.clear();
  };
  for (auto const& item : x.items) {
    if (auto const* field = try_as<ast::record::field>(item)) {
      fields.emplace_back(field->name.name, MaskedArray<Array<Data>>{
                                              frame.eval(field->expr), mask});
      continue;
    }
    auto const& spread = as<ast::spread>(item);
    auto spread_value = eval_spread(frame, spread.expr);
    if (not spread_value) {
      continue;
    }
    apply_fields();
    contribute(std::move(*spread_value));
  }
  apply_fields();
  if (not result) {
    return Array<Data>{Array<Record>::make_empty(frame.length())};
  }
  return Array<Data>{std::move(*result)};
}

} // namespace tenzir::nova
