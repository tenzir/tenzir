//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/filter.hpp"

#include <tenzir/data.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/option.hpp>
#include <tenzir/time.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/entity_path.hpp>
#include <tenzir/variant.hpp>

#include <iceberg/expression/expression.h>
#include <iceberg/expression/expressions.h>
#include <iceberg/expression/literal.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/type.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <concepts>
#include <cstdint>
#include <limits>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::iceberg {

namespace {

namespace ice = ::iceberg;

constexpr auto ns_per_us = int64_t{1'000};
constexpr auto ns_per_day = int64_t{86'400'000'000'000};
// Largest integer magnitudes representable without rounding in IEEE-754
// binary64 (`double`, 53 bits) and binary32 (`float`, 24 bits).
constexpr auto max_exact_double_int = int64_t{1} << 53;
constexpr auto max_exact_float_int = int64_t{1} << 24;

/// A translated Iceberg expression paired with whether negating it stays sound
/// under TQL's three-valued boolean semantics.
///
/// For any TQL predicate `E` evaluating on row `r` to `v(E, r) in {T, F, N}`:
/// - Always: `v(E, r) == T` (or a runtime diagnostic) implies `expr` does not
///   prune a file containing `r`.
/// - When `exact` is true: `v(E, r) == F` implies `expr->Negate()` does not
///   prune a file containing `r`.
struct Translated {
  std::shared_ptr<ice::Expression> expr;
  bool exact = false;
};

auto conservative() -> Translated {
  return {ice::Expressions::AlwaysTrue(), false};
}

auto always_true(bool exact) -> Translated {
  return {ice::Expressions::AlwaysTrue(), exact};
}

auto always_false(bool exact) -> Translated {
  return {ice::Expressions::AlwaysFalse(), exact};
}

/// Euclidean floor division for `divisor > 0`: returns `{q, r}` with
/// `a == q * divisor + r` and `0 <= r < divisor`, including for negative `a`.
constexpr auto floor_div_mod(int64_t a, int64_t divisor)
  -> std::pair<int64_t, int64_t> {
  TENZIR_ASSERT(divisor > 0);
  auto q = a / divisor;
  auto r = a % divisor;
  if (r < 0) {
    --q;
    r += divisor;
  }
  return {q, r};
}

constexpr auto flip_comparison(ast::binary_op op) -> ast::binary_op {
  switch (op) {
    case ast::binary_op::lt:
      return ast::binary_op::gt;
    case ast::binary_op::leq:
      return ast::binary_op::geq;
    case ast::binary_op::gt:
      return ast::binary_op::lt;
    case ast::binary_op::geq:
      return ast::binary_op::leq;
    default:
      return op;
  }
}

/// Extracts a compile-time constant value, folding unary `+` and `-` when they
/// evaluate silently without overflow in TQL.
auto extract_constant(ast::expression const& expr)
  -> Option<ast::constant::kind> {
  if (not expr.kind) {
    return None{};
  }
  if (auto const* c = try_as<ast::constant>(expr)) {
    return c->value;
  }
  auto const* unary = try_as<ast::unary_expr>(expr);
  if (not unary) {
    return None{};
  }
  if (unary->op == ast::unary_op::pos) {
    auto inner = extract_constant(unary->expr);
    if (not inner) {
      return None{};
    }
    return match(
      *inner, []<class T>(T const& x) -> Option<ast::constant::kind> {
        if constexpr (std::same_as<T, int64_t> or std::same_as<T, uint64_t>
                      or std::same_as<T, double>) {
          return ast::constant::kind{x};
        } else {
          return None{};
        }
      });
  }
  if (unary->op == ast::unary_op::neg) {
    auto inner = extract_constant(unary->expr);
    if (not inner) {
      return None{};
    }
    return match(
      *inner,
      [](int64_t v) -> Option<ast::constant::kind> {
        if (v == std::numeric_limits<int64_t>::min()) {
          return None{};
        }
        return ast::constant::kind{int64_t{-v}};
      },
      [](uint64_t v) -> Option<ast::constant::kind> {
        constexpr auto limit
          = static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1;
        if (v > limit) {
          return None{};
        }
        if (v == limit) {
          return ast::constant::kind{std::numeric_limits<int64_t>::min()};
        }
        return ast::constant::kind{-static_cast<int64_t>(v)};
      },
      [](double v) -> Option<ast::constant::kind> {
        return ast::constant::kind{-v};
      },
      [](duration v) -> Option<ast::constant::kind> {
        if (v.count() == std::numeric_limits<duration::rep>::min()) {
          return None{};
        }
        return ast::constant::kind{duration{-v.count()}};
      },
      [](auto const&) -> Option<ast::constant::kind> {
        return None{};
      });
  }
  return None{};
}

struct ResolvedField {
  std::string name;
  ice::TypeId type_id;
};

enum class FieldStatus {
  found,
  missing,
  conservative,
};

struct FieldLookup {
  FieldStatus status = FieldStatus::conservative;
  Option<ResolvedField> field = None{};
};

/// The schemas of a translation: TQL paths resolve against `resolve`, the
/// schema whose names the events carry, and the expression names each field
/// as `bind` does, the schema that the scan binds the expression against.
/// The two differ when a restored scan keeps the schema of the original plan.
struct Schemas {
  ice::Schema const& resolve;
  ice::Schema const& bind;
};

/// Renames the last field of `path`, the fields of `schemas.resolve` from the
/// top-level struct down to the leaf, reached by `dotted`, to its name in
/// `schemas.bind`. Returns `None` if `bind` lacks a field of the path, gives
/// one another type, or requires one that `resolve` makes optional, so that
/// the caller stays conservative. A required field would let Iceberg fold a
/// null check to false and prune files that hold nulls.
auto bind_name(Schemas const& schemas, std::string dotted,
               std::span<ice::SchemaField const* const> path)
  -> Option<std::string> {
  if (&schemas.resolve == &schemas.bind) {
    return dotted;
  }
  for (auto const* field : path) {
    auto bound = schemas.bind.FindFieldById(field->field_id());
    if (not bound.has_value() or not bound->has_value()) {
      return None{};
    }
    auto const& bound_field = bound->value().get();
    if (bound_field.type()->type_id() != field->type()->type_id()
        or (field->optional() and not bound_field.optional())) {
      return None{};
    }
  }
  auto const field_id = path.back()->field_id();
  auto name = schemas.bind.FindColumnNameById(field_id);
  if (not name.has_value() or not name->has_value()) {
    return None{};
  }
  // The same guard as for `resolve`: the dotted name must lead back to the
  // field.
  auto by_name = schemas.bind.FindFieldByName(**name, /*case_sensitive=*/true);
  if (not by_name.has_value() or not by_name->has_value()
      or by_name->value().get().field_id() != field_id) {
    return None{};
  }
  return std::string{**name};
}

/// Resolves a TQL field path against an Iceberg schema, walking strictly
/// through `StructType` ancestors.
auto resolve_field(ast::field_path const& path, Schemas const& schemas)
  -> FieldLookup {
  auto const& schema = schemas.resolve;
  auto const segments = path.path();
  if (segments.empty()) {
    return {};
  }
  auto const* current = static_cast<ice::StructType const*>(&schema);
  auto dotted = std::string{};
  // The fields from the top-level struct down to the current segment.
  auto fields = std::vector<ice::SchemaField const*>{};
  for (auto i = size_t{0}; i < segments.size(); ++i) {
    auto const& seg_name = segments[i].id.name;
    if (seg_name.empty()) {
      return {};
    }
    if (i > 0) {
      dotted.push_back('.');
    }
    dotted.append(seg_name);
    auto found = current->GetFieldByName(seg_name, /*case_sensitive=*/true);
    if (not found.has_value()) {
      return {};
    }
    if (not found->has_value()) {
      // Every ancestor was a struct, so this path genuinely does not exist in
      // `schema`. Only fold to `missing` when `?` suppresses missing-field
      // warnings on this segment and every remaining segment; otherwise stay
      // conservative so files are not pruned and TQL still emits its warning.
      auto const silent = std::ranges::all_of(segments | std::views::drop(i),
                                              [](auto const& seg) {
                                                return seg.has_question_mark;
                                              });
      if (silent) {
        return {.status = FieldStatus::missing, .field = None{}};
      }
      return {};
    }
    auto const& field = found->value().get();
    auto const& type = *field.type();
    fields.push_back(&field);
    if (i + 1 < segments.size()) {
      if (type.is_struct()) {
        current = static_cast<ice::StructType const*>(&type);
        continue;
      }
      // Non-struct ancestors (`MapType` imports as a dynamic record;
      // `VariantType` holds semi-structured data; `ListType` and primitives
      // warn on field access in TQL) must stay conservative.
      return {};
    }
    // Iceberg metrics and `NamedReference::Bind` only support primitive leaf
    // columns; nested leaves cannot be bound safely.
    if (not type.is_primitive()) {
      return {};
    }
    // Confirm that Iceberg's dotted lookup resolves back to the exact same
    // field ID (guards against literal dots in column names colliding with
    // nested struct paths).
    auto by_name = schema.FindFieldByName(dotted, /*case_sensitive=*/true);
    if (not by_name.has_value() or not by_name->has_value()
        or by_name->value().get().field_id() != field.field_id()) {
      return {};
    }
    auto bound = bind_name(schemas, std::move(dotted), fields);
    if (not bound) {
      return {};
    }
    return {
      .status = FieldStatus::found,
      .field = ResolvedField{std::move(*bound), type.type_id()},
    };
  }
  TENZIR_UNREACHABLE();
}

auto make_comparison(ast::binary_op op, std::string const& name,
                     ice::Literal lit) -> std::shared_ptr<ice::Expression> {
  switch (op) {
    case ast::binary_op::eq:
      return ice::Expressions::Equal(name, std::move(lit));
    case ast::binary_op::neq:
      return ice::Expressions::NotEqual(name, std::move(lit));
    case ast::binary_op::lt:
      return ice::Expressions::LessThan(name, std::move(lit));
    case ast::binary_op::leq:
      return ice::Expressions::LessThanOrEqual(name, std::move(lit));
    case ast::binary_op::gt:
      return ice::Expressions::GreaterThan(name, std::move(lit));
    case ast::binary_op::geq:
      return ice::Expressions::GreaterThanOrEqual(name, std::move(lit));
    default:
      TENZIR_UNREACHABLE();
  }
}

/// Folds a comparison whose constant lies strictly outside the column type's
/// representable range.
///
/// In TQL, `null != v` is `true` (so `!=` folds to `AlwaysTrue`), while
/// `null < v` / `null <= v` / `null > v` / `null >= v` evaluate to `null`
/// (falsy in `where`, so ordering tautologies fold to `NotNull`).
auto fold_out_of_range(ast::binary_op op, std::string const& name,
                       bool above_max) -> Translated {
  switch (op) {
    case ast::binary_op::eq:
      return always_false(/*exact=*/true);
    case ast::binary_op::neq:
      return always_true(/*exact=*/true);
    case ast::binary_op::lt:
    case ast::binary_op::leq:
      return above_max
               ? Translated{ice::Expressions::NotNull(name), /*exact=*/true}
               : always_false(/*exact=*/true);
    case ast::binary_op::gt:
    case ast::binary_op::geq:
      return above_max
               ? always_false(/*exact=*/true)
               : Translated{ice::Expressions::NotNull(name), /*exact=*/true};
    default:
      TENZIR_UNREACHABLE();
  }
}

/// Folds a comparison against a constant that falls strictly between `floor`
/// and `floor + 1` in a coarser column resolution (sub-microsecond timestamp or
/// time, or sub-day date).
auto fold_inexact_floor(ast::binary_op op, std::string const& name,
                        ice::Literal floor_lit) -> Translated {
  switch (op) {
    case ast::binary_op::eq:
      return always_false(/*exact=*/true);
    case ast::binary_op::neq:
      // In TQL, `null != v` is `true`, and every non-null value has remainder 0
      // so it is also unequal to `v`.
      return always_true(/*exact=*/true);
    case ast::binary_op::gt:
    case ast::binary_op::geq:
      return {ice::Expressions::GreaterThan(name, std::move(floor_lit)),
              /*exact=*/true};
    case ast::binary_op::lt:
    case ast::binary_op::leq:
      return {ice::Expressions::LessThanOrEqual(name, std::move(floor_lit)),
              /*exact=*/true};
    default:
      TENZIR_UNREACHABLE();
  }
}

auto extract_exact_float(ast::constant::kind const& value, bool is_float32)
  -> Option<double> {
  if (auto const* d = try_as<double>(value)) {
    if (not std::isfinite(*d)) {
      return None{};
    }
    if (is_float32 and static_cast<double>(static_cast<float>(*d)) != *d) {
      return None{};
    }
    return *d;
  }
  if (auto const* v = try_as<int64_t>(value)) {
    auto const limit = is_float32 ? max_exact_float_int : max_exact_double_int;
    if (*v < -limit or *v > limit) {
      return None{};
    }
    return static_cast<double>(*v);
  }
  if (auto const* v = try_as<uint64_t>(value)) {
    auto const limit = static_cast<uint64_t>(is_float32 ? max_exact_float_int
                                                        : max_exact_double_int);
    if (*v > limit) {
      return None{};
    }
    return static_cast<double>(*v);
  }
  return None{};
}

/// Translates a comparison on a floating-point column.
///
/// Always returns `exact = false` so `not(...)` stays conservative (avoiding
/// NaN mismatches such as `not (NaN < 5.0) == true` vs Iceberg's `GtEq`
/// pruning all-NaN files), and leaves `!=` conservative because Iceberg's
/// `InclusiveMetricsEvaluator::NotEq` can prune files with NaNs when
/// `nan_value_counts` is omitted.
auto translate_float_comparison(ast::binary_op op, std::string const& name,
                                double value, bool is_float32) -> Translated {
  if (op == ast::binary_op::neq) {
    return conservative();
  }
  auto make_lit = [&](double d) -> ice::Literal {
    return is_float32 ? ice::Literal::Float(static_cast<float>(d))
                      : ice::Literal::Double(d);
  };
  // Iceberg uses IEEE-754 totalOrder where `-0.0 < +0.0`, while TQL treats
  // `-0.0 == +0.0`. Pick the bound sign that preserves both zeros.
  if (value == 0.0) {
    switch (op) {
      case ast::binary_op::eq:
        return {
          ice::Expressions::Or(ice::Expressions::Equal(name, make_lit(-0.0)),
                               ice::Expressions::Equal(name, make_lit(0.0))),
          /*exact=*/false};
      case ast::binary_op::lt:
        return {ice::Expressions::LessThan(name, make_lit(-0.0)),
                /*exact=*/false};
      case ast::binary_op::leq:
        return {ice::Expressions::LessThanOrEqual(name, make_lit(0.0)),
                /*exact=*/false};
      case ast::binary_op::gt:
        return {ice::Expressions::GreaterThan(name, make_lit(0.0)),
                /*exact=*/false};
      case ast::binary_op::geq:
        return {ice::Expressions::GreaterThanOrEqual(name, make_lit(-0.0)),
                /*exact=*/false};
      default:
        TENZIR_UNREACHABLE();
    }
  }
  return {make_comparison(op, name, make_lit(value)), /*exact=*/false};
}

/// Translates a comparison against a field absent from `schema`, where TQL
/// evaluates the missing field to `null`.
auto translate_missing_comparison(ast::binary_op op,
                                  ast::constant::kind const& rhs)
  -> Translated {
  if (is<secret>(rhs)) {
    return conservative();
  }
  if (is<caf::none_t>(rhs)) {
    switch (op) {
      case ast::binary_op::eq:
      case ast::binary_op::leq:
      case ast::binary_op::geq:
        return always_true(/*exact=*/true);
      case ast::binary_op::neq:
      case ast::binary_op::lt:
      case ast::binary_op::gt:
        return always_false(/*exact=*/true);
      default:
        TENZIR_UNREACHABLE();
    }
  }
  switch (op) {
    case ast::binary_op::neq:
      return always_true(/*exact=*/true);
    case ast::binary_op::eq:
    case ast::binary_op::lt:
    case ast::binary_op::leq:
    case ast::binary_op::gt:
    case ast::binary_op::geq:
      return always_false(/*exact=*/true);
    default:
      TENZIR_UNREACHABLE();
  }
}

/// Returns whether `type_id` is converted with fixed-factor scaling into
/// 64-bit nanoseconds at read time, where out-of-range values (outside roughly
/// `[1677, 2262]`) overflow and become `null`.
constexpr auto may_overflow_to_null(ice::TypeId type_id) -> bool {
  return type_id == ice::TypeId::kDate or type_id == ice::TypeId::kTimestamp
         or type_id == ice::TypeId::kTimestampTz;
}

/// Translates `col <op> null` for an existing primitive column.
auto translate_null_comparison(ast::binary_op op, ResolvedField const& field)
  -> Translated {
  auto const overflow_null = may_overflow_to_null(field.type_id);
  switch (op) {
    case ast::binary_op::eq:
    case ast::binary_op::leq:
    case ast::binary_op::geq:
      // Out-of-range `date` / `timestamp` / `timestamptz` values become `null`
      // when decoded into 64-bit nanoseconds, so `IsNull` would falsely prune
      // a non-null file containing e.g. `9999-12-31`.
      if (overflow_null) {
        return conservative();
      }
      return {ice::Expressions::IsNull(field.name), /*exact=*/true};
    case ast::binary_op::neq:
      // Sound for `col != null` (an all-null file cannot yield any non-null
      // row after decoding), but keep `exact = false` when out-of-range values
      // may become `null` so `not (col != null)` does not negate to `IsNull`.
      return {ice::Expressions::NotNull(field.name),
              /*exact=*/not overflow_null};
    case ast::binary_op::lt:
    case ast::binary_op::gt:
      return always_false(/*exact=*/true);
    default:
      TENZIR_UNREACHABLE();
  }
}

auto translate_typed_comparison(ast::binary_op op, ResolvedField const& field,
                                ast::constant::kind const& rhs) -> Translated {
  if (is<caf::none_t>(rhs)) {
    return translate_null_comparison(op, field);
  }
  if (is<secret>(rhs)) {
    return conservative();
  }
  switch (field.type_id) {
    case ice::TypeId::kBoolean: {
      if (auto const* v = try_as<bool>(rhs)) {
        return {make_comparison(op, field.name, ice::Literal::Boolean(*v)),
                /*exact=*/true};
      }
      return conservative();
    }
    case ice::TypeId::kInt: {
      if (auto const* v = try_as<int64_t>(rhs)) {
        if (*v > int64_t{std::numeric_limits<int32_t>::max()}) {
          return fold_out_of_range(op, field.name, /*above_max=*/true);
        }
        if (*v < int64_t{std::numeric_limits<int32_t>::min()}) {
          return fold_out_of_range(op, field.name, /*above_max=*/false);
        }
        return {make_comparison(op, field.name,
                                ice::Literal::Int(static_cast<int32_t>(*v))),
                /*exact=*/true};
      }
      if (auto const* v = try_as<uint64_t>(rhs)) {
        if (*v > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
          return fold_out_of_range(op, field.name, /*above_max=*/true);
        }
        return {make_comparison(op, field.name,
                                ice::Literal::Int(static_cast<int32_t>(*v))),
                /*exact=*/true};
      }
      return conservative();
    }
    case ice::TypeId::kLong: {
      if (auto const* v = try_as<int64_t>(rhs)) {
        return {make_comparison(op, field.name, ice::Literal::Long(*v)),
                /*exact=*/true};
      }
      if (auto const* v = try_as<uint64_t>(rhs)) {
        if (*v > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
          return fold_out_of_range(op, field.name, /*above_max=*/true);
        }
        return {make_comparison(op, field.name,
                                ice::Literal::Long(static_cast<int64_t>(*v))),
                /*exact=*/true};
      }
      return conservative();
    }
    case ice::TypeId::kFloat: {
      if (auto d = extract_exact_float(rhs, /*is_float32=*/true)) {
        return translate_float_comparison(op, field.name, *d,
                                          /*is_float32=*/true);
      }
      return conservative();
    }
    case ice::TypeId::kDouble: {
      if (auto d = extract_exact_float(rhs, /*is_float32=*/false)) {
        return translate_float_comparison(op, field.name, *d,
                                          /*is_float32=*/false);
      }
      return conservative();
    }
    case ice::TypeId::kString: {
      if (auto const* v = try_as<std::string>(rhs)) {
        return {make_comparison(op, field.name, ice::Literal::String(*v)),
                /*exact=*/true};
      }
      return conservative();
    }
    case ice::TypeId::kTimestamp:
    case ice::TypeId::kTimestampTz: {
      auto const* v = try_as<time>(rhs);
      if (not v) {
        return conservative();
      }
      auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        v->time_since_epoch())
                        .count();
      auto const [us, rem] = floor_div_mod(ns, ns_per_us);
      auto lit = field.type_id == ice::TypeId::kTimestamp
                   ? ice::Literal::Timestamp(us)
                   : ice::Literal::TimestampTz(us);
      if (rem == 0) {
        return {make_comparison(op, field.name, std::move(lit)),
                /*exact=*/true};
      }
      return fold_inexact_floor(op, field.name, std::move(lit));
    }
    case ice::TypeId::kTimestampNs:
    case ice::TypeId::kTimestampTzNs: {
      auto const* v = try_as<time>(rhs);
      if (not v) {
        return conservative();
      }
      auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        v->time_since_epoch())
                        .count();
      auto lit = field.type_id == ice::TypeId::kTimestampNs
                   ? ice::Literal::TimestampNs(ns)
                   : ice::Literal::TimestampTzNs(ns);
      return {make_comparison(op, field.name, std::move(lit)), /*exact=*/true};
    }
    case ice::TypeId::kDate: {
      auto const* v = try_as<time>(rhs);
      if (not v) {
        return conservative();
      }
      auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        v->time_since_epoch())
                        .count();
      auto const [days, rem] = floor_div_mod(ns, ns_per_day);
      if (days > int64_t{std::numeric_limits<int32_t>::max()}) {
        return fold_out_of_range(op, field.name, /*above_max=*/true);
      }
      if (days < int64_t{std::numeric_limits<int32_t>::min()}) {
        return fold_out_of_range(op, field.name, /*above_max=*/false);
      }
      auto lit = ice::Literal::Date(static_cast<int32_t>(days));
      if (rem == 0) {
        return {make_comparison(op, field.name, std::move(lit)),
                /*exact=*/true};
      }
      return fold_inexact_floor(op, field.name, std::move(lit));
    }
    case ice::TypeId::kTime: {
      auto const* v = try_as<duration>(rhs);
      if (not v) {
        return conservative();
      }
      auto const ns
        = std::chrono::duration_cast<std::chrono::nanoseconds>(*v).count();
      if (ns < 0 or ns >= ns_per_day) {
        return conservative();
      }
      auto const [us, rem] = floor_div_mod(ns, ns_per_us);
      auto lit = ice::Literal::Time(us);
      if (rem == 0) {
        return {make_comparison(op, field.name, std::move(lit)),
                /*exact=*/true};
      }
      return fold_inexact_floor(op, field.name, std::move(lit));
    }
    default:
      return conservative();
  }
}

auto translate_comparison(ast::binary_expr const& expr, Schemas const& schema)
  -> Translated {
  auto op = expr.op;
  auto path = ast::field_path::try_from(expr.left);
  auto rhs = extract_constant(expr.right);
  if (not path or not rhs) {
    path = ast::field_path::try_from(expr.right);
    rhs = extract_constant(expr.left);
    op = flip_comparison(op);
  }
  if (not path or not rhs) {
    return conservative();
  }
  auto lookup = resolve_field(*path, schema);
  if (lookup.status == FieldStatus::missing) {
    return translate_missing_comparison(op, *rhs);
  }
  if (lookup.status != FieldStatus::found) {
    return conservative();
  }
  return translate_typed_comparison(op, *lookup.field, *rhs);
}

auto extract_constant_list(ast::expression const& expr)
  -> Option<std::vector<ast::constant::kind>> {
  if (not expr.kind) {
    return None{};
  }
  if (auto const* list = try_as<ast::list>(expr)) {
    auto elements = std::vector<ast::constant::kind>{};
    elements.reserve(list->items.size());
    for (auto const& item : list->items) {
      auto const* elem = try_as<ast::expression>(item);
      if (not elem) {
        return None{};
      }
      auto value = extract_constant(*elem);
      if (not value or is<secret>(*value)) {
        return None{};
      }
      elements.push_back(std::move(*value));
    }
    return elements;
  }
  if (auto const* c = try_as<ast::constant>(expr)) {
    auto const* list = try_as<tenzir::list>(c->value);
    if (not list) {
      return None{};
    }
    auto elements = std::vector<ast::constant::kind>{};
    elements.reserve(list->size());
    for (auto const& item : *list) {
      auto converted = match(
        item,
        [](pattern const&) -> Option<ast::constant::kind> {
          return None{};
        },
        [](secret const&) -> Option<ast::constant::kind> {
          return None{};
        },
        [](auto const& x) -> Option<ast::constant::kind> {
          return ast::constant::kind{x};
        });
      if (not converted) {
        return None{};
      }
      elements.push_back(std::move(*converted));
    }
    return elements;
  }
  return None{};
}

/// Appends any matching Iceberg literals for `elem` into `out`, setting
/// `has_null` for `null` elements and returning `false` if `elem` is
/// type-incompatible or cannot be converted safely.
auto append_in_element(ice::TypeId type_id, ast::constant::kind const& elem,
                       std::vector<ice::Literal>& out, bool& has_null) -> bool {
  if (is<caf::none_t>(elem)) {
    has_null = true;
    return true;
  }
  switch (type_id) {
    case ice::TypeId::kBoolean: {
      if (auto const* v = try_as<bool>(elem)) {
        out.push_back(ice::Literal::Boolean(*v));
        return true;
      }
      return false;
    }
    case ice::TypeId::kInt: {
      if (auto const* v = try_as<int64_t>(elem)) {
        if (*v >= int64_t{std::numeric_limits<int32_t>::min()}
            and *v <= int64_t{std::numeric_limits<int32_t>::max()}) {
          out.push_back(ice::Literal::Int(static_cast<int32_t>(*v)));
        }
        return true;
      }
      if (auto const* v = try_as<uint64_t>(elem)) {
        if (*v <= static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
          out.push_back(ice::Literal::Int(static_cast<int32_t>(*v)));
        }
        return true;
      }
      return false;
    }
    case ice::TypeId::kLong: {
      if (auto const* v = try_as<int64_t>(elem)) {
        out.push_back(ice::Literal::Long(*v));
        return true;
      }
      if (auto const* v = try_as<uint64_t>(elem)) {
        if (*v <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
          out.push_back(ice::Literal::Long(static_cast<int64_t>(*v)));
        }
        return true;
      }
      return false;
    }
    case ice::TypeId::kFloat:
    case ice::TypeId::kDouble: {
      auto const is_float32 = type_id == ice::TypeId::kFloat;
      auto d = extract_exact_float(elem, is_float32);
      if (not d) {
        return false;
      }
      auto push = [&](double x) {
        out.push_back(is_float32 ? ice::Literal::Float(static_cast<float>(x))
                                 : ice::Literal::Double(x));
      };
      if (*d == 0.0) {
        push(-0.0);
        push(0.0);
      } else {
        push(*d);
      }
      return true;
    }
    case ice::TypeId::kString: {
      if (auto const* v = try_as<std::string>(elem)) {
        out.push_back(ice::Literal::String(*v));
        return true;
      }
      return false;
    }
    case ice::TypeId::kTimestamp:
    case ice::TypeId::kTimestampTz: {
      auto const* v = try_as<time>(elem);
      if (not v) {
        return false;
      }
      auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        v->time_since_epoch())
                        .count();
      auto const [us, rem] = floor_div_mod(ns, ns_per_us);
      if (rem == 0) {
        out.push_back(type_id == ice::TypeId::kTimestamp
                        ? ice::Literal::Timestamp(us)
                        : ice::Literal::TimestampTz(us));
      }
      return true;
    }
    case ice::TypeId::kTimestampNs:
    case ice::TypeId::kTimestampTzNs: {
      auto const* v = try_as<time>(elem);
      if (not v) {
        return false;
      }
      auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        v->time_since_epoch())
                        .count();
      out.push_back(type_id == ice::TypeId::kTimestampNs
                      ? ice::Literal::TimestampNs(ns)
                      : ice::Literal::TimestampTzNs(ns));
      return true;
    }
    case ice::TypeId::kDate: {
      auto const* v = try_as<time>(elem);
      if (not v) {
        return false;
      }
      auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        v->time_since_epoch())
                        .count();
      auto const [days, rem] = floor_div_mod(ns, ns_per_day);
      if (rem == 0 and days >= int64_t{std::numeric_limits<int32_t>::min()}
          and days <= int64_t{std::numeric_limits<int32_t>::max()}) {
        out.push_back(ice::Literal::Date(static_cast<int32_t>(days)));
      }
      return true;
    }
    case ice::TypeId::kTime: {
      auto const* v = try_as<duration>(elem);
      if (not v) {
        return false;
      }
      auto const ns
        = std::chrono::duration_cast<std::chrono::nanoseconds>(*v).count();
      if (ns < 0 or ns >= ns_per_day) {
        return false;
      }
      auto const [us, rem] = floor_div_mod(ns, ns_per_us);
      if (rem == 0) {
        out.push_back(ice::Literal::Time(us));
      }
      return true;
    }
    default:
      return false;
  }
}

auto translate_in(ast::binary_expr const& expr, Schemas const& schema)
  -> Translated {
  auto elements = extract_constant_list(expr.right);
  if (not elements) {
    return conservative();
  }
  auto path = ast::field_path::try_from(expr.left);
  if (not path) {
    return conservative();
  }
  auto lookup = resolve_field(*path, schema);
  if (lookup.status == FieldStatus::missing) {
    for (auto const& elem : *elements) {
      if (is<caf::none_t>(elem)) {
        return always_true(/*exact=*/true);
      }
    }
    return always_false(/*exact=*/true);
  }
  if (lookup.status != FieldStatus::found) {
    return conservative();
  }
  if (elements->empty()) {
    return always_false(/*exact=*/true);
  }
  auto const& field = *lookup.field;
  auto literals = std::vector<ice::Literal>{};
  literals.reserve(elements->size());
  auto has_null = false;
  for (auto const& elem : *elements) {
    if (not append_in_element(field.type_id, elem, literals, has_null)) {
      return conservative();
    }
  }
  if (has_null and may_overflow_to_null(field.type_id)) {
    // Out-of-range `date` / `timestamp` / `timestamptz` values become `null`
    // at read time and therefore match `col in [..., null]`; stay conservative.
    return conservative();
  }
  auto const exact = field.type_id != ice::TypeId::kFloat
                     and field.type_id != ice::TypeId::kDouble;
  if (literals.empty()) {
    if (has_null) {
      return {ice::Expressions::IsNull(field.name), /*exact=*/true};
    }
    return always_false(exact);
  }
  auto in_expr = std::shared_ptr<ice::Expression>{
    ice::Expressions::In(field.name, std::move(literals))};
  if (has_null) {
    // In TQL, `null in [..., null, ...]` evaluates to `true`.
    return {ice::Expressions::Or(ice::Expressions::IsNull(field.name),
                                 std::move(in_expr)),
            exact};
  }
  return {std::move(in_expr), exact};
}

auto is_std_starts_with(ast::function_call const& call) -> bool {
  if (call.fn.ref.resolved()) {
    return call.fn.ref.pkg() == entity_pkg_std
           and call.fn.ref.segments().size() == 1
           and call.fn.ref.segments().front() == "starts_with";
  }
  return call.fn.path.size() == 1
         and call.fn.path.front().name == "starts_with";
}

auto translate_starts_with(ast::function_call const& call,
                           Schemas const& schema) -> Translated {
  // Requiring exactly two positional arguments rejects named options such as
  // `ignore_case=true` as well as malformed invocations.
  if (not is_std_starts_with(call) or call.args.size() != 2) {
    return conservative();
  }
  auto path = ast::field_path::try_from(call.args[0]);
  auto prefix = extract_constant(call.args[1]);
  if (not path or not prefix) {
    return conservative();
  }
  auto const* prefix_str = try_as<std::string>(*prefix);
  if (not prefix_str) {
    return conservative();
  }
  auto lookup = resolve_field(*path, schema);
  if (lookup.status == FieldStatus::missing) {
    return always_false(/*exact=*/true);
  }
  if (lookup.status != FieldStatus::found
      or lookup.field->type_id != ice::TypeId::kString) {
    return conservative();
  }
  return {ice::Expressions::StartsWith(lookup.field->name, *prefix_str),
          /*exact=*/true};
}

auto translate_bare_field(ast::expression const& expr, Schemas const& schema)
  -> Translated {
  auto path = ast::field_path::try_from(expr);
  if (not path) {
    return conservative();
  }
  auto lookup = resolve_field(*path, schema);
  if (lookup.status == FieldStatus::missing) {
    return always_false(/*exact=*/true);
  }
  if (lookup.status == FieldStatus::found
      and lookup.field->type_id == ice::TypeId::kBoolean) {
    return {ice::Expressions::Equal(lookup.field->name,
                                    ice::Literal::Boolean(true)),
            /*exact=*/true};
  }
  return conservative();
}

auto translate_impl(ast::expression const& expr, Schemas const& schema)
  -> Translated {
  if (not expr.kind) {
    return conservative();
  }
  return expr.match(
    [&](ast::binary_expr const& x) -> Translated {
      switch (x.op) {
        case ast::binary_op::and_: {
          auto lhs = translate_impl(x.left, schema);
          auto rhs = translate_impl(x.right, schema);
          return {
            ice::Expressions::And(std::move(lhs.expr), std::move(rhs.expr)),
            lhs.exact and rhs.exact,
          };
        }
        case ast::binary_op::or_: {
          auto lhs = translate_impl(x.left, schema);
          auto rhs = translate_impl(x.right, schema);
          return {
            ice::Expressions::Or(std::move(lhs.expr), std::move(rhs.expr)),
            lhs.exact and rhs.exact,
          };
        }
        case ast::binary_op::eq:
        case ast::binary_op::neq:
        case ast::binary_op::lt:
        case ast::binary_op::leq:
        case ast::binary_op::gt:
        case ast::binary_op::geq:
          return translate_comparison(x, schema);
        case ast::binary_op::in:
          return translate_in(x, schema);
        default:
          return conservative();
      }
    },
    [&](ast::unary_expr const& x) -> Translated {
      if (x.op != ast::unary_op::not_) {
        return conservative();
      }
      auto inner = translate_impl(x.expr, schema);
      if (not inner.exact) {
        return conservative();
      }
      auto negated = inner.expr->Negate();
      if (not negated.has_value()) {
        return conservative();
      }
      return {std::move(*negated), /*exact=*/true};
    },
    [&](ast::function_call const& x) -> Translated {
      return translate_starts_with(x, schema);
    },
    [&](ast::constant const& x) -> Translated {
      if (auto const* b = try_as<bool>(x.value)) {
        return *b ? always_true(/*exact=*/true) : always_false(/*exact=*/true);
      }
      if (is<caf::none_t>(x.value)) {
        return always_false(/*exact=*/true);
      }
      return conservative();
    },
    [&](ast::root_field const&) -> Translated {
      return translate_bare_field(expr, schema);
    },
    [&](ast::field_access const&) -> Translated {
      return translate_bare_field(expr, schema);
    },
    [&](ast::index_expr const&) -> Translated {
      return translate_bare_field(expr, schema);
    },
    [](auto const&) -> Translated {
      return conservative();
    });
}

} // namespace

auto translate_filter(ir::OptimizeFilter const& filter,
                      ice::Schema const& schema)
  -> std::shared_ptr<ice::Expression> {
  return translate_filter(filter, schema, schema);
}

auto translate_filter(ir::OptimizeFilter const& filter,
                      ice::Schema const& schema, ice::Schema const& bind_schema)
  -> std::shared_ptr<ice::Expression> {
  auto const schemas = Schemas{.resolve = schema, .bind = bind_schema};
  auto result
    = std::shared_ptr<ice::Expression>{ice::Expressions::AlwaysTrue()};
  for (auto const& predicate : filter) {
    auto translated = translate_impl(predicate, schemas).expr;
    result = ice::Expressions::And(std::move(result), std::move(translated));
    if (result->op() == ice::Expression::Operation::kFalse) {
      break;
    }
  }
  return result;
}

auto translate_expression(ast::expression const& expr,
                          ice::Schema const& schema)
  -> std::shared_ptr<ice::Expression> {
  return translate_impl(expr, Schemas{.resolve = schema, .bind = schema}).expr;
}

} // namespace tenzir::plugins::iceberg
