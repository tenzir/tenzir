//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet/row_group_pruning.hpp"

#include <tenzir/concepts.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/option.hpp>
#include <tenzir/time.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/variant.hpp>

#include <arrow/type.h>
#include <parquet/arrow/schema.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include <compare>
#include <string>
#include <utility>

namespace tenzir::plugins::parquet {

namespace {

/// A value of a filter constant or of column statistics, reduced to the kinds
/// whose comparisons are known to agree with TQL.
using Value = variant<int64_t, uint64_t, double, time, std::string, bool>;

/// Compares two values the way TQL does, or returns `None` if it cannot tell.
auto compare(Value const& lhs, Value const& rhs)
  -> Option<std::partial_ordering> {
  return match(std::tie(lhs, rhs),
               []<class L, class R>(
                 L const& l, R const& r) -> Option<std::partial_ordering> {
                 if constexpr (std::integral<L> and std::integral<R>
                               and not std::same_as<L, bool>
                               and not std::same_as<R, bool>) {
                   if (std::cmp_less(l, r)) {
                     return std::partial_ordering::less;
                   }
                   if (std::cmp_greater(l, r)) {
                     return std::partial_ordering::greater;
                   }
                   return std::partial_ordering::equivalent;
                 } else if constexpr (std::same_as<L, R>) {
                   auto result = std::partial_ordering{l <=> r};
                   if (result == std::partial_ordering::unordered) {
                     return None{};
                   }
                   return result;
                 } else {
                   // TODO: Compare integers with doubles exactly, as TQL does.
                   return None{};
                 }
               });
}

auto to_value(ast::constant::kind const& constant) -> Option<Value> {
  return match(constant, []<class T>(T const& x) -> Option<Value> {
    if constexpr (concepts::one_of<T, int64_t, uint64_t, double, time,
                                   std::string, bool>) {
      return Value{x};
    } else {
      // TODO: Durations, IPs, and subnets need their Parquet encodings.
      return None{};
    }
  });
}

/// What the statistics of a column chunk tell about its values.
struct Bounds {
  /// A value that no value of the chunk is less than, if known.
  Option<Value> min;
  /// A value that no value of the chunk is greater than, if known.
  Option<Value> max;
  /// Whether the chunk may hold NaNs, which statistics do not record.
  bool floating = false;
};

/// The bounds of a column chunk, if they are known and have a logical type
/// whose ordering matches the one of the imported values.
///
/// `restored` is the type that the reader restores for the column, which may
/// differ from what the Parquet types say. Arrow writes durations as plain
/// `INT64`, for example, and restores them from the embedded Arrow schema.
auto min_max(::parquet::ColumnDescriptor const& column,
             ::arrow::DataType const& restored,
             ::parquet::Statistics const& stats) -> Option<Bounds> {
  // Older writers ordered strings by signed bytes, for example, which the
  // column order of the schema reveals.
  if (not stats.HasMinMax() or not column.can_use_min_max()) {
    return None{};
  }
  auto const& logical = *column.logical_type();
  auto typed = [&]<class Type>(auto convert) -> Option<Bounds> {
    auto const& x = static_cast<::parquet::TypedStatistics<Type> const&>(stats);
    // Writers may truncate long values and then flag them as inexact. Files
    // that predate the flag leave it unset, and their bounds are exact.
    // TODO: A truncated minimum is still a lower bound, and a truncated
    // maximum an upper bound if the writer rounded it up. Relying on that
    // needs to know which writers do.
    auto exact = [](Option<bool> flag) {
      return flag.value_or(true);
    };
    auto min = Option<Value>{convert(x.min())};
    auto max = Option<Value>{convert(x.max())};
    // A bound without a representation as a value means that the import
    // rejects some value of the chunk, so its statistics decide nothing.
    if (not min or not max) {
      return None{};
    }
    return Bounds{
      .min = exact(stats.is_min_value_exact()) ? std::move(min) : None{},
      .max = exact(stats.is_max_value_exact()) ? std::move(max) : None{},
      .floating = std::is_floating_point_v<typename Type::c_type>,
    };
  };
  auto as_int64 = [](auto x) {
    return int64_t{x};
  };
  auto is_signed_int = [&] {
    return logical.is_none()
           or (logical.is_int()
               and static_cast<::parquet::IntLogicalType const&>(logical)
                     .is_signed());
  };
  auto restores_as = [&](auto... ids) {
    return ((restored.id() == ids) or ...);
  };
  switch (column.physical_type()) {
    case ::parquet::Type::BOOLEAN:
      if (not restores_as(::arrow::Type::BOOL)) {
        return None{};
      }
      return typed.operator()<::parquet::BooleanType>([](bool x) {
        return x;
      });
    case ::parquet::Type::INT32:
      // TODO: Unsigned integers are ordered as unsigned in the statistics.
      if (not is_signed_int()
          or not restores_as(::arrow::Type::INT8, ::arrow::Type::INT16,
                             ::arrow::Type::INT32)) {
        return None{};
      }
      return typed.operator()<::parquet::Int32Type>(as_int64);
    case ::parquet::Type::INT64:
      if (is_signed_int()) {
        if (not restores_as(::arrow::Type::INT64)) {
          return None{};
        }
        return typed.operator()<::parquet::Int64Type>(as_int64);
      }
      if (logical.is_timestamp() and restores_as(::arrow::Type::TIMESTAMP)) {
        // TODO: Check how timestamps that are not adjusted to UTC import.
        auto unit = static_cast<::parquet::TimestampLogicalType const&>(logical)
                      .time_unit();
        auto factor = unit == ::parquet::LogicalType::TimeUnit::MILLIS
                        ? int64_t{1'000'000}
                      : unit == ::parquet::LogicalType::TimeUnit::MICROS
                        ? int64_t{1'000}
                        : int64_t{1};
        return typed.operator()<::parquet::Int64Type>(
          [factor](int64_t x) -> Option<Value> {
            // Like the import, reject what nanoseconds cannot represent.
            auto nanoseconds = int64_t{};
            if (__builtin_mul_overflow(x, factor, &nanoseconds)) {
              return None{};
            }
            return Value{time{duration{nanoseconds}}};
          });
      }
      return None{};
    case ::parquet::Type::FLOAT:
    case ::parquet::Type::DOUBLE:
      // Statistics leave out NaNs, and -0 and +0 compare equal.
      if (not logical.is_none()
          or not restores_as(::arrow::Type::FLOAT, ::arrow::Type::DOUBLE)) {
        return None{};
      }
      if (column.physical_type() == ::parquet::Type::FLOAT) {
        return typed.operator()<::parquet::FloatType>([](float x) {
          return double{x};
        });
      }
      return typed.operator()<::parquet::DoubleType>([](double x) {
        return x;
      });
    case ::parquet::Type::BYTE_ARRAY:
      // Dictionaries and extension types import as something else.
      if (not logical.is_string()
          or not restores_as(::arrow::Type::STRING, ::arrow::Type::LARGE_STRING,
                             ::arrow::Type::STRING_VIEW)) {
        return None{};
      }
      return typed.operator()<::parquet::ByteArrayType>(
        [](::parquet::ByteArray x) {
          return std::string{reinterpret_cast<char const*>(x.ptr), x.len};
        });
    default:
      // TODO: Dates, decimals, and fixed-length byte arrays such as IPs.
      return None{};
  }
}

/// What a row group means for a predicate.
enum class Verdict {
  /// No row matches, and evaluating the predicate reports nothing.
  skip,
  /// Rows may match, and evaluating the predicate reports nothing.
  read,
  /// Evaluating the predicate may report something, so the row group must be
  /// read even if another predicate rules it out.
  must_read,
};

/// Combines the verdicts of two predicates of which the second only applies
/// to the rows that the first one selects, like the operands of `and`.
auto both(Verdict first, auto second) -> Verdict {
  return first == Verdict::read ? second() : first;
}

/// Answers what one row group means for an expression.
///
/// Evaluating the filter can emit diagnostics, for example for comparisons of
/// mismatched types or with missing fields. Skipping a row group must not
/// suppress them, so a predicate that cannot be shown to evaluate silently
/// forces a read. The operands of `and` and `or` short-circuit from left to
/// right, as they do at runtime.
class RowGroupCheck {
public:
  RowGroupCheck(::parquet::FileMetaData const& metadata,
                ::parquet::arrow::SchemaManifest const& manifest, int row_group)
    : schema_{*metadata.schema()},
      manifest_{manifest},
      row_group_{metadata.RowGroup(row_group)} {
  }

  auto check(ast::expression const& expr) const -> Verdict {
    auto const* binary = std::get_if<ast::binary_expr>(&*expr.kind);
    if (not binary) {
      // TODO: `not`, and functions such as `starts_with` or `is_empty`, which
      // need to know when they evaluate silently.
      return Verdict::must_read;
    }
    switch (binary->op) {
      case ast::binary_op::and_:
        return both(check(binary->left), [&] {
          return check(binary->right);
        });
      case ast::binary_op::or_: {
        // The right operand applies to the rows that the left one rejects.
        auto left = check(binary->left);
        if (left == Verdict::must_read) {
          return left;
        }
        auto right = check(binary->right);
        if (left == Verdict::skip or right == Verdict::must_read) {
          return right;
        }
        return Verdict::read;
      }
      case ast::binary_op::eq:
      case ast::binary_op::neq:
      case ast::binary_op::lt:
      case ast::binary_op::leq:
      case ast::binary_op::gt:
      case ast::binary_op::geq:
        return check_comparison(*binary);
      default:
        // TODO: `in` with a list of constants.
        return Verdict::must_read;
    }
  }

private:
  static auto flip(ast::binary_op op) -> ast::binary_op {
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

  /// The leaf column that `path` refers to, unless it is inside a list or
  /// ambiguous.
  auto find_column(ast::field_path const& path) const -> Option<int> {
    auto const* node
      = static_cast<::parquet::schema::Node const*>(schema_.group_node());
    for (auto const& segment : path.path()) {
      if (not node->is_group()) {
        return None{};
      }
      auto const& group
        = static_cast<::parquet::schema::GroupNode const&>(*node);
      auto const* next = static_cast<::parquet::schema::Node const*>(nullptr);
      for (auto i = 0; i < group.field_count(); ++i) {
        if (group.field(i)->name() != segment.id.name) {
          continue;
        }
        // The import keeps the last of duplicate fields, which need not be
        // the one whose statistics we would read. This also applies to any
        // parent of the leaf.
        // TODO: Follow the field that the import keeps.
        if (next) {
          return None{};
        }
        next = group.field(i).get();
      }
      if (not next or next->is_repeated()) {
        return None{};
      }
      node = next;
    }
    if (not node->is_primitive()) {
      return None{};
    }
    return schema_.ColumnIndex(*node);
  }

  auto check_comparison(ast::binary_expr const& x) const -> Verdict {
    auto op = x.op;
    auto path = ast::field_path::try_from(x.left);
    auto const* constant = std::get_if<ast::constant>(&*x.right.kind);
    if (not path) {
      path = ast::field_path::try_from(x.right);
      constant = std::get_if<ast::constant>(&*x.left.kind);
      op = flip(op);
    }
    if (not path or not constant) {
      // TODO: Fold deterministic constant subexpressions, such as `-1` or
      // `2026-01-01 + 1d`. Non-deterministic ones such as `now()` need care,
      // since the filter evaluates them again for every batch.
      return Verdict::must_read;
    }
    auto column = find_column(*path);
    if (not column) {
      // A field that the file lacks warns.
      return Verdict::must_read;
    }
    auto chunk = row_group_->ColumnChunk(*column);
    auto stats = chunk->is_stats_set() ? chunk->statistics() : nullptr;
    if (not stats) {
      return Verdict::must_read;
    }
    auto verdict = [](bool may_match) {
      return may_match ? Verdict::read : Verdict::skip;
    };
    // Null only equals null, and ordering comparisons warn for it.
    auto ordering = op != ast::binary_op::eq and op != ast::binary_op::neq;
    auto may_have_nulls = not stats->HasNullCount() or stats->null_count() > 0;
    if (ordering and may_have_nulls) {
      return Verdict::must_read;
    }
    if (std::holds_alternative<caf::none_t>(constant->value)) {
      if (ordering) {
        return Verdict::must_read;
      }
      // TODO: `!= null` needs the number of non-null values.
      return verdict(op != ast::binary_op::eq or may_have_nulls);
    }
    // Ordering comparisons also warn for types without an order.
    auto value = to_value(constant->value);
    auto const* field
      = static_cast<::parquet::arrow::SchemaField const*>(nullptr);
    auto bounds
      = manifest_.GetColumnField(*column, &field).ok() and field->field
          ? min_max(*schema_.Column(*column), *field->field->type(), *stats)
          : None{};
    if (not value or not bounds
        or (ordering and (is<std::string>(*value) or is<bool>(*value)))) {
      // TODO: Know which column types compare silently with a constant
      // without statistics, so that a comparison with an IP, for example,
      // does not prevent other predicates from skipping.
      return Verdict::must_read;
    }
    // Each comparison only relies on the bounds it needs. An unknown bound
    // cannot rule anything out.
    auto lower = bounds->min.and_then([&](Value const& min) {
      return compare(min, *value);
    });
    auto upper = bounds->max.and_then([&](Value const& max) {
      return compare(max, *value);
    });
    if (not lower and not upper) {
      // Comparing with neither bound worked, so the types may mismatch, which
      // warns.
      // TODO: Integers and doubles compare silently, but not yet here.
      return Verdict::must_read;
    }
    switch (op) {
      case ast::binary_op::eq:
        return verdict((not lower or *lower <= 0)
                       and (not upper or *upper >= 0));
      case ast::binary_op::neq:
        // A NaN is unequal to everything, but the statistics do not record it.
        if (bounds->floating or may_have_nulls or not lower or not upper) {
          return Verdict::read;
        }
        return verdict(*lower != 0 or *upper != 0);
      case ast::binary_op::lt:
        return verdict(not lower or *lower < 0);
      case ast::binary_op::leq:
        return verdict(not lower or *lower <= 0);
      case ast::binary_op::gt:
        return verdict(not upper or *upper > 0);
      case ast::binary_op::geq:
        return verdict(not upper or *upper >= 0);
      default:
        TENZIR_UNREACHABLE();
    }
  }

  ::parquet::SchemaDescriptor const& schema_;
  ::parquet::arrow::SchemaManifest const& manifest_;
  std::unique_ptr<::parquet::RowGroupMetaData> row_group_;
};

} // namespace

auto select_row_groups(ir::OptimizeFilter const& filter,
                       ::parquet::FileMetaData const& metadata)
  -> nova::storage::BitMap {
  if (filter.empty()) {
    return nova::storage::BitMap{metadata.num_row_groups(), true};
  }
  // The types that the reader restores for the columns, which statistics must
  // agree with.
  auto manifest = ::parquet::arrow::SchemaManifest{};
  if (not ::parquet::arrow::SchemaManifest::Make(
            metadata.schema(), metadata.key_value_metadata(),
            ::parquet::ArrowReaderProperties{}, &manifest)
            .ok()) {
    return nova::storage::BitMap{metadata.num_row_groups(), true};
  }
  auto result = nova::storage::BitMap::Builder{};
  for (auto group = 0; group < metadata.num_row_groups(); ++group) {
    // Every predicate applies to the rows the ones before it selected.
    auto check = RowGroupCheck{metadata, manifest, group};
    auto verdict = Verdict::read;
    for (auto const& predicate : filter) {
      verdict = both(verdict, [&] {
        return check.check(predicate);
      });
    }
    result.emplace_back(verdict != Verdict::skip);
  }
  return result.finish();
}

} // namespace tenzir::plugins::parquet
