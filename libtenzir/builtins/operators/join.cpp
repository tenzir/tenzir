//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async.hpp>
#include <tenzir/async/notify.hpp>
#include <tenzir/atomic.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/saturating_arithmetic.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/hash/hash.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/ir_lift.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/concatenate.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/eval_util.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/nova/materialize.hpp>
#include <tenzir/nova/slice.hpp>
#include <tenzir/nova/union_array.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <caf/binary_serializer.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <tsl/robin_map.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ranges>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::join {

namespace {

using namespace tenzir::si_literals;

constexpr auto default_max_memory = uint64_t{512_Mi};
constexpr auto output_chunk_rows = nova::storage::Index{16'384};
constexpr auto output_chunk_bytes = uint64_t{64_Mi};
constexpr auto max_single_row_bytes
  = static_cast<uint64_t>(std::numeric_limits<nova::storage::Index>::max() / 2);

enum class JoinKind : uint8_t {
  inner,
  left,
  semi,
  anti,
};

auto parse_kind(std::string_view value) -> Option<JoinKind> {
  if (value == "inner") {
    return JoinKind::inner;
  }
  if (value == "left") {
    return JoinKind::left;
  }
  if (value == "semi") {
    return JoinKind::semi;
  }
  if (value == "anti") {
    return JoinKind::anti;
  }
  return None{};
}

auto kind_name(JoinKind kind) -> std::string_view {
  switch (kind) {
    case JoinKind::inner:
      return "inner";
    case JoinKind::left:
      return "left";
    case JoinKind::semi:
      return "semi";
    case JoinKind::anti:
      return "anti";
  }
  TENZIR_UNREACHABLE();
}

auto format_bytes(uint64_t bytes) -> std::string {
  if (bytes >= 1_Gi and bytes % 1_Gi == 0) {
    return fmt::format("{} GiB", bytes / 1_Gi);
  }
  if (bytes >= 1_Mi and bytes % 1_Mi == 0) {
    return fmt::format("{} MiB", bytes / 1_Mi);
  }
  if (bytes >= 1_Ki and bytes % 1_Ki == 0) {
    return fmt::format("{} KiB", bytes / 1_Ki);
  }
  return fmt::format("{} bytes", bytes);
}

// ---------------------------------------------------------------------------
// Key expression classification & validation
// ---------------------------------------------------------------------------

struct JoinKeyPair {
  ast::expression left;
  ast::expression right;
  location left_loc;
  location right_loc;
};

auto is_comparison_or_logical_op(ast::binary_op op) -> bool {
  switch (op) {
    case ast::binary_op::eq:
    case ast::binary_op::neq:
    case ast::binary_op::gt:
    case ast::binary_op::geq:
    case ast::binary_op::lt:
    case ast::binary_op::leq:
    case ast::binary_op::and_:
    case ast::binary_op::or_:
    case ast::binary_op::in:
      return true;
    case ast::binary_op::add:
    case ast::binary_op::sub:
    case ast::binary_op::mul:
    case ast::binary_op::div:
    case ast::binary_op::if_:
    case ast::binary_op::else_:
      return false;
  }
  TENZIR_UNREACHABLE();
}

auto is_comparison_or_logical_expr(ast::expression const& expr) -> bool {
  if (auto const* bin = try_as<ast::binary_expr>(*expr.kind)) {
    return is_comparison_or_logical_op(bin->op);
  }
  if (auto const* un = try_as<ast::unary_expr>(*expr.kind)) {
    return un->op == ast::unary_op::not_;
  }
  return false;
}

auto is_pure_constant_impl(ast::expression const& expr, registry const& reg,
                           std::vector<std::string_view>& bound_params)
  -> bool {
  return match(
    *expr.kind,
    [](ast::constant const&) {
      return true;
    },
    [](ast::pkg_dollar_var const&) {
      return true;
    },
    [](ast::type_expr const&) {
      return true;
    },
    [](ast::underscore const&) {
      return true;
    },
    [&](ast::root_field const& field) {
      return std::ranges::contains(bound_params, field.id.name);
    },
    [&](ast::unary_expr const& un) {
      return is_pure_constant_impl(un.expr, reg, bound_params);
    },
    [&](ast::binary_expr const& bin) {
      return is_pure_constant_impl(bin.left, reg, bound_params)
             and is_pure_constant_impl(bin.right, reg, bound_params);
    },
    [&](ast::field_access const& access) {
      return is_pure_constant_impl(access.left, reg, bound_params);
    },
    [&](ast::index_expr const& idx) {
      return is_pure_constant_impl(idx.expr, reg, bound_params)
             and is_pure_constant_impl(idx.index, reg, bound_params);
    },
    [&](ast::unpack const& u) {
      return is_pure_constant_impl(u.expr, reg, bound_params);
    },
    [&](ast::assignment const& assign) {
      return is_pure_constant_impl(assign.right, reg, bound_params);
    },
    [&](ast::lambda_expr const& lambda) {
      auto const saved = bound_params.size();
      for (auto const& param : lambda.params) {
        bound_params.push_back(param.name);
      }
      auto const result = is_pure_constant_impl(lambda.body, reg, bound_params);
      bound_params.resize(saved);
      return result;
    },
    [&](ast::function_call const& call) {
      if (not call.fn.ref.resolved() or not reg.get(call).is_deterministic()) {
        return false;
      }
      return std::ranges::all_of(call.args, [&](ast::expression const& arg) {
        return is_pure_constant_impl(arg, reg, bound_params);
      });
    },
    [&](ast::format_expr const& fmt_expr) {
      return std::ranges::all_of(
        fmt_expr.segments, [&](ast::format_expr::segment const& seg) {
          return match(
            seg,
            [](std::string const&) {
              return true;
            },
            [&](ast::format_expr::replacement const& rep) {
              return is_pure_constant_impl(rep.expr, reg, bound_params);
            });
        });
    },
    [&](ast::record const& rec) {
      return std::ranges::all_of(rec.items, [&](ast::record::item const& item) {
        return match(
          item,
          [&](ast::record::field const& field) {
            return is_pure_constant_impl(field.expr, reg, bound_params);
          },
          [&](ast::spread const& spread) {
            return is_pure_constant_impl(spread.expr, reg, bound_params);
          });
      });
    },
    [&](ast::list const& list) {
      return std::ranges::all_of(list.items, [&](ast::list::item const& item) {
        return match(
          item,
          [&](ast::expression const& inner) {
            return is_pure_constant_impl(inner, reg, bound_params);
          },
          [&](ast::spread const& spread) {
            return is_pure_constant_impl(spread.expr, reg, bound_params);
          });
      });
    },
    [](ast::meta const&) {
      return false;
    },
    [](ast::this_ const&) {
      return false;
    },
    [](ast::pipeline_expr const&) {
      return false;
    },
    [](ast::dollar_var const&) {
      return false;
    },
    [](ast::resolved_secret const&) {
      return false;
    });
}

auto is_pure_constant(ast::expression const& expr, registry const& reg)
  -> bool {
  auto bound_params = std::vector<std::string_view>{};
  return is_pure_constant_impl(expr, reg, bound_params);
}

auto is_constant_option_name(std::string_view name) -> bool {
  return name == "kind" or name == "max_memory";
}

/// Detects an ambiguous `==` captured inside a top-level `if`/`else` key
/// expression, such as `a == b if cond else 0`, `a == b else 0 else 1`, or
/// `a else b == c`. Leaves `if_->right` alone so `a if x == 1 else b` remains
/// a valid shared key expression.
auto ambiguous_eq_conditional_op(ast::binary_expr const& bin)
  -> Option<char const*> {
  if (bin.op != ast::binary_op::if_ and bin.op != ast::binary_op::else_) {
    return None{};
  }
  auto const* op_str = bin.op == ast::binary_op::else_ ? "else" : "if";
  auto const* cur = &bin.left;
  while (auto const* inner = try_as<ast::binary_expr>(*cur->kind)) {
    if (inner->op == ast::binary_op::eq) {
      return op_str;
    }
    if (inner->op == ast::binary_op::if_
        or inner->op == ast::binary_op::else_) {
      cur = &inner->left;
      continue;
    }
    break;
  }
  if (bin.op == ast::binary_op::else_) {
    if (auto const* rhs = try_as<ast::binary_expr>(*bin.right.kind);
        rhs and rhs->op == ast::binary_op::eq) {
      return "else";
    }
  }
  return None{};
}

/// Classifies one positional join key argument into its `(left, right)`
/// expressions. A single outermost `lhs == rhs` splits into an input key (`lhs`)
/// and a right-hand source key (`rhs`). Other expressions evaluate identically
/// on both sides, except predicate-like forms that are rejected with hints.
auto classify_join_key(ast::expression expr, registry const& reg,
                       diagnostic_handler& dh) -> failure_or<JoinKeyPair> {
  auto const loc = expr.get_location();
  if (is<ast::list>(*expr.kind)) {
    diagnostic::error(
      "composite join keys must be comma-separated, not wrapped in a list")
      .primary(loc)
      .hint("write `join a, b { ... }` instead of `join [a, b] { ... }`")
      .emit(dh);
    return failure::promise();
  }
  if (auto const* un = try_as<ast::unary_expr>(*expr.kind);
      un and un->op == ast::unary_op::not_) {
    diagnostic::error("`not` is not supported as a top-level join key")
      .primary(loc)
      .hint("compute a boolean field on each input and join on that field")
      .emit(dh);
    return failure::promise();
  }
  if (auto* bin = try_as<ast::binary_expr>(*expr.kind)) {
    switch (bin->op) {
      case ast::binary_op::and_:
        diagnostic::error("cannot combine join keys with `and`")
          .primary(loc)
          .hint("separate join keys with a comma, e.g. `join a == b, c == d "
                "{ ... }`")
          .emit(dh);
        return failure::promise();
      case ast::binary_op::or_:
        diagnostic::error("cannot combine join keys with `or`")
          .primary(loc)
          .emit(dh);
        return failure::promise();
      case ast::binary_op::neq:
      case ast::binary_op::gt:
      case ast::binary_op::geq:
      case ast::binary_op::lt:
      case ast::binary_op::leq:
      case ast::binary_op::in:
        diagnostic::error("only `==` is supported in join key conditions")
          .primary(loc)
          .note("non-equi join conditions are not supported")
          .emit(dh);
        return failure::promise();
      case ast::binary_op::if_:
      case ast::binary_op::else_:
        if (auto op_str = ambiguous_eq_conditional_op(*bin)) {
          diagnostic::error("ambiguous `==` inside `{}` join key", *op_str)
            .primary(loc)
            .hint("wrap the conditional operand in parentheses, e.g. "
                  "`a == (b {} c)` or `(a {} b) == c`",
                  *op_str, *op_str)
            .emit(dh);
          return failure::promise();
        }
        break;
      case ast::binary_op::eq: {
        if (is_comparison_or_logical_expr(bin->left)
            or is_comparison_or_logical_expr(bin->right)) {
          diagnostic::error(
            "chained comparisons are not supported in join keys")
            .primary(loc)
            .emit(dh);
          return failure::promise();
        }
        if (auto const* root = try_as<ast::root_field>(*bin->left.kind);
            root and is_constant_option_name(root->id.name)
            and is_pure_constant(bin->right, reg)) {
          diagnostic::error("unexpected `==` with option name `{}`",
                            root->id.name)
            .primary(loc)
            .hint("use `=` instead of `==` to pass the `{}` option",
                  root->id.name)
            .emit(dh);
          return failure::promise();
        }
        if (is_pure_constant(bin->left, reg)
            and is_pure_constant(bin->right, reg)) {
          diagnostic::error("join key must reference at least one event field")
            .primary(loc)
            .emit(dh);
          return failure::promise();
        }
        auto left_loc = bin->left.get_location();
        auto right_loc = bin->right.get_location();
        return JoinKeyPair{
          .left = std::move(bin->left),
          .right = std::move(bin->right),
          .left_loc = left_loc,
          .right_loc = right_loc,
        };
      }
      case ast::binary_op::add:
      case ast::binary_op::sub:
      case ast::binary_op::mul:
      case ast::binary_op::div:
        break;
    }
  }
  if (is_pure_constant(expr, reg)) {
    diagnostic::error("join key must reference at least one event field")
      .primary(loc)
      .emit(dh);
    return failure::promise();
  }
  auto right_copy = expr;
  return JoinKeyPair{
    .left = std::move(expr),
    .right = std::move(right_copy),
    .left_loc = loc,
    .right_loc = loc,
  };
}

struct JoinArgs {
  location operator_location;
  std::vector<ast::expression> keys;
  Option<located<std::string>> kind;
  Option<ast::field_path> into;
  Option<located<uint64_t>> max_memory;
};

// ---------------------------------------------------------------------------
// Join key representation, classification, hashing, and equality
// ---------------------------------------------------------------------------

struct JoinKey : std::vector<nova::Data> {
  using vector::vector;
};

struct JoinKeyView : std::vector<nova::RowView<nova::Data>> {
  using vector::vector;
};

template <class Key>
concept JoinKeyType = concepts::one_of<Key, JoinKey, JoinKeyView>;

struct JoinKeyHash {
  using is_transparent = void;

  /// Combines per-component hashes identically to `nova::hash_rows`, so
  /// precomputed column hashes can probe the hash table directly.
  template <JoinKeyType Key>
  auto operator()(Key const& key) const noexcept -> uint64_t {
    auto result = uint64_t{0};
    for (auto const& value : key) {
      result = tenzir::hash(result, static_cast<uint64_t>(nova::hash(
                                      nova::RowView<nova::Data>{value})));
    }
    return result;
  }
};

struct JoinKeyEqual {
  using is_transparent = void;

  /// Every key stored in or probed against the hash table is guaranteed to
  /// contain no `NaN` and no `Secret`, so `nova::equal` is reflexive and
  /// consistent with `JoinKeyHash`.
  template <JoinKeyType Lhs, JoinKeyType Rhs>
  auto operator()(Lhs const& lhs, Rhs const& rhs) const -> bool {
    return std::ranges::equal(lhs, rhs, [](auto const& x, auto const& y) {
      return nova::equal(nova::RowView<nova::Data>{x},
                         nova::RowView<nova::Data>{y});
    });
  }
};

auto materialize_key(JoinKeyView const& views) -> JoinKey {
  auto result = JoinKey{};
  result.reserve(views.size());
  for (auto const& view : views) {
    result.push_back(nova::materialize_data(view));
  }
  return result;
}

// ---------------------------------------------------------------------------
// Memory estimation helpers (key and output-row sizing)
// ---------------------------------------------------------------------------

/// Estimates bytes that a borrowed `RowView<Data>` would occupy once
/// materialized into a `JoinKey` or copied into an output builder.
auto estimate_view_bytes(nova::RowView<nova::Data> const& value) -> uint64_t {
  return match(value, [&]<nova::data_type Tag>(nova::RowView<Tag> const& v) {
    if constexpr (std::same_as<Tag, nova::String>) {
      return static_cast<uint64_t>((*v).size());
    } else if constexpr (std::same_as<Tag, nova::Blob>) {
      return static_cast<uint64_t>((*v).size());
    } else if constexpr (std::same_as<Tag, nova::Secret>) {
      return static_cast<uint64_t>((*v).size());
    } else if constexpr (std::same_as<Tag, nova::List>) {
      auto total = static_cast<uint64_t>(v.length()) * sizeof(nova::Data);
      for (auto i = nova::storage::Index{0}; i < v.length(); ++i) {
        total = detail::saturating_add(total, estimate_view_bytes(v.get(i)));
      }
      return total;
    } else if constexpr (std::same_as<Tag, nova::Record>) {
      auto total = uint64_t{0};
      for (auto const& [name, field_val] : v) {
        total = detail::saturating_add(
          total, static_cast<uint64_t>(sizeof(nova::Data) + name.size()));
        total = detail::saturating_add(total, estimate_view_bytes(field_val));
      }
      return total;
    } else {
      return static_cast<uint64_t>(sizeof(nova::Data));
    }
  });
}

/// Out-of-line bytes owned by `key` (excluding `sizeof(JoinKey)` which lives
/// inline inside the hash-table bucket).
auto estimate_key_heap_bytes(JoinKey const& key) -> uint64_t {
  auto total = static_cast<uint64_t>(key.capacity()) * sizeof(nova::Data);
  for (auto const& component : key) {
    total = detail::saturating_add(
      total, static_cast<uint64_t>(component.approx_heap_bytes()));
  }
  return total;
}

auto estimate_key_view_heap_bytes(JoinKeyView const& views) -> uint64_t {
  auto total = static_cast<uint64_t>(views.size()) * sizeof(nova::Data);
  for (auto const& view : views) {
    total = detail::saturating_add(total, estimate_view_bytes(view));
  }
  return total;
}

auto estimate_event_row_bytes(nova::Events const& events,
                              nova::storage::Index row) -> uint64_t {
  auto total
    = estimate_view_bytes(nova::RowView<nova::Data>{events.data.get(row)});
  auto const meta_bytes = detail::saturating_add(
    static_cast<uint64_t>((*events.meta.name.get(row)).size()),
    static_cast<uint64_t>(sizeof(nova::Time) + sizeof(bool)));
  return detail::saturating_add(total, meta_bytes);
}

// ---------------------------------------------------------------------------
// Key value eligibility & observed type tracking
// ---------------------------------------------------------------------------

enum class KeyStatus : uint8_t {
  eligible,
  ineligible,
  has_secret,
};

/// Classifies a value nested inside a record or list key.
/// Nested nulls are allowed and compare structurally; nested NaN makes the key
/// ineligible (so `nova::equal` stays reflexive on all stored keys); nested
/// secrets are rejected because all Nova secrets compare equal.
auto classify_nested_value(nova::RowView<nova::Data> const& value)
  -> KeyStatus {
  return match(value, []<nova::data_type Tag>(nova::RowView<Tag> const& v) {
    if constexpr (std::same_as<Tag, nova::Secret>) {
      return KeyStatus::has_secret;
    } else if constexpr (std::same_as<Tag, nova::Float>) {
      return std::isnan(*v) ? KeyStatus::ineligible : KeyStatus::eligible;
    } else if constexpr (std::same_as<Tag, nova::List>) {
      auto status = KeyStatus::eligible;
      for (auto i = nova::storage::Index{0}; i < v.length(); ++i) {
        auto item_status = classify_nested_value(v.get(i));
        if (item_status == KeyStatus::has_secret) {
          return KeyStatus::has_secret;
        }
        if (item_status == KeyStatus::ineligible) {
          status = KeyStatus::ineligible;
        }
      }
      return status;
    } else if constexpr (std::same_as<Tag, nova::Record>) {
      auto status = KeyStatus::eligible;
      for (auto const& [_, field_val] : v) {
        auto field_status = classify_nested_value(field_val);
        if (field_status == KeyStatus::has_secret) {
          return KeyStatus::has_secret;
        }
        if (field_status == KeyStatus::ineligible) {
          status = KeyStatus::ineligible;
        }
      }
      return status;
    } else {
      return KeyStatus::eligible;
    }
  });
}

/// Classifies a top-level join-key component.
/// Top-level null (including missing fields) and NaN are ineligible and never
/// match; secrets are rejected.
auto classify_top_level_value(nova::RowView<nova::Data> const& value)
  -> KeyStatus {
  if (is<nova::RowView<nova::Null>>(value)) {
    return KeyStatus::ineligible;
  }
  return classify_nested_value(value);
}

/// Type-compatibility bitmask for diagnosing zero-match type mismatches.
/// Int, UInt, and Float share a single numeric bit because Nova compares them
/// by exact numeric value across types.
enum class KeyTypeGroup : uint32_t {
  numeric = 1u << 0,
  boolean = 1u << 1,
  string = 1u << 2,
  blob = 1u << 3,
  ip = 1u << 4,
  subnet = 1u << 5,
  time = 1u << 6,
  duration = 1u << 7,
  list = 1u << 8,
  record = 1u << 9,
};

template <nova::data_type Tag>
constexpr auto key_type_bit() -> uint32_t {
  if constexpr (concepts::one_of<Tag, nova::Int, nova::UInt, nova::Float>) {
    return static_cast<uint32_t>(KeyTypeGroup::numeric);
  } else if constexpr (std::same_as<Tag, nova::Bool>) {
    return static_cast<uint32_t>(KeyTypeGroup::boolean);
  } else if constexpr (std::same_as<Tag, nova::String>) {
    return static_cast<uint32_t>(KeyTypeGroup::string);
  } else if constexpr (std::same_as<Tag, nova::Blob>) {
    return static_cast<uint32_t>(KeyTypeGroup::blob);
  } else if constexpr (std::same_as<Tag, nova::Ip>) {
    return static_cast<uint32_t>(KeyTypeGroup::ip);
  } else if constexpr (std::same_as<Tag, nova::Subnet>) {
    return static_cast<uint32_t>(KeyTypeGroup::subnet);
  } else if constexpr (std::same_as<Tag, nova::Time>) {
    return static_cast<uint32_t>(KeyTypeGroup::time);
  } else if constexpr (std::same_as<Tag, nova::Duration>) {
    return static_cast<uint32_t>(KeyTypeGroup::duration);
  } else if constexpr (std::same_as<Tag, nova::List>) {
    return static_cast<uint32_t>(KeyTypeGroup::list);
  } else if constexpr (std::same_as<Tag, nova::Record>) {
    return static_cast<uint32_t>(KeyTypeGroup::record);
  } else {
    return 0u;
  }
}

auto format_type_mask(uint32_t mask) -> std::pair<std::string, bool> {
  constexpr auto entries
    = std::array<std::pair<KeyTypeGroup, std::string_view>, 10>{{
      {KeyTypeGroup::numeric, "number"},
      {KeyTypeGroup::boolean, "bool"},
      {KeyTypeGroup::string, "string"},
      {KeyTypeGroup::blob, "blob"},
      {KeyTypeGroup::ip, "ip"},
      {KeyTypeGroup::subnet, "subnet"},
      {KeyTypeGroup::time, "time"},
      {KeyTypeGroup::duration, "duration"},
      {KeyTypeGroup::list, "list"},
      {KeyTypeGroup::record, "record"},
    }};
  auto names = std::vector<std::string_view>{};
  for (auto const& [group, name] : entries) {
    if ((mask & static_cast<uint32_t>(group)) != 0u) {
      names.push_back(name);
    }
  }
  return {fmt::format("{}", fmt::join(names, ", ")), names.size() > 1};
}

/// Evaluators and diagnostic state for one side of the join.
struct SideKeyState {
  std::vector<nova::Evaluator> evaluators;
  std::vector<location> locations;
  std::vector<uint32_t> observed_types;
  bool warned_secret = false;
};

struct KeyEvalResult {
  std::vector<nova::Array<nova::Data>> columns;
  nova::storage::BitMap eligible_mask;
  std::vector<uint64_t> hashes;
};

auto may_contain_nan_or_secret(nova::Array<nova::Data> const& array) -> bool;

template <nova::data_type Tag>
auto typed_may_contain_nan_or_secret(nova::Array<Tag> const& arr) -> bool {
  if constexpr (concepts::one_of<Tag, nova::Float, nova::Secret>) {
    return true;
  } else if constexpr (std::same_as<Tag, nova::List>) {
    return match(
      arr.storage(),
      [](nova::storage::ListStorage const& s) {
        return may_contain_nan_or_secret(s.values());
      },
      [](auto const&) {
        return true;
      });
  } else if constexpr (std::same_as<Tag, nova::Record>) {
    return match(
      arr.storage(),
      [](nova::storage::RecordStorage const& s) {
        return std::ranges::any_of((*s).arrays, [](auto const& field) {
          return may_contain_nan_or_secret(field.data);
        });
      },
      [](auto const&) {
        return true;
      });
  } else {
    return false;
  }
}

auto may_contain_nan_or_secret(nova::Array<nova::Data> const& array) -> bool {
  return match(
    array,
    []<nova::data_type Tag>(nova::Array<Tag> const& arr) {
      return typed_may_contain_nan_or_secret(arr);
    },
    [](nova::UnionArray const& u) {
      return std::ranges::any_of(u.fields(), [](auto const& field) {
        return match(field.data,
                     []<nova::data_type Tag>(nova::Array<Tag> const& arr) {
                       return typed_may_contain_nan_or_secret(arr);
                     });
      });
    });
}

/// Evaluates all key columns, computes the subset of `events.mask` rows whose
/// keys are eligible (no top-level null, no NaN, no Secret), updates
/// `side.observed_types` from the final eligible mask, and hashes eligible rows.
auto evaluate_keys(SideKeyState& side, nova::Events const& events,
                   diagnostic_handler& dh) -> KeyEvalResult {
  auto ctx = nova::EvalCtx{dh};
  auto columns = std::vector<nova::Array<nova::Data>>{};
  columns.reserve(side.evaluators.size());
  for (auto& evaluator : side.evaluators) {
    columns.push_back(evaluator.eval(events, ctx));
  }
  auto eligible = events.mask;
  auto warn_secret = [&](size_t c) {
    if (not side.warned_secret) {
      side.warned_secret = true;
      diagnostic::warning("cannot join on secret values")
        .primary(side.locations[c])
        .note("rows with secret join keys are treated as unmatched")
        .emit(dh);
    }
  };
  for (auto c = size_t{0}; c < columns.size(); ++c) {
    if (not eligible.any()) {
      break;
    }
    auto const& col = columns[c];
    match(
      col,
      [&]<nova::data_type Tag>(nova::Array<Tag> const& arr) {
        if constexpr (std::same_as<Tag, nova::Null>) {
          eligible = nova::storage::BitMap{events.length(), false};
        } else if constexpr (std::same_as<Tag, nova::Secret>) {
          warn_secret(c);
          eligible = nova::storage::BitMap{events.length(), false};
        } else if constexpr (std::same_as<Tag, nova::Float>) {
          auto is_const_nan = match(
            arr.storage(), []<class S>(S const& physical) -> Option<bool> {
              if constexpr (requires { physical.value(); }) {
                return std::isnan(physical.value());
              }
              return None{};
            });
          if (is_const_nan) {
            if (*is_const_nan) {
              eligible = nova::storage::BitMap{events.length(), false};
            }
            return;
          }
          auto next = nova::storage::BitMap::Mutable{eligible};
          for (auto row : nova::storage::true_bits(eligible)) {
            if (std::isnan(*arr.get(row))) {
              next.set(row, false);
            }
          }
          eligible = std::move(next).finish();
        } else if constexpr (concepts::one_of<Tag, nova::List, nova::Record>) {
          auto const_status = match(
            arr.storage(), []<class S>(S const& physical) -> Option<KeyStatus> {
              if constexpr (requires { physical.value(); }) {
                return classify_nested_value(
                  nova::RowView<nova::Data>{physical.get(0)});
              }
              return None{};
            });
          if (const_status) {
            if (*const_status == KeyStatus::has_secret) {
              warn_secret(c);
              eligible = nova::storage::BitMap{events.length(), false};
            } else if (*const_status == KeyStatus::ineligible) {
              eligible = nova::storage::BitMap{events.length(), false};
            }
            return;
          }
          if (not typed_may_contain_nan_or_secret(arr)) {
            return;
          }
          auto next = nova::storage::BitMap::Mutable{eligible};
          for (auto row : nova::storage::true_bits(eligible)) {
            auto status
              = classify_nested_value(nova::RowView<nova::Data>{arr.get(row)});
            if (status == KeyStatus::has_secret) {
              warn_secret(c);
              next.set(row, false);
            } else if (status == KeyStatus::ineligible) {
              next.set(row, false);
            }
          }
          eligible = std::move(next).finish();
        }
      },
      [&](nova::UnionArray const& u) {
        auto nulls = u.alternative_mask<nova::Null>();
        if (nulls.any()) {
          eligible = std::move(eligible).and_not(nulls);
        }
        auto secrets = u.alternative_mask<nova::Secret>();
        if ((eligible & secrets).any()) {
          warn_secret(c);
          eligible = std::move(eligible).and_not(secrets);
        }
        if (not eligible.any()) {
          return;
        }
        auto check_mask = (eligible & u.alternative_mask<nova::Float>())
                          | (eligible & u.alternative_mask<nova::List>())
                          | (eligible & u.alternative_mask<nova::Record>());
        if (check_mask.any()) {
          auto next = nova::storage::BitMap::Mutable{eligible};
          for (auto row : nova::storage::true_bits(check_mask)) {
            auto status = classify_top_level_value(col.get(row));
            if (status == KeyStatus::has_secret) {
              warn_secret(c);
              next.set(row, false);
            } else if (status == KeyStatus::ineligible) {
              next.set(row, false);
            }
          }
          eligible = std::move(next).finish();
        }
      });
  }
  auto hashes = std::vector<uint64_t>{};
  if (eligible.any()) {
    // Commit observed key types only from rows that survived all components.
    for (auto c = size_t{0}; c < columns.size(); ++c) {
      auto& type_mask = side.observed_types[c];
      match(
        columns[c],
        [&]<nova::data_type Tag>(nova::Array<Tag> const&) {
          type_mask |= key_type_bit<Tag>();
        },
        [&](nova::UnionArray const& u) {
          for (auto const& field : u.fields()) {
            if ((eligible & field.present).any()) {
              match(field.data,
                    [&]<nova::data_type Tag>(nova::Array<Tag> const&) {
                      type_mask |= key_type_bit<Tag>();
                    });
            }
          }
        });
    }
    hashes.assign(detail::narrow<size_t>(eligible.true_count()), 0);
    for (auto const& col : columns) {
      nova::hash_rows(col, eligible, hashes);
    }
  }
  return KeyEvalResult{
    .columns = std::move(columns),
    .eligible_mask = std::move(eligible),
    .hashes = std::move(hashes),
  };
}

// ---------------------------------------------------------------------------
// Build-side hash index
// ---------------------------------------------------------------------------

struct RowRef {
  uint32_t batch = 0;
  nova::storage::Index row = 0;
};

class BuildIndex {
public:
  BuildIndex(SideKeyState key_state, bool store_payloads, uint64_t max_memory,
             location memory_loc)
    : key_state_{std::move(key_state)},
      store_payloads_{store_payloads},
      max_memory_{max_memory},
      memory_loc_{memory_loc} {
  }

  /// Adds one right-side batch into the hash index.
  /// Called exclusively from `process_sub` during the build phase.
  auto add(nova::Events events, diagnostic_handler& dh) -> void {
    if (memory_exceeded_ or not events.mask.any()) {
      return;
    }
    auto eval_result = evaluate_keys(key_state_, events, dh);
    if (not eval_result.eligible_mask.any()) {
      return;
    }
    auto batch_idx = uint32_t{0};
    auto compacted = false;
    if (store_payloads_) {
      // Compact once if fewer than half the physical rows are eligible so
      // inactive or ineligible rows do not pin large buffers.
      // `physically_reduce_to_mask` preserves active row order, so the i-th
      // true bit of `eligible_mask` lands at physical row `i`.
      auto const eligible_count = eval_result.eligible_mask.true_count();
      if (static_cast<int64_t>(eligible_count) * 2
          < static_cast<int64_t>(events.length())) {
        events.mask = eval_result.eligible_mask;
        events = nova::physically_reduce_to_mask(std::move(events));
        compacted = true;
      }
      // Retain only the record column; right-side `mask` and `meta` are unused.
      auto batch_data = std::move(events.data);
      auto const batch_bytes = static_cast<uint64_t>(batch_data.approx_bytes());
      if (detail::saturating_add(approx_bytes(), batch_bytes) > max_memory_) {
        fail_memory_limit(dh);
        return;
      }
      batch_idx = detail::narrow<uint32_t>(batches_.size());
      batches_bytes_ = detail::saturating_add(batches_bytes_, batch_bytes);
      batches_.push_back(std::move(batch_data));
    }
    auto key_view = JoinKeyView{};
    key_view.reserve(eval_result.columns.size());
    auto hash_idx = size_t{0};
    auto dense_row = nova::storage::Index{0};
    for (auto row : nova::storage::true_bits(eval_result.eligible_mask)) {
      auto const h = eval_result.hashes[hash_idx++];
      auto const stored_row = compacted ? dense_row++ : row;
      key_view.clear();
      for (auto const& col : eval_result.columns) {
        key_view.push_back(col.get(row));
      }
      auto it = map_.find(key_view, h);
      if (it == map_.end()) {
        // Preflight key memory before materializing so a huge single key
        // cannot bypass `max_memory`.
        auto const predicted_key_bytes = estimate_key_view_heap_bytes(key_view);
        if (detail::saturating_add(approx_bytes(), predicted_key_bytes)
            > max_memory_) {
          fail_memory_limit(dh);
          return;
        }
        auto materialized = materialize_key(key_view);
        auto const key_bytes = estimate_key_heap_bytes(materialized);
        auto const entry_idx = postings_.size();
        map_.emplace(std::move(materialized), entry_idx);
        keys_bytes_ = detail::saturating_add(keys_bytes_, key_bytes);
        if (store_payloads_) {
          auto posting = std::vector<RowRef>{RowRef{batch_idx, stored_row}};
          postings_bytes_ = detail::saturating_add(
            postings_bytes_,
            static_cast<uint64_t>(posting.capacity() * sizeof(RowRef)));
          postings_.push_back(std::move(posting));
        }
      } else if (store_payloads_) {
        auto& posting = postings_[it->second];
        auto const old_cap = posting.capacity();
        posting.push_back(RowRef{batch_idx, stored_row});
        auto const new_cap = posting.capacity();
        if (new_cap > old_cap) {
          postings_bytes_ = detail::saturating_add(
            postings_bytes_,
            static_cast<uint64_t>((new_cap - old_cap) * sizeof(RowRef)));
        }
      }
      if (approx_bytes() > max_memory_) {
        fail_memory_limit(dh);
        return;
      }
    }
  }

  auto clear() -> void {
    batches_ = {};
    map_ = {};
    postings_ = {};
    batches_bytes_ = 0;
    keys_bytes_ = 0;
    postings_bytes_ = 0;
  }

  auto memory_exceeded() const noexcept -> bool {
    return memory_exceeded_;
  }

  auto empty() const noexcept -> bool {
    return map_.empty();
  }

  auto contains(JoinKeyView const& key, uint64_t hash) const -> bool {
    return map_.find(key, hash) != map_.end();
  }

  auto find_posting(JoinKeyView const& key, uint64_t hash) const
    -> Option<size_t> {
    auto it = map_.find(key, hash);
    if (it == map_.end()) {
      return None{};
    }
    return it->second;
  }

  auto posting(size_t index) const -> std::span<RowRef const> {
    return postings_[index];
  }

  auto batch(uint32_t index) const -> nova::Array<nova::Record> const& {
    return batches_[index];
  }

  auto key_state() const -> SideKeyState const& {
    return key_state_;
  }

  auto approx_bytes() const noexcept -> uint64_t {
    // Each robin_map bucket stores `(JoinKey, size_t)` inline plus probe
    // metadata; out-of-line key buffers are tracked separately in `keys_bytes_`.
    auto const table_bytes = static_cast<uint64_t>(map_.bucket_count())
                             * (sizeof(JoinKey) + sizeof(size_t) + 8);
    auto const container_bytes
      = detail::saturating_add(static_cast<uint64_t>(batches_.capacity())
                                 * sizeof(nova::Array<nova::Record>),
                               static_cast<uint64_t>(postings_.capacity())
                                 * sizeof(std::vector<RowRef>));
    return detail::saturating_add(
      detail::saturating_add(batches_bytes_, keys_bytes_),
      detail::saturating_add(
        postings_bytes_, detail::saturating_add(table_bytes, container_bytes)));
  }

private:
  auto fail_memory_limit(diagnostic_handler& dh) -> void {
    memory_exceeded_ = true;
    clear();
    diagnostic::error("`join` exceeded memory limit of {}",
                      format_bytes(max_memory_))
      .primary(memory_loc_)
      .hint("increase `max_memory` or filter the right-hand pipeline")
      .emit(dh);
  }

  SideKeyState key_state_;
  bool store_payloads_ = true;
  uint64_t max_memory_ = default_max_memory;
  location memory_loc_ = location::unknown;
  std::vector<nova::Array<nova::Record>> batches_;
  tsl::robin_map<JoinKey, size_t, JoinKeyHash, JoinKeyEqual> map_;
  std::vector<std::vector<RowRef>> postings_;
  uint64_t batches_bytes_ = 0;
  uint64_t keys_bytes_ = 0;
  uint64_t postings_bytes_ = 0;
  bool memory_exceeded_ = false;
};

// ---------------------------------------------------------------------------
// Output assembly helpers
// ---------------------------------------------------------------------------

auto combine_records(nova::Events left, nova::Array<nova::Data> right_col,
                     nova::storage::BitMap out_mask,
                     Option<ast::field_path> const& into,
                     diagnostic_handler& dh) -> nova::Events {
  if (into) {
    auto data
      = nova::assign_nested_field(std::move(left.data), into->path(),
                                  nova::MaskedArray<nova::Array<nova::Data>>{
                                    std::move(right_col), out_mask},
                                  dh);
    return nova::Events{std::move(data), std::move(out_mask),
                        std::move(left.meta)};
  }
  auto fields = std::array<
    std::pair<std::string_view, nova::Array<nova::Record>::MaskedArray>, 2>{{
    {"left", {nova::Array<nova::Data>{std::move(left.data)}, out_mask}},
    {"right", {std::move(right_col), out_mask}},
  }};
  auto data = nova::Array<nova::Record>::from_fields(fields);
  return nova::Events{std::move(data), std::move(out_mask),
                      std::move(left.meta)};
}

// ---------------------------------------------------------------------------
// Runtime operator
// ---------------------------------------------------------------------------

class Join final : public Operator<nova::Events, nova::Events> {
public:
  Join(JoinArgs args, ir::pipeline right_pipe)
    : args_{std::move(args)}, right_pipe_{std::move(right_pipe)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (ctx.checkpoint_settings()) {
      phase_ = Phase::done;
      diagnostic::error("`join` does not support checkpointing yet")
        .primary(args_.operator_location)
        .emit(ctx);
      co_return;
    }
    if (args_.kind) {
      auto parsed = parse_kind(args_.kind->inner);
      TENZIR_ASSERT(parsed);
      kind_ = *parsed;
    }
    if (args_.keys.empty()) {
      phase_ = Phase::done;
      diagnostic::error("missing join key")
        .primary(args_.operator_location)
        .hint("specify one or more key expressions, e.g. `join id { ... }` or "
              "`join left_id == right_id { ... }`")
        .emit(ctx);
      co_return;
    }
    auto left_exprs = std::vector<ast::expression>{};
    auto right_exprs = std::vector<ast::expression>{};
    auto left_locs = std::vector<location>{};
    auto right_locs = std::vector<location>{};
    left_exprs.reserve(args_.keys.size());
    right_exprs.reserve(args_.keys.size());
    left_locs.reserve(args_.keys.size());
    right_locs.reserve(args_.keys.size());
    for (auto& key_expr : args_.keys) {
      auto pair = classify_join_key(std::move(key_expr), ctx.reg(), ctx.dh());
      if (not pair) {
        phase_ = Phase::done;
        co_return;
      }
      left_locs.push_back(pair->left_loc);
      right_locs.push_back(pair->right_loc);
      left_exprs.push_back(std::move(pair->left));
      right_exprs.push_back(std::move(pair->right));
    }
    args_.keys.clear();
    auto left_evaluators = std::vector<nova::Evaluator>{};
    left_evaluators.reserve(left_exprs.size());
    for (auto& expr : left_exprs) {
      auto evaluator = co_await nova::Evaluator::make(std::move(expr), ctx);
      if (not evaluator) {
        phase_ = Phase::done;
        co_return;
      }
      left_evaluators.push_back(std::move(*evaluator));
    }
    auto right_evaluators = std::vector<nova::Evaluator>{};
    right_evaluators.reserve(right_exprs.size());
    for (auto& expr : right_exprs) {
      auto evaluator = co_await nova::Evaluator::make(std::move(expr), ctx);
      if (not evaluator) {
        phase_ = Phase::done;
        co_return;
      }
      right_evaluators.push_back(std::move(*evaluator));
    }
    left_key_state_ = SideKeyState{
      .evaluators = std::move(left_evaluators),
      .locations = std::move(left_locs),
      .observed_types = std::vector<uint32_t>(left_exprs.size(), 0),
    };
    auto right_key_state = SideKeyState{
      .evaluators = std::move(right_evaluators),
      .locations = std::move(right_locs),
      .observed_types = std::vector<uint32_t>(right_exprs.size(), 0),
    };
    auto const store_payloads
      = kind_ == JoinKind::inner or kind_ == JoinKind::left;
    auto const max_memory
      = args_.max_memory ? args_.max_memory->inner : default_max_memory;
    auto const memory_loc
      = args_.max_memory ? args_.max_memory->source : args_.operator_location;
    build_index_.emplace(std::move(right_key_state), store_payloads, max_memory,
                         memory_loc);
    phase_ = Phase::building;
    auto sub = co_await ctx.plan_and_spawn_sub<void>(data{"right"},
                                                     std::move(right_pipe_));
    if (not sub) {
      phase_ = Phase::done;
      build_index_ = None{};
      co_return;
    }
    co_return;
  }

  auto state() -> OperatorState override {
    switch (phase_) {
      case Phase::building:
      case Phase::draining_fanout:
        return OperatorState::blocked;
      case Phase::probing:
        return OperatorState::normal;
      case Phase::done:
        return OperatorState::done;
    }
    TENZIR_UNREACHABLE();
  }

  /// Receives right-side batches on the subpipeline bridge task.
  /// Sole writer to `build_index_` while `phase_ == Phase::building`.
  auto process_sub(SubKeyView key, nova::Events events,
                   Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(key, push);
    if (stopped_during_build_.load(std::memory_order_acquire)
        or build_memory_exceeded_.load(std::memory_order_acquire)) {
      co_return;
    }
    TENZIR_ASSERT(build_index_);
    build_index_->add(std::move(events), ctx.dh());
    if (build_index_->memory_exceeded()) {
      build_memory_exceeded_.store(true, std::memory_order_release);
      drain_notify_.notify_one();
    }
    co_return;
  }

  /// Seals the build index once the right subpipeline completes cleanly.
  auto finish_sub(SubKeyView key, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(key, push, ctx);
    if (phase_ != Phase::building) {
      if (build_index_) {
        build_index_->clear();
      }
      co_return;
    }
    TENZIR_ASSERT(build_index_);
    if (build_index_->memory_exceeded()) {
      phase_ = Phase::done;
      build_index_->clear();
      co_return;
    }
    if (build_index_->empty()
        and (kind_ == JoinKind::inner or kind_ == JoinKind::semi)) {
      // No left row can ever match an empty right side.
      phase_ = Phase::done;
      build_index_->clear();
      co_return;
    }
    phase_ = Phase::probing;
    co_return;
  }

  /// Never publish a partial build index if the right subpipeline failed.
  auto finish_sub(SubKeyView key, failure error, Push<nova::Events>& push,
                  OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(key, error, push, ctx);
    phase_ = Phase::done;
    if (build_index_) {
      build_index_->clear();
    }
    co_return;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    // The executor forwards `GracefulStop` to the right subpipeline, which may
    // truncate it mid-build. Report an interrupted join rather than silently
    // exiting with zero output or probing a partial build table.
    if (phase_ == Phase::building) {
      stopped_during_build_.store(true, std::memory_order_release);
      phase_ = Phase::done;
      if (not build_memory_exceeded_.load(std::memory_order_acquire)) {
        diagnostic::error(
          "`join` interrupted before the right-hand pipeline finished")
          .primary(args_.operator_location)
          .emit(ctx);
      }
    }
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_await drain_notify_.wait();
    co_return DrainTick{};
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not result.try_as<DrainTick>()) {
      co_return;
    }
    if (phase_ == Phase::building
        and build_memory_exceeded_.load(std::memory_order_acquire)) {
      // Stop accepting upstream left input immediately; the error emitted by
      // `fail_memory_limit` cancels the pipeline and right subpipeline under
      // standard handlers, while `BuildIndex::clear()` already released memory.
      phase_ = Phase::done;
      co_return;
    }
    if (phase_ != Phase::draining_fanout or not pending_fanout_) {
      co_return;
    }
    if (auto chunk = next_fanout_chunk(*pending_fanout_, ctx.dh())) {
      co_await push(std::move(*chunk));
    }
    if (phase_ == Phase::done) {
      pending_fanout_ = None{};
      co_return;
    }
    if (pending_fanout_->finished()) {
      pending_fanout_ = None{};
      phase_ = Phase::probing;
      co_return;
    }
    drain_notify_.notify_one();
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (phase_ != Phase::probing or not input.mask.any()) {
      co_return;
    }
    TENZIR_ASSERT(build_index_);
    TENZIR_ASSERT(not pending_fanout_);
    // Fast path when the right-hand side had no eligible rows.
    if (build_index_->empty()) {
      if (kind_ == JoinKind::anti) {
        co_await push(std::move(input));
        co_return;
      }
      TENZIR_ASSERT(kind_ == JoinKind::left);
      auto out_mask = input.mask;
      auto right_col = nova::Array<nova::Data>{
        nova::Array<nova::Null>{nova::storage::NullStorage{input.length()}}};
      co_await push(combine_records(std::move(input), std::move(right_col),
                                    std::move(out_mask), args_.into, ctx.dh()));
      co_return;
    }
    auto eval_result = evaluate_keys(left_key_state_, input, ctx.dh());
    if (kind_ == JoinKind::semi or kind_ == JoinKind::anti) {
      co_await process_semi_anti(std::move(input), eval_result, push);
      co_return;
    }
    co_await process_inner_left(std::move(input), eval_result, push, ctx.dh());
  }

  auto finalize(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push);
    if (phase_ == Phase::done or not build_index_) {
      co_return FinalizeBehavior::done;
    }
    TENZIR_ASSERT(not pending_fanout_);
    maybe_warn_type_mismatch(ctx.dh());
    co_return FinalizeBehavior::done;
  }

private:
  enum class Phase : uint8_t {
    building,
    probing,
    draining_fanout,
    done,
  };

  struct DrainTick {};

  struct RowMatches {
    nova::storage::Index left_row = 0;
    Option<size_t> posting_idx;
  };

  struct PendingFanout {
    nova::Events input;
    std::vector<RowMatches> rows;
    size_t row_pos = 0;
    size_t match_pos = 0;

    auto finished() const noexcept -> bool {
      return row_pos >= rows.size();
    }
  };

  auto process_semi_anti(nova::Events input, KeyEvalResult const& eval_result,
                         Push<nova::Events>& push) -> Task<void> {
    auto out_mask = nova::storage::BitMap::Mutable{
      nova::storage::BitMap{input.length(), false}};
    if (kind_ == JoinKind::anti) {
      auto const ineligible = input.mask.and_not(eval_result.eligible_mask);
      for (auto row : nova::storage::true_bits(ineligible)) {
        out_mask.set(row, true);
      }
    }
    auto key_view = JoinKeyView{};
    key_view.reserve(eval_result.columns.size());
    auto hash_idx = size_t{0};
    for (auto row : nova::storage::true_bits(eval_result.eligible_mask)) {
      auto const h = eval_result.hashes[hash_idx++];
      key_view.clear();
      for (auto const& col : eval_result.columns) {
        key_view.push_back(col.get(row));
      }
      auto const matched = build_index_->contains(key_view, h);
      if (matched) {
        had_match_ = true;
      }
      if ((kind_ == JoinKind::semi and matched)
          or (kind_ == JoinKind::anti and not matched)) {
        out_mask.set(row, true);
      }
    }
    auto final_mask = std::move(out_mask).finish();
    if (final_mask.any()) {
      co_await push(nova::Events{std::move(input.data), std::move(final_mask),
                                 std::move(input.meta)});
    }
  }

  auto process_inner_left(nova::Events input, KeyEvalResult const& eval_result,
                          Push<nova::Events>& push, diagnostic_handler& dh)
    -> Task<void> {
    auto row_matches = std::vector<RowMatches>{};
    row_matches.reserve(detail::narrow<size_t>(input.active_count()));
    auto key_view = JoinKeyView{};
    key_view.reserve(eval_result.columns.size());
    auto hash_idx = size_t{0};
    auto needs_chunking = false;
    auto estimated_right_bytes = uint64_t{0};
    for (auto row : nova::storage::true_bits(input.mask)) {
      auto posting = Option<size_t>{};
      if (eval_result.eligible_mask.get(row)) {
        auto const h = eval_result.hashes[hash_idx++];
        key_view.clear();
        for (auto const& col : eval_result.columns) {
          key_view.push_back(col.get(row));
        }
        posting = build_index_->find_posting(key_view, h);
      }
      if (posting) {
        auto const refs = build_index_->posting(*posting);
        had_match_ = true;
        if (refs.size() > 1) {
          needs_chunking = true;
        } else if (not needs_chunking) {
          auto const ref = refs.front();
          auto const row_bytes = estimate_view_bytes(nova::RowView<nova::Data>{
            build_index_->batch(ref.batch).get(ref.row)});
          estimated_right_bytes
            = detail::saturating_add(estimated_right_bytes, row_bytes);
          if (estimated_right_bytes > output_chunk_bytes) {
            needs_chunking = true;
          }
        }
        row_matches.push_back(RowMatches{row, posting});
      } else if (kind_ == JoinKind::left) {
        row_matches.push_back(RowMatches{row, None{}});
      }
    }
    if (row_matches.empty()) {
      co_return;
    }
    if (not needs_chunking and input.length() <= output_chunk_rows
        and detail::narrow<nova::storage::Index>(row_matches.size())
              <= output_chunk_rows) {
      // Zero-copy left fast path: every active left row yields 0 or 1 outputs
      // and the materialized right column fits well within one chunk budget.
      auto out_mask = kind_ == JoinKind::left
                        ? input.mask
                        : nova::storage::BitMap{input.length(), false};
      auto matched_mask_builder = Option<nova::storage::BitMap::Mutable>{};
      if (kind_ == JoinKind::inner) {
        matched_mask_builder.emplace(
          nova::storage::BitMap{input.length(), false});
      }
      auto right_builder = nova::ArrayBuilder<nova::Data>{};
      for (auto const& rm : row_matches) {
        right_builder.skip_n(rm.left_row - right_builder.length());
        if (rm.posting_idx) {
          auto const ref = build_index_->posting(*rm.posting_idx).front();
          nova::append_row(right_builder,
                           nova::RowView<nova::Data>{
                             build_index_->batch(ref.batch).get(ref.row)});
          if (matched_mask_builder) {
            matched_mask_builder->set(rm.left_row, true);
          }
        } else {
          right_builder.null();
        }
      }
      right_builder.skip_n(input.length() - right_builder.length());
      if (matched_mask_builder) {
        out_mask = std::move(*matched_mask_builder).finish();
      }
      TENZIR_ASSERT(out_mask.any());
      co_await push(combine_records(std::move(input), right_builder.finish(),
                                    std::move(out_mask), args_.into, dh));
      co_return;
    }
    // General chunked path (N:M fan-out or wide matched right rows): emit the
    // first bounded chunk synchronously and yield remaining chunks through
    // `process_task()` so downstream `HardStop` (such as `head`) preempts
    // promptly and output buffers stay bounded.
    auto pending = PendingFanout{
      .input = std::move(input),
      .rows = std::move(row_matches),
      .row_pos = 0,
      .match_pos = 0,
    };
    if (auto chunk = next_fanout_chunk(pending, dh)) {
      co_await push(std::move(*chunk));
    }
    if (phase_ == Phase::done) {
      co_return;
    }
    if (not pending.finished()) {
      pending_fanout_ = std::move(pending);
      phase_ = Phase::draining_fanout;
      drain_notify_.notify_one();
    }
  }

  auto next_fanout_chunk(PendingFanout& pending, diagnostic_handler& dh)
    -> Option<nova::Events> {
    if (pending.finished()) {
      return None{};
    }
    auto left_assembler = nova::EventsAssembler{};
    auto right_builder = nova::ArrayBuilder<nova::Data>{};
    auto emitted = nova::storage::Index{0};
    auto emitted_bytes = uint64_t{0};
    auto hit_byte_limit = false;
    while (emitted < output_chunk_rows and not hit_byte_limit
           and pending.row_pos < pending.rows.size()) {
      auto const& rm = pending.rows[pending.row_pos];
      auto const left_row_bytes
        = estimate_event_row_bytes(pending.input, rm.left_row);
      if (not rm.posting_idx) {
        if (emitted == 0 and left_row_bytes > max_single_row_bytes) {
          phase_ = Phase::done;
          diagnostic::error("`join` matched row exceeds maximum output batch "
                            "size")
            .primary(args_.operator_location)
            .emit(dh);
          return None{};
        }
        if (emitted > 0
            and detail::saturating_add(emitted_bytes, left_row_bytes)
                  > output_chunk_bytes) {
          hit_byte_limit = true;
          break;
        }
        left_assembler.append(pending.input, rm.left_row);
        right_builder.null();
        emitted_bytes = detail::saturating_add(emitted_bytes, left_row_bytes);
        ++emitted;
        ++pending.row_pos;
        pending.match_pos = 0;
        continue;
      }
      auto const refs = build_index_->posting(*rm.posting_idx);
      while (emitted < output_chunk_rows and pending.match_pos < refs.size()) {
        auto const& ref = refs[pending.match_pos];
        auto const right_view = nova::RowView<nova::Data>{
          build_index_->batch(ref.batch).get(ref.row)};
        auto const row_bytes = detail::saturating_add(
          left_row_bytes, estimate_view_bytes(right_view));
        if (emitted == 0 and row_bytes > max_single_row_bytes) {
          phase_ = Phase::done;
          diagnostic::error("`join` matched row exceeds maximum output batch "
                            "size")
            .primary(args_.operator_location)
            .emit(dh);
          return None{};
        }
        if (emitted > 0
            and detail::saturating_add(emitted_bytes, row_bytes)
                  > output_chunk_bytes) {
          hit_byte_limit = true;
          break;
        }
        ++pending.match_pos;
        left_assembler.append(pending.input, rm.left_row);
        nova::append_row(right_builder, right_view);
        emitted_bytes = detail::saturating_add(emitted_bytes, row_bytes);
        ++emitted;
      }
      if (not hit_byte_limit and pending.match_pos >= refs.size()) {
        ++pending.row_pos;
        pending.match_pos = 0;
      }
    }
    if (emitted == 0) {
      return None{};
    }
    auto left_events = std::move(left_assembler).finish();
    auto out_mask = left_events.mask;
    return combine_records(std::move(left_events), right_builder.finish(),
                           std::move(out_mask), args_.into, dh);
  }

  auto maybe_warn_type_mismatch(diagnostic_handler& dh) const -> void {
    if (had_match_ or not build_index_) {
      return;
    }
    auto const& right_state = build_index_->key_state();
    TENZIR_ASSERT_EQ(left_key_state_.observed_types.size(),
                     right_state.observed_types.size());
    for (auto i = size_t{0}; i < left_key_state_.observed_types.size(); ++i) {
      auto const lt = left_key_state_.observed_types[i];
      auto const rt = right_state.observed_types[i];
      if (lt != 0u and rt != 0u and (lt & rt) == 0u) {
        auto const [left_names, left_plural] = format_type_mask(lt);
        auto const [right_names, right_plural] = format_type_mask(rt);
        auto diag = diagnostic::warning(
          "`join` matched no events due to incompatible key types");
        if (left_key_state_.locations[i] != right_state.locations[i]) {
          diag = std::move(diag)
                   .primary(left_key_state_.locations[i], "input key has {} {}",
                            left_plural ? "types" : "type", left_names)
                   .secondary(right_state.locations[i], "source key has {} {}",
                              right_plural ? "types" : "type", right_names);
        } else {
          diag = std::move(diag)
                   .primary(left_key_state_.locations[i])
                   .note("input key has {} {}, source key has {} {}",
                         left_plural ? "types" : "type", left_names,
                         right_plural ? "types" : "type", right_names);
        }
        std::move(diag).emit(dh);
        return;
      }
    }
  }

  JoinArgs args_;
  ir::pipeline right_pipe_;
  JoinKind kind_ = JoinKind::inner;
  Phase phase_ = Phase::building;
  Atomic<bool> stopped_during_build_{false};
  Atomic<bool> build_memory_exceeded_{false};
  SideKeyState left_key_state_;
  bool had_match_ = false;
  Option<BuildIndex> build_index_;
  Option<PendingFanout> pending_fanout_;
  mutable Notify drain_notify_;
};

// ---------------------------------------------------------------------------
// IR operator & optimizer pushdowns
// ---------------------------------------------------------------------------

auto build_field_path_expr(std::span<ast::field_path::segment const> segments)
  -> ast::expression {
  TENZIR_ASSERT(not segments.empty());
  auto expr = ast::expression{ast::root_field{
    segments[0].id,
    segments[0].has_question_mark,
  }};
  for (auto i = size_t{1}; i < segments.size(); ++i) {
    auto const loc = segments[i].id.location;
    expr = ast::expression{ast::field_access{
      std::move(expr),
      loc,
      segments[i].has_question_mark,
      segments[i].id,
    }};
  }
  return expr;
}

auto strip_field_path_prefix(ast::field_path const& fp, size_t prefix_len)
  -> ast::field_path {
  TENZIR_ASSERT_LT(prefix_len, fp.path().size());
  auto segments = std::vector<ast::field_path::segment>{
    fp.path().begin() + detail::narrow<ptrdiff_t>(prefix_len),
    fp.path().end(),
  };
  auto expr = build_field_path_expr(segments);
  return ast::field_path::make(std::move(expr), false, std::move(segments));
}

auto replace_field_path_prefix(ast::field_path const& fp, size_t prefix_len,
                               ast::field_path const& replacement)
  -> ast::expression {
  TENZIR_ASSERT_GT(prefix_len, 0u);
  TENZIR_ASSERT_LEQ(prefix_len, fp.path().size());
  TENZIR_ASSERT(not replacement.path().empty());
  auto segments = std::vector<ast::field_path::segment>{
    replacement.path().begin(),
    replacement.path().end(),
  };
  segments.back().has_question_mark
    = fp.path()[prefix_len - 1].has_question_mark;
  segments.insert(segments.end(),
                  fp.path().begin() + detail::narrow<ptrdiff_t>(prefix_len),
                  fp.path().end());
  return build_field_path_expr(segments);
}

auto has_prefix_segments(ast::field_path const& fp,
                         std::span<std::string_view const> prefix) -> bool {
  if (fp.path().size() < prefix.size()) {
    return false;
  }
  for (auto i = size_t{0}; i < prefix.size(); ++i) {
    if (fp.path()[i].id.name != prefix[i]) {
      return false;
    }
  }
  return true;
}

auto is_prefix_of_segments(ast::field_path const& fp,
                           std::span<std::string_view const> target) -> bool {
  if (fp.path().size() > target.size()) {
    return false;
  }
  for (auto i = size_t{0}; i < fp.path().size(); ++i) {
    if (fp.path()[i].id.name != target[i]) {
      return false;
    }
  }
  return true;
}

auto make_prefix_stripper(std::vector<std::string_view> prefix)
  -> ast::FieldSubstitution {
  return [prefix = std::move(prefix)](
           ast::field_path const& fp) -> Option<ast::expression> {
    if (not has_prefix_segments(fp, prefix)) {
      return None{};
    }
    if (fp.path().size() == prefix.size()) {
      return ast::expression{ast::this_{fp.get_location()}};
    }
    return strip_field_path_prefix(fp, prefix.size()).unwrap();
  };
}

/// Extracts the sub-projection under `prefix` from a downstream projection.
/// Returns `None` if downstream needs all fields or references `prefix` itself
/// (or a parent of `prefix`).
auto extract_sub_projection(Option<ir::OptimizeProjection> const& proj,
                            std::span<std::string_view const> prefix)
  -> Option<ir::OptimizeProjection> {
  if (not proj) {
    return None{};
  }
  auto result = Option<ir::OptimizeProjection>{ir::OptimizeProjection{}};
  for (auto const& fp : *proj) {
    if (is_prefix_of_segments(fp, prefix)) {
      return None{};
    }
    if (has_prefix_segments(fp, prefix)) {
      ir::add_to_projection(result, strip_field_path_prefix(fp, prefix.size()));
    }
  }
  return result;
}

auto is_plain_field_path(ast::expression const& expr)
  -> Option<ast::field_path> {
  auto fp = ast::field_path::try_from(expr);
  if (not fp or fp->path().empty()) {
    return None{};
  }
  for (auto const& seg : fp->path()) {
    if (seg.has_question_mark) {
      return None{};
    }
  }
  return fp;
}

auto is_equal_congruent_constant(ast::expression const& expr) -> bool {
  return match(
    *expr.kind,
    [](ast::constant const&) {
      return true;
    },
    [](ast::unary_expr const& un) {
      return (un.op == ast::unary_op::pos or un.op == ast::unary_op::neg)
             and is_equal_congruent_constant(un.expr);
    },
    [](ast::list const& list) {
      return std::ranges::all_of(list.items, [](ast::list::item const& item) {
        auto const* e = try_as<ast::expression>(item);
        return e and is_equal_congruent_constant(*e);
      });
    },
    [](ast::record const& rec) {
      return std::ranges::all_of(rec.items, [](ast::record::item const& item) {
        auto const* f = try_as<ast::record::field>(item);
        return f and is_equal_congruent_constant(f->expr);
      });
    },
    [](auto const&) {
      return false;
    });
}

auto is_equal_congruent_operand(ast::expression const& expr) -> bool {
  if (auto fp = ast::field_path::try_from(expr);
      fp and not fp->path().empty()) {
    return true;
  }
  return is_equal_congruent_constant(expr);
}

/// Returns true only when `expr` is invariant under `nova::equal` substitution
/// of its referenced field paths (comparisons and `in` over field paths and
/// constants, plus logical connectives). Rejects expressions such as
/// `f"{id}" == "1"` or type-sensitive calls that distinguish equal values like
/// `1` and `1.0`.
auto is_equal_congruent_predicate(ast::expression const& expr) -> bool {
  if (auto fp = ast::field_path::try_from(expr);
      fp and not fp->path().empty()) {
    return true;
  }
  return match(
    *expr.kind,
    [](ast::unary_expr const& un) {
      return un.op == ast::unary_op::not_
             and is_equal_congruent_predicate(un.expr);
    },
    [](ast::binary_expr const& bin) {
      switch (bin.op) {
        case ast::binary_op::and_:
        case ast::binary_op::or_:
          return is_equal_congruent_predicate(bin.left)
                 and is_equal_congruent_predicate(bin.right);
        case ast::binary_op::eq:
        case ast::binary_op::neq:
        case ast::binary_op::lt:
        case ast::binary_op::leq:
        case ast::binary_op::gt:
        case ast::binary_op::geq:
        case ast::binary_op::in:
          return is_equal_congruent_operand(bin.left)
                 and is_equal_congruent_operand(bin.right);
        case ast::binary_op::add:
        case ast::binary_op::sub:
        case ast::binary_op::mul:
        case ast::binary_op::div:
        case ast::binary_op::if_:
        case ast::binary_op::else_:
          return false;
      }
      TENZIR_UNREACHABLE();
    },
    [](auto const&) {
      return false;
    });
}

auto serialize_expression(ast::expression const& expr) -> caf::byte_buffer {
  auto buf = caf::byte_buffer{};
  auto sink = caf::binary_serializer{buf};
  auto ok = sink.apply(const_cast<ast::expression&>(expr));
  TENZIR_ASSERT(ok);
  return buf;
}

auto describe_join() -> Description {
  auto d = Describer<JoinArgs>{};
  d.name("join");
  d.operator_location(&JoinArgs::operator_location);
  // Use `optional_variadic` so `validate()` emits a targeted missing-key
  // diagnostic and leaves zero keys open for a future `kind="cross"` mode.
  auto keys_arg
    = d.optional_variadic("key", &JoinArgs::keys, "expr|left==right");
  auto kind_arg = d.named("kind", &JoinArgs::kind, "string");
  auto into_arg = d.named("into", &JoinArgs::into);
  auto max_memory_arg = d.named("max_memory", &JoinArgs::max_memory);
  // `JoinIr::optimize()` optimizes the right-hand subpipeline itself with
  // pushed projections, filters, and order hints.
  d.pipeline(SubOptimize::off);
  d.inline_pipeline();
  d.validate([=](DescribeCtx& ctx) -> Empty {
    auto keys = ctx.get_all(keys_arg);
    if (keys.empty()) {
      diagnostic::error("missing join key")
        .primary(ctx.operator_location())
        .hint("specify one or more key expressions, e.g. `join id { ... }` "
              "or `join left_id == right_id { ... }`")
        .emit(ctx);
    } else {
      // Validate every resolved key; entries referencing an unbound `$var`
      // (such as `$group`) return `None` until the instantiating pass binds
      // them and re-runs this validator.
      for (auto& key : keys) {
        if (key) {
          std::ignore = classify_join_key(std::move(*key), ctx.reg(), ctx);
        }
      }
    }
    auto parsed_kind = JoinKind::inner;
    if (auto kind_val = ctx.get(kind_arg)) {
      if (auto parsed = parse_kind(kind_val->inner)) {
        parsed_kind = *parsed;
      } else {
        diagnostic::error(
          "`kind` must be one of `\"inner\"`, `\"left\"`, `\"semi\"`, or "
          "`\"anti\"`")
          .primary(*kind_val)
          .emit(ctx);
      }
    }
    if (auto into_val = ctx.get(into_arg)) {
      if (parsed_kind == JoinKind::semi or parsed_kind == JoinKind::anti) {
        diagnostic::error("`into` cannot be used with `kind=\"{}\"`",
                          kind_name(parsed_kind))
          .primary(into_val->get_location())
          .emit(ctx);
      } else if (into_val->path().empty()) {
        diagnostic::error("`into` must be a field path, not `this`")
          .primary(into_val->get_location())
          .emit(ctx);
      }
    }
    if (auto mem = ctx.get(max_memory_arg); mem and mem->inner == 0) {
      diagnostic::error("`max_memory` must be greater than zero")
        .primary(*mem)
        .emit(ctx);
    }
    return {};
  });
  return d.only_arguments();
}

using JoinArguments = OperatorArguments<JoinArgs, describe_join>;

class JoinIr final : public ir::Operator {
public:
  JoinIr() = default;

  explicit JoinIr(JoinArguments args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "join_ir";
  }

  auto display_name() const -> std::string override {
    return "join";
  }

  auto main_location() const -> location override {
    return args_.main_location();
  }

  auto copy() const -> Box<ir::Operator> override {
    return JoinIr{args_};
  }

  auto move() && -> Box<ir::Operator> override {
    return JoinIr{std::move(args_)};
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    return args_.substitute(ctx, instantiate);
  }

  auto infer_type(element_type_tag input, base_ctx ctx) const
    -> failure_or<element_type_tag> override {
    if (input.is_not<nova::Events>()) {
      diagnostic::error("operator does not accept {}", input)
        .primary(args_.main_location())
        .docs("https://tenzir.com/docs/reference/operators/join")
        .emit(ctx);
      return failure::promise();
    }
    auto pipe = args_.pipe();
    TENZIR_ASSERT(pipe);
    TRY(auto right_ty, pipe->inner.infer_type(tag_v<void>, ctx));
    if (right_ty.is_not<nova::Events>()) {
      diagnostic::error("`join` subpipeline must be a source producing events")
        .primary(pipe->source)
        .emit(ctx);
      return failure::promise();
    }
    return tag_v<nova::Events>;
  }

  auto optimize(ir::OptimizeRequest req,
                const ir::OptimizeCtx& octx) && -> ir::OptimizeResult override {
    auto pipe = args_.pipe();
    TENZIR_ASSERT(pipe);
    auto args = args_.get();
    auto kind = JoinKind::inner;
    if (args.kind) {
      auto parsed = parse_kind(args.kind->inner);
      TENZIR_ASSERT(parsed);
      kind = *parsed;
    }
    auto const& into = args.into;
    auto noop_dh = null_diagnostic_handler{};
    auto key_pairs = std::vector<JoinKeyPair>{};
    key_pairs.reserve(args.keys.size());
    for (auto& key_expr : args.keys) {
      auto pair = classify_join_key(std::move(key_expr), octx.reg, noop_dh);
      if (not pair) {
        // Fallback barrier if keys could not be classified cleanly.
        auto right_opt = std::move(pipe->inner)
                           .optimize(
                             ir::OptimizeRequest{
                               .filter = {},
                               .order = EventOrder::ordered,
                             },
                             ir::OptimizeCtx{
                               .reg = octx.reg,
                               .can_any_op_reorder = false,
                             });
        pipe->inner = std::move(right_opt.replacement);
        pipe->inner.prepend(std::move(right_opt.filter), octx.reg);
        return std::move(*this).ir::Operator::optimize(std::move(req), octx);
      }
      key_pairs.push_back(std::move(*pair));
    }

    auto const no_meta
      = ast::MetaSubstitution{[](ast::meta const&) -> Option<ast::expression> {
          return None{};
        }};
    auto into_segments = std::vector<std::string_view>{};
    if (into) {
      into_segments.reserve(into->path().size());
      for (auto const& seg : into->path()) {
        into_segments.push_back(seg.id.name);
      }
    }
    auto const left_prefix = std::vector<std::string_view>{"left"};
    auto const right_prefix
      = into ? into_segments : std::vector<std::string_view>{"right"};
    auto const left_stripper = make_prefix_stripper(left_prefix);
    auto const right_stripper = make_prefix_stripper(right_prefix);

    // 1. Split downstream filters into left upstream, direct right pushdown,
    // and filters kept behind the join.
    auto left_filter = ir::OptimizeFilter{};
    auto direct_right_filter = ir::OptimizeFilter{};
    auto kept_filter = ir::OptimizeFilter{};
    auto any_kept = false;
    auto const into_touched
      = into ? ast::ExprRefs{.field_paths = {*into}} : ast::ExprRefs{};

    // Inner and left joins may emit several rows for one left row, and inner
    // joins may emit several rows for one right row. A predicate in front of
    // such a leg runs once for all of these rows, so it must yield the same
    // value for each of them. A non-repeatable one, such as `random() < 0.5`,
    // would otherwise keep or drop all copies together instead of each one.
    auto const left_fans_out
      = kind == JoinKind::inner or kind == JoinKind::left;
    for (auto& expr : req.filter) {
      auto const must_be_infallible_left = any_kept or kind != JoinKind::left;
      auto const repeatable = ir::is_repeatable(expr, octx.reg);
      auto left_candidate = Option<ast::expression>{};
      if (kind == JoinKind::semi or kind == JoinKind::anti) {
        left_candidate = expr;
      } else if (into) {
        if (auto refs = ast::collect_refs(expr);
            refs and not refs->overlaps(into_touched)) {
          left_candidate = expr;
        }
      } else {
        if (auto sub = ast::substitute_refs(expr, octx.reg, left_stripper);
            sub
            and ir::expression_size(*sub) <= ir::max_rewritten_predicate_size) {
          left_candidate = std::move(*sub);
        }
      }
      if (left_candidate and (repeatable or not left_fans_out)) {
        *left_candidate
          = ir::simplify_predicate(std::move(*left_candidate), octx.reg);
        if (not must_be_infallible_left or ir::is_infallible(*left_candidate)) {
          left_filter.push_back(std::move(*left_candidate));
          continue;
        }
      }
      if (kind == JoinKind::inner and repeatable) {
        if (auto sub
            = ast::substitute_refs(expr, octx.reg, right_stripper, no_meta);
            sub
            and ir::expression_size(*sub) <= ir::max_rewritten_predicate_size) {
          *sub = ir::simplify_predicate(std::move(*sub), octx.reg);
          // Predicates pushed into the right build side run on unmatched right
          // rows too, so they must never fail the pipeline.
          if (ir::is_infallible(*sub)) {
            direct_right_filter.push_back(std::move(*sub));
            continue;
          }
        }
      }
      kept_filter.push_back(std::move(expr));
      any_kept = true;
    }

    // 2. Equivalence-class filter propagation across plain `L == R` key pairs.
    struct PlainKeyPair {
      ast::field_path left;
      ast::field_path right;
    };
    auto plain_keys = std::vector<PlainKeyPair>{};
    for (auto const& kp : key_pairs) {
      auto lfp = is_plain_field_path(kp.left);
      auto rfp = is_plain_field_path(kp.right);
      if (lfp and rfp) {
        plain_keys.push_back(PlainKeyPair{std::move(*lfp), std::move(*rfp)});
      }
    }
    auto right_filter = direct_right_filter;
    if (not plain_keys.empty()) {
      auto make_equiv_subst
        = [&](bool left_to_right) -> ast::FieldSubstitution {
        return [&, left_to_right](
                 ast::field_path const& fp) -> Option<ast::expression> {
          for (auto const& pk : plain_keys) {
            auto const& src = left_to_right ? pk.left : pk.right;
            auto const& dst = left_to_right ? pk.right : pk.left;
            if (ir::is_field_path_prefix(src, fp)) {
              return replace_field_path_prefix(fp, src.path().size(), dst);
            }
          }
          return None{};
        };
      };
      auto seen_right = std::set<caf::byte_buffer>{};
      for (auto const& expr : right_filter) {
        seen_right.insert(serialize_expression(expr));
      }
      auto const l_to_r = make_equiv_subst(true);
      // Left -> right propagation is sound for all join kinds: every surviving
      // left row satisfies `p`, so any right row that violates the equivalent
      // predicate can never match a surviving left row.
      for (auto const& expr : left_filter) {
        if (not ir::is_infallible(expr)
            or not is_equal_congruent_predicate(expr)) {
          continue;
        }
        if (auto implied
            = ast::substitute_refs(expr, octx.reg, l_to_r, no_meta);
            implied
            and ir::expression_size(*implied)
                  <= ir::max_rewritten_predicate_size) {
          *implied = ir::simplify_predicate(std::move(*implied), octx.reg);
          if (ir::is_infallible(*implied)
              and seen_right.insert(serialize_expression(*implied)).second) {
            right_filter.push_back(std::move(*implied));
          }
        }
      }
      // Right -> left propagation is sound for `inner` joins only.
      if (kind == JoinKind::inner and not direct_right_filter.empty()) {
        auto seen_left = std::set<caf::byte_buffer>{};
        for (auto const& expr : left_filter) {
          seen_left.insert(serialize_expression(expr));
        }
        auto const r_to_l = make_equiv_subst(false);
        for (auto const& expr : direct_right_filter) {
          if (not ir::is_infallible(expr)
              or not is_equal_congruent_predicate(expr)) {
            continue;
          }
          if (auto implied
              = ast::substitute_refs(expr, octx.reg, r_to_l, no_meta);
              implied
              and ir::expression_size(*implied)
                    <= ir::max_rewritten_predicate_size) {
            *implied = ir::simplify_predicate(std::move(*implied), octx.reg);
            if (ir::is_infallible(*implied)
                and seen_left.insert(serialize_expression(*implied)).second) {
              left_filter.push_back(std::move(*implied));
            }
          }
        }
      }
    }
    left_filter = ir::simplify_filter(std::move(left_filter), octx.reg);
    right_filter = ir::simplify_filter(std::move(right_filter), octx.reg);

    // 3. Projection pushdown into both legs.
    auto out_proj = std::move(req.projection);
    for (auto const& expr : kept_filter) {
      ir::add_refs_to_projection(out_proj, expr);
    }
    auto left_proj = Option<ir::OptimizeProjection>{};
    auto right_proj = Option<ir::OptimizeProjection>{};
    if (kind == JoinKind::semi or kind == JoinKind::anti) {
      left_proj = out_proj;
      right_proj = ir::OptimizeProjection{};
    } else if (into) {
      left_proj = out_proj;
      ir::project_assignment(left_proj, *into);
      right_proj = extract_sub_projection(out_proj, into_segments);
    } else {
      left_proj = extract_sub_projection(out_proj, left_prefix);
      right_proj = extract_sub_projection(out_proj, right_prefix);
    }
    for (auto const& kp : key_pairs) {
      ir::add_refs_to_projection(left_proj, kp.left);
      ir::add_refs_to_projection(right_proj, kp.right);
    }
    for (auto const& expr : left_filter) {
      ir::add_refs_to_projection(left_proj, expr);
    }
    for (auto const& expr : right_filter) {
      ir::add_refs_to_projection(right_proj, expr);
    }

    // 4. Order and limit propagation.
    auto const left_order = req.order;
    auto const right_order = (kind == JoinKind::semi or kind == JoinKind::anti)
                               ? EventOrder::unordered
                               : req.order;
    auto const left_limit = (kind == JoinKind::left and kept_filter.empty())
                              ? req.limit
                              : Option<uint64_t>{};

    // 5. Optimize the right-hand source subpipeline.
    auto right_octx = ir::OptimizeCtx{
      .reg = octx.reg,
      .can_any_op_reorder = false,
    };
    auto right_opt = std::move(pipe->inner)
                       .optimize(
                         ir::OptimizeRequest{
                           .filter = std::move(right_filter),
                           .order = right_order,
                           .limit = None{},
                           .projection = std::move(right_proj),
                         },
                         right_octx);
    pipe->inner = std::move(right_opt.replacement);
    pipe->inner.prepend(std::move(right_opt.filter), octx.reg);

    // 6. Reassemble the join and any filters kept behind it.
    auto replacement = std::vector<Box<ir::Operator>>{};
    replacement.push_back(std::move(*this).move());
    for (auto& expr : kept_filter) {
      replacement.push_back(make_where_ir(std::move(expr), octx.reg));
    }
    return {
      .filter = std::move(left_filter),
      .order = left_order,
      .replacement = ir::pipeline{{}, std::move(replacement)},
      .limit = left_limit,
      .projection = std::move(left_proj),
    };
  }

  auto spawn(element_type_tag input, registry const&) const
    -> AnyOperator override {
    TENZIR_ASSERT(input.is<nova::Events>());
    auto pipe = args_.pipe();
    TENZIR_ASSERT(pipe);
    return Box<tenzir::Operator<nova::Events, nova::Events>>{
      Join{args_.get(), pipe->inner}.with_name("join")};
  }

  friend auto inspect(auto& f, JoinIr& x) -> bool {
    return f.apply(x.args_);
  }

private:
  JoinArguments args_;
};

class Plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "join_operator";
  }

  auto operator_name() const -> std::string override {
    return "join";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    TRY(auto args, JoinArguments::parse(std::move(inv), ctx));
    return JoinIr{std::move(args)};
  }
};

using join_ir_plugin = inspection_plugin<ir::Operator, JoinIr>;

} // namespace

} // namespace tenzir::plugins::join

TENZIR_REGISTER_PLUGIN(tenzir::plugins::join::Plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::join::join_ir_plugin)
