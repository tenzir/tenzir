//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/builtins/join/common.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval_util.hpp>
#include <tenzir/nova/materialize.hpp>
#include <tenzir/nova/slice.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/registry.hpp>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <array>
#include <cmath>

namespace tenzir::plugins::join {

namespace {

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
  return name == "kind" or name == "max_memory" or name == "refresh"
         or name == "before" or name == "after" or name == "within"
         or name == "tolerance" or name == "idle_timeout" or name == "pick";
}

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

enum class KeyStatus : uint8_t {
  eligible,
  ineligible,
  has_secret,
};

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

auto classify_top_level_value(nova::RowView<nova::Data> const& value)
  -> KeyStatus {
  if (is<nova::RowView<nova::Null>>(value)) {
    return KeyStatus::ineligible;
  }
  return classify_nested_value(value);
}

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

} // namespace

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

auto parse_pick(std::string_view value) -> Option<PickMode> {
  if (value == "all") {
    return PickMode::all;
  }
  if (value == "latest") {
    return PickMode::latest;
  }
  if (value == "earliest") {
    return PickMode::earliest;
  }
  if (value == "nearest") {
    return PickMode::nearest;
  }
  return None{};
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

auto classify_timestamp_expr(ast::expression expr, registry const& reg,
                             diagnostic_handler& dh)
  -> failure_or<JoinKeyPair> {
  auto const loc = expr.get_location();
  if (is<ast::list>(*expr.kind)) {
    diagnostic::error("`on` must be a timestamp expression, not a list")
      .primary(loc)
      .hint("write `on=ts` or `on=left_ts == right_ts`")
      .emit(dh);
    return failure::promise();
  }
  if (auto const* un = try_as<ast::unary_expr>(*expr.kind);
      un and un->op == ast::unary_op::not_) {
    diagnostic::error("`on` must be a timestamp expression, not a boolean "
                      "expression")
      .primary(loc)
      .emit(dh);
    return failure::promise();
  }
  if (auto* bin = try_as<ast::binary_expr>(*expr.kind)) {
    switch (bin->op) {
      case ast::binary_op::and_:
      case ast::binary_op::or_:
      case ast::binary_op::neq:
      case ast::binary_op::gt:
      case ast::binary_op::geq:
      case ast::binary_op::lt:
      case ast::binary_op::leq:
      case ast::binary_op::in:
        diagnostic::error("only `==` is supported in `on` timestamp mapping")
          .primary(loc)
          .hint("write `on=ts` or `on=left_ts == right_ts`")
          .emit(dh);
        return failure::promise();
      case ast::binary_op::if_:
      case ast::binary_op::else_:
        if (auto op_str = ambiguous_eq_conditional_op(*bin)) {
          diagnostic::error("ambiguous `==` inside `{}` in `on`", *op_str)
            .primary(loc)
            .hint("wrap the conditional operand in parentheses")
            .emit(dh);
          return failure::promise();
        }
        break;
      case ast::binary_op::eq: {
        if (is_comparison_or_logical_expr(bin->left)
            or is_comparison_or_logical_expr(bin->right)) {
          diagnostic::error("chained comparisons are not supported in `on`")
            .primary(loc)
            .emit(dh);
          return failure::promise();
        }
        if (is_pure_constant(bin->left, reg)
            or is_pure_constant(bin->right, reg)) {
          diagnostic::error("`on` timestamp expression must reference an event "
                            "field on each side")
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
    diagnostic::error("`on` timestamp expression must reference an event field")
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

auto materialize_key(JoinKeyView const& views) -> JoinKey {
  auto result = JoinKey{};
  result.reserve(views.size());
  for (auto const& view : views) {
    result.push_back(nova::materialize_data(view));
  }
  return result;
}

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

auto warn_type_mismatch_if_disjoint(SideKeyState const& left_state,
                                    SideKeyState const& right_state,
                                    diagnostic_handler& dh) -> void {
  TENZIR_ASSERT_EQ(left_state.observed_types.size(),
                   right_state.observed_types.size());
  for (auto i = size_t{0}; i < left_state.observed_types.size(); ++i) {
    auto const lt = left_state.observed_types[i];
    auto const rt = right_state.observed_types[i];
    if (lt != 0u and rt != 0u and (lt & rt) == 0u) {
      auto const [left_names, left_plural] = format_type_mask(lt);
      auto const [right_names, right_plural] = format_type_mask(rt);
      auto diag = diagnostic::warning(
        "`join` matched no events due to incompatible key types");
      if (left_state.locations[i] != right_state.locations[i]) {
        diag = std::move(diag)
                 .primary(left_state.locations[i], "input key has {} {}",
                          left_plural ? "types" : "type", left_names)
                 .secondary(right_state.locations[i], "source key has {} {}",
                            right_plural ? "types" : "type", right_names);
      } else {
        diag = std::move(diag)
                 .primary(left_state.locations[i])
                 .note("input key has {} {}, source key has {} {}",
                       left_plural ? "types" : "type", left_names,
                       right_plural ? "types" : "type", right_names);
      }
      std::move(diag).emit(dh);
      return;
    }
  }
}

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

BuildIndex::BuildIndex(SideKeyState key_state, bool store_payloads,
                       uint64_t max_memory, location memory_loc,
                       bool warn_on_limit, uint64_t reserved_bytes)
  : key_state_{std::move(key_state)},
    store_payloads_{store_payloads},
    warn_on_limit_{warn_on_limit},
    max_memory_{max_memory},
    reserved_bytes_{reserved_bytes},
    memory_loc_{memory_loc} {
}

auto BuildIndex::add(nova::Events events, diagnostic_handler& dh) -> void {
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
    auto const eligible_count = eval_result.eligible_mask.true_count();
    if (static_cast<int64_t>(eligible_count) * 2
        < static_cast<int64_t>(events.length())) {
      events.mask = eval_result.eligible_mask;
      events = nova::physically_reduce_to_mask(std::move(events));
      compacted = true;
    }
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

auto BuildIndex::clear() -> void {
  batches_ = {};
  map_ = {};
  postings_ = {};
  batches_bytes_ = 0;
  keys_bytes_ = 0;
  postings_bytes_ = 0;
}

auto BuildIndex::approx_bytes() const noexcept -> uint64_t {
  auto const table_bytes = static_cast<uint64_t>(map_.bucket_count())
                           * (sizeof(JoinKey) + sizeof(size_t) + 8);
  auto const container_bytes = detail::saturating_add(
    static_cast<uint64_t>(batches_.capacity())
      * sizeof(nova::Array<nova::Record>),
    static_cast<uint64_t>(postings_.capacity()) * sizeof(std::vector<RowRef>));
  return detail::saturating_add(
    reserved_bytes_,
    detail::saturating_add(
      detail::saturating_add(batches_bytes_, keys_bytes_),
      detail::saturating_add(postings_bytes_,
                             detail::saturating_add(table_bytes,
                                                    container_bytes))));
}

auto BuildIndex::fail_memory_limit(diagnostic_handler& dh) -> void {
  memory_exceeded_ = true;
  clear();
  if (warn_on_limit_) {
    diagnostic::warning("`join` refresh exceeded memory limit of {}",
                        format_bytes(max_memory_))
      .primary(memory_loc_)
      .note("keeping the previous snapshot")
      .hint("increase `max_memory` or filter the right-hand pipeline")
      .emit(dh);
    return;
  }
  diagnostic::error("`join` exceeded memory limit of {}",
                    format_bytes(max_memory_))
    .primary(memory_loc_)
    .hint("increase `max_memory` or filter the right-hand pipeline")
    .emit(dh);
}

} // namespace tenzir::plugins::join
