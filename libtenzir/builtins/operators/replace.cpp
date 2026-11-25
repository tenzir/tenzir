//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/argument_parser2.hpp"
#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/async.hpp"
#include "tenzir/collect.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/nova/array_merge.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/const_eval.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/instantiate_ctx.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/series.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/registry.hpp"

#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/buffer.h>
#include <arrow/type_fwd.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/bitmap_ops.h>

#include <algorithm>
#include <cmath>
#include <ranges>
#include <span>

namespace tenzir::plugins::replace {

namespace {

auto comparable(const type& x, const type& y) -> bool {
  return match(std::tie(x, y), []<typename X, typename Y>(const X&, const Y&) {
    return std::same_as<X, Y> or concepts::one_of<null_type, X, Y>
           or (concepts::number<type_to_data_t<X>>
               and concepts::number<type_to_data_t<Y>>);
  });
}

auto equals(const data_view3& l, const data& r) -> bool {
  return match(
    std::tie(l, r),
    [](const concepts::integer auto& x, const concepts::integer auto& y) {
      return std::cmp_equal(x, y);
    },
    [](const concepts::number auto& x, const concepts::number auto& y) {
      return x == y;
    },
    [&](const auto&, const auto&) {
      return l == r;
    });
}

auto check_name(const std::vector<ast::field_path>& paths,
                const std::string_view name, const size_t idx) -> bool {
  return std::ranges::any_of(paths, [&](auto& path) {
    return name == path.path()[idx].id.name;
  });
}

auto update_paths(const std::vector<ast::field_path>& paths,
                  const std::string_view name, const size_t idx)
  -> std::vector<ast::field_path> {
  return std::views::filter(paths,
                            [&](auto& path) {
                              return name == path.path()[idx].id.name;
                            })
         | std::ranges::to<std::vector<ast::field_path>>();
}

auto find_splits(const series& in, std::set<int64_t>& indices,
                 const std::vector<ast::field_path>& paths, const size_t idx,
                 const type& what_type, const data& what) -> void {
  if (comparable(in.type, what_type)) {
    auto replace = false;
    for (const auto& [curr, val] :
         detail::enumerate<int64_t>(values3(*in.array))) {
      if (equals(val, what) != replace) {
        indices.insert(curr);
        replace = not replace;
      }
    }
  }
  const auto replace_all
    = paths.empty() or std::ranges::any_of(paths, [&](auto& path) {
        return idx >= path.path().size();
      });
  if (const auto rin = in.as<record_type>()) {
    for (const auto& field : rin->fields()) {
      if (replace_all or check_name(paths, field.name, idx)) {
        const auto& new_paths
          = replace_all ? paths : update_paths(paths, field.name, idx);
        find_splits(field.data, indices, new_paths, idx + 1, what_type, what);
      }
    }
  }
}

auto replace_split_series(series in, const std::vector<ast::field_path>& paths,
                          const std::size_t idx, const type& what_type,
                          const data& what, const data& with) -> series {
  if (comparable(in.type, what_type)) {
    const auto val = view_at(*in.array, 0);
    if (equals(val, what)) {
      return data_to_series(with, in.length());
    }
  }
  const auto replace_all
    = paths.empty() or std::ranges::any_of(paths, [&](auto& path) {
        return idx >= path.path().size();
      });
  if (const auto rin = in.as<record_type>()) {
    auto fields = collect(rin->fields());
    for (auto& field : fields) {
      if (replace_all) {
        field.data = replace_split_series(field.data, paths, idx + 1, what_type,
                                          what, with);
        continue;
      }
      if (check_name(paths, field.name, idx)) {
        field.data = replace_split_series(field.data,
                                          update_paths(paths, field.name, idx),
                                          idx + 1, what_type, what, with);
      }
    }
    return make_record_series(fields, *rin->array);
  }
  return in;
}

auto replace_series(const basic_series<record_type>& input,
                    const std::vector<ast::field_path>& paths,
                    const type& what_type, const data& what, const data& with)
  -> std::vector<basic_series<record_type>> {
  auto fields = collect(input.fields());
  if (input.length() == 0 or fields.empty()) {
    return {input};
  }
  auto split_indices = std::set<int64_t>{0, input.length()};
  const auto replace_all = paths.empty();
  for (const auto& field : fields) {
    if (replace_all or check_name(paths, field.name, 0)) {
      const auto new_paths = update_paths(paths, field.name, 0);
      find_splits(field.data, split_indices, new_paths, 1, what_type, what);
    }
  }
  auto splits
    = std::vector<std::vector<series_field>>{split_indices.size() - 1};
  for (auto&& field : fields) {
    auto it = split_indices.begin();
    for (auto i = size_t{}; i != splits.size(); ++i) {
      const auto begin = *it;
      const auto end = *(++it);
      auto split = [&] {
        if (replace_all) {
          return replace_split_series(field.data.slice(begin, end), paths, 1,
                                      what_type, what, with);
        }
        if (check_name(paths, field.name, 0)) {
          const auto new_paths = update_paths(paths, field.name, 0);
          return replace_split_series(field.data.slice(begin, end), new_paths,
                                      1, what_type, what, with);
        }
        return field.data.slice(begin, end);
      }();
      splits[i].emplace_back(field.name, std::move(split));
    }
  }
  auto replaced = std::vector<basic_series<record_type>>{};
  auto offset = int64_t{};
  for (auto&& split : splits) {
    auto input_slice = std::static_pointer_cast<arrow::StructArray>(
      input.array->Slice(offset, split[0].data.length()));
    replaced.emplace_back(make_record_series(split, *input_slice));
    offset += split[0].data.length();
  }
  return replaced;
}

auto replace_series_with_null(const basic_series<record_type>& input,
                              const std::vector<ast::field_path>& paths,
                              const size_t idx, const type& what_type,
                              const data& what) -> basic_series<record_type> {
  auto fields = collect(input.fields());
  const auto replace_all
    = paths.empty() or std::ranges::any_of(paths, [&](auto& path) {
        return idx >= path.path().size();
      });
  for (auto&& field : fields) {
    if (not replace_all and not check_name(paths, field.name, idx)) {
      continue;
    }
    if (not comparable(field.data.type, what_type)) {
      if (const auto rin = field.data.as<record_type>()) {
        const auto& new_paths
          = replace_all ? paths : update_paths(paths, field.name, idx);
        auto replaced
          = replace_series_with_null(*rin, new_paths, idx + 1, what_type, what);
        field.data.array = std::move(replaced.array);
      }
      continue;
    }
    auto& array = field.data.array;
    const auto& array_data = array->data();
    const auto offset = array_data->offset;
    const auto total_count = array_data->length + offset;
    auto null_bitmap = std::shared_ptr<arrow::Buffer>{};
    if (array->null_bitmap_data()) {
      // Copy the existing bitmap to ensure mutability.
      // Copy from position 0 to total_count to preserve offset-based indexing.
      null_bitmap = check(arrow::internal::CopyBitmap(
        arrow_memory_pool(), array->null_bitmap_data(), 0, total_count));
    } else {
      // No existing bitmap - allocate a fresh one and set all bits to valid.
      null_bitmap = check(arrow::AllocateEmptyBitmap(total_count));
      arrow::bit_util::SetBitsTo(null_bitmap->mutable_data(), 0, total_count,
                                 true);
    }
    TENZIR_ASSERT(null_bitmap->size()
                  >= arrow::bit_util::BytesForBits(total_count));
    auto* null_bitmap_data = null_bitmap->mutable_data();
    TENZIR_ASSERT(null_bitmap_data);
    auto null_count = int64_t{};
    for (const auto& [i, val] :
         detail::enumerate<int64_t>(field.data.values())) {
      if (is<caf::none_t>(val) or equals(val, what)) {
        arrow::bit_util::SetBitTo(null_bitmap_data, i + offset, false);
        ++null_count;
      }
    }
    auto buffers = array_data->buffers;
    if (buffers.empty()) {
      buffers.emplace_back(null_bitmap);
    } else {
      buffers.front() = null_bitmap;
    }
    auto new_array_data
      = arrow::ArrayData::Make(array_data->type, array_data->length, buffers,
                               null_count, array_data->offset);
    array = arrow::MakeArray(new_array_data);
  }
  return make_record_series(fields, *input.array);
}

struct replace_args {
  std::vector<ast::field_path> path;
  located<data> what;
  located<data> with;

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    const auto what_type = type::infer(what.inner);
    const auto with_type = type::infer(with.inner);
    if (not what_type) {
      diagnostic::error("failed to infer type of `what`").primary(what).emit(dh);
      return failure::promise();
    }
    if (not with_type) {
      diagnostic::error("failed to infer type of `with`").primary(with).emit(dh);
      return failure::promise();
    }
    return {};
  }

  friend auto inspect(auto& f, replace_args& x) -> bool {
    return f.object(x).fields(f.field("path", x.path), f.field("what", x.what),
                              f.field("with", x.with));
  }
};

struct ReplaceArgs {
  std::vector<ast::field_path> path;
  located<data> what;
  located<data> with;
};

class Replace final : public Operator<table_slice, table_slice> {
public:
  explicit Replace(ReplaceArgs args)
    : args_{std::move(args)},
      what_type_{type::infer(args_.what.inner).value()},
      replace_with_null_{
        type::infer(args_.with.inner).value().kind().is<null_type>()} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& /*ctx*/)
    -> Task<void> override {
    auto s = basic_series<record_type>{input};
    if (replace_with_null_) {
      auto rs = replace_series_with_null(s, args_.path, 0, what_type_,
                                         args_.what.inner);
      co_await push(table_slice{
        record_batch_from_struct_array(input.schema().to_arrow_schema(),
                                       *rs.array),
        input.schema(),
      });
      co_return;
    }
    auto rs = replace_series(s, args_.path, what_type_, args_.what.inner,
                             args_.with.inner);
    auto attrs = collect(input.schema().attributes());
    for (auto& r : rs) {
      auto rty = type{input.schema().name(), r.type, auto{attrs}};
      co_await push(table_slice{
        record_batch_from_struct_array(rty.to_arrow_schema(), *r.array),
        std::move(rty),
      });
    }
  }

private:
  ReplaceArgs args_;
  type what_type_;
  bool replace_with_null_ = false;
};

class ReplaceNova final : public Operator<nova::Events, nova::Events> {
public:
  explicit ReplaceNova(ReplaceArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto what = co_await nova::Evaluator::make(
      ast::expression{ast::constant::make(std::move(args_.what))}, ctx);
    if (not what) {
      co_return;
    }
    what_.emplace(std::move(*what));
    auto with = co_await nova::Evaluator::make(
      ast::expression{ast::constant::make(std::move(args_.with))}, ctx);
    if (not with) {
      co_return;
    }
    with_.emplace(std::move(*with));
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not what_ or not with_) {
      co_return;
    }
    auto what = what_->eval(input, nova::EvalCtx{ctx.dh()});
    auto with = with_->eval(input, nova::EvalCtx{ctx.dh()});
    input.data = replace_record(std::move(input.data), input.mask, args_.path,
                                0, what, with);
    co_await push(std::move(input));
  }

private:
  auto replace_data(nova::Array<nova::Data> input,
                    const nova::storage::BitMap& active,
                    const std::vector<ast::field_path>& paths, size_t index,
                    const nova::Array<nova::Data>& what,
                    const nova::Array<nova::Data>& with) const
    -> nova::Array<nova::Data> {
    auto matches = nova::storage::BitMap::Mutable{input.length()};
    nova::storage::for_each_true(active, [&](auto row) {
      matches.set(row, nova::equal(input.get(row), what.get(row)));
    });
    auto matched = std::move(matches).finish();
    auto nested = active.and_not(matched);
    if (nested.any()) {
      input = std::move(input).map_alternative<nova::Record>(
        [&](nova::MaskedArray<nova::Array<nova::Record>> records) {
          auto record_rows = nested & records.present;
          return replace_record(std::move(records.data), record_rows, paths,
                                index, what, with);
        });
    }
    if (not matched.any()) {
      return input;
    }
    return nova::with_merged(
      {input, nova::storage::BitMap{input.length(), true}}, {with, matched});
  }

  auto replace_record(nova::Array<nova::Record> input,
                      const nova::storage::BitMap& active,
                      const std::vector<ast::field_path>& paths, size_t index,
                      const nova::Array<nova::Data>& what,
                      const nova::Array<nova::Data>& with) const
    -> nova::Array<nova::Record> {
    const auto replace_all
      = paths.empty() or std::ranges::any_of(paths, [&](const auto& path) {
          return index >= path.path().size();
        });
    auto names = std::vector<std::string>{};
    if (replace_all) {
      nova::storage::for_each_true(active, [&](auto row) {
        for (auto [name, unused] : input.get(row)) {
          TENZIR_UNUSED(unused);
          if (not std::ranges::contains(names, name)) {
            names.emplace_back(name);
          }
        }
      });
    } else {
      for (const auto& path : paths) {
        auto segments = path.path();
        if (index < segments.size()
            and not std::ranges::contains(names, segments[index].id.name)) {
          names.push_back(segments[index].id.name);
        }
      }
    }
    for (const auto& name : names) {
      auto field = std::move(input).dangerously_extract_field(name);
      if (not field) {
        continue;
      }
      auto selected = active & field->present;
      auto child_paths = replace_all ? paths : update_paths(paths, name, index);
      field->data = replace_data(std::move(field->data), selected, child_paths,
                                 index + 1, what, with);
      input = std::move(input).with_field_overwrite(name, std::move(*field));
    }
    return input;
  }

  ReplaceArgs args_;
  Option<nova::Evaluator> what_;
  Option<nova::Evaluator> with_;
};

/// Returns whether `value` is an evaluable scalar (including `null`, excluding
/// `NaN`, records, lists, and non-evaluable constants such as `secret`).
auto is_scalar(auto const& value) -> bool {
  return match(
    value,
    [](double x) {
      return not std::isnan(x);
    },
    []<class T>(T const&) {
      return concepts::one_of<T, caf::none_t, bool, int64_t, uint64_t, duration,
                              time, std::string, ip, subnet, blob>;
    });
}

auto is_scalar_constant(ast::expression const& expr) -> bool {
  auto const* constant = try_as<ast::constant>(expr);
  return constant and is_scalar(constant->value);
}

auto is_scalar_constant_list(ast::expression const& expr) -> bool {
  if (auto const* constant = try_as<ast::constant>(expr)) {
    auto const* items = try_as<list>(constant->value);
    return items and std::ranges::all_of(*items, [](data const& item) {
             return is_scalar(item);
           });
  }
  auto const* items = try_as<ast::list>(expr);
  return items
         and std::ranges::all_of(items->items, [](ast::list::item const& item) {
               auto const* elem = try_as<ast::expression>(item);
               return elem and is_scalar_constant(*elem);
             });
}

/// Returns the top-level root segment of a non-empty target path. `replace`
/// checks every ancestor along a target path and can replace it (including
/// turning a scalar ancestor into a record), so conservative dependency
/// tracking and projection must cover the top-level root.
auto root_field_path(ast::field_path const& path) -> ast::field_path {
  TENZIR_ASSERT(not path.path().empty());
  auto const& root = path.path().front();
  auto segment = ast::field_path::segment{root.id, false};
  return ast::field_path::make(ast::root_field{root.id, false}, false,
                               {segment});
}

auto overlaps_any_target(std::span<ast::field_path const> targets,
                         bool replace_all, ast::field_path const& path)
  -> bool {
  if (replace_all) {
    return true;
  }
  return std::ranges::any_of(targets, [&](ast::field_path const& target) {
    return ir::is_field_path_prefix(target, path)
           or ir::is_field_path_prefix(path, target);
  });
}

auto eval_bool_constant(ast::expression const& expr, registry const& reg)
  -> Option<bool> {
  auto dh = collecting_diagnostic_handler{};
  auto value = nova::const_eval(expr, nova::InstantiateCtx{dh, reg});
  if (not value or not dh.empty()) {
    return None{};
  }
  if (auto const* result = try_as<nova::Bool>(&*value)) {
    return *result;
  }
  return None{};
}

/// Rewrites an atom `P(p)` (`p == k`, `p != k`, `k == p`, `k != p`, or
/// `p in [k1, ...]`, with non-empty static `field_path` `p` and scalar
/// constant(s) `k`) for an eligible scalar `replace` (`what` non-null scalar,
/// `with` scalar or `null`).
///
/// For any `p` overlapping a target (including ancestors of a nested target,
/// which `replace` also checks and replaces when non-record), evaluating
/// `P(what)` and `P(with)` gives:
/// - `P(p)` unchanged when `P(what) == P(with)`,
/// - `P(with) if p == what else P(p)` otherwise.
///
/// Because `k` is a scalar (never a record or list), record values compare
/// identically before and after `replace`, lists are not descended into,
/// absent and `null` values stay unchanged (`what != null`), cross-type
/// numeric equality matches `nova::const_eval`, and `secret` values (where
/// `p == what` yields `null`) fall through to `P(p)`.
auto rewrite_atom(ast::binary_expr const& expr,
                  std::span<ast::field_path const> targets, bool replace_all,
                  data const& what, data const& with, registry const& reg)
  -> Option<ast::expression> {
  auto path = Option<ast::field_path>{};
  auto field_on_left = true;
  if (expr.op == ast::binary_op::eq or expr.op == ast::binary_op::neq) {
    if (is_scalar_constant(expr.right)) {
      path = ast::field_path::try_from(expr.left);
      field_on_left = true;
    } else if (is_scalar_constant(expr.left)) {
      path = ast::field_path::try_from(expr.right);
      field_on_left = false;
    } else {
      return None{};
    }
  } else if (expr.op == ast::binary_op::in) {
    if (not is_scalar_constant_list(expr.right)) {
      return None{};
    }
    path = ast::field_path::try_from(expr.left);
    field_on_left = true;
  } else {
    return None{};
  }
  if (not path or path->path().empty()) {
    return None{};
  }
  if (not overlaps_any_target(targets, replace_all, *path)) {
    return ast::expression{expr};
  }
  auto const source = path->get_location();
  auto const make_probe = [&](data const& value) -> ast::expression {
    auto constant
      = ast::expression{ast::constant::make(located{value, source})};
    if (field_on_left) {
      return ast::binary_expr{std::move(constant), expr.op, expr.right};
    }
    return ast::binary_expr{expr.left, expr.op, std::move(constant)};
  };
  TRY(auto at_what, eval_bool_constant(make_probe(what), reg));
  TRY(auto at_with, eval_bool_constant(make_probe(with), reg));
  if (at_what == at_with) {
    return ast::expression{expr};
  }
  auto what_const = ast::expression{ast::constant::make(located{what, source})};
  auto cond = ast::expression{
    ast::binary_expr{path->inner(), ast::binary_op::eq, std::move(what_const)}};
  auto guarded = ast::expression{ast::binary_expr{
    ast::constant{at_with, source}, ast::binary_op::if_, std::move(cond)}};
  return ast::expression{
    ast::binary_expr{std::move(guarded), ast::binary_op::else_, expr}};
}

/// Rewrites an affected predicate if it belongs to the positive grammar:
/// boolean/null constants, `and`, `or`, `not`, `if/else` (so chained replaces
/// compose), and scalar comparison atoms (`rewrite_atom`).
auto rewrite_predicate(ast::expression const& expr,
                       std::span<ast::field_path const> targets,
                       bool replace_all, data const& what, data const& with,
                       registry const& reg) -> Option<ast::expression> {
  return expr.match(
    [&](ast::constant const& x) -> Option<ast::expression> {
      if (is<bool>(x.value) or is<caf::none_t>(x.value)) {
        return expr;
      }
      return None{};
    },
    [&](ast::unary_expr const& x) -> Option<ast::expression> {
      if (x.op != ast::unary_op::not_) {
        return None{};
      }
      TRY(auto inner,
          rewrite_predicate(x.expr, targets, replace_all, what, with, reg));
      return ast::expression{
        ast::unary_expr{located{x.op, x.location}, std::move(inner)}};
    },
    [&](ast::binary_expr const& x) -> Option<ast::expression> {
      switch (x.op) {
        case ast::binary_op::and_:
        case ast::binary_op::or_: {
          TRY(auto left,
              rewrite_predicate(x.left, targets, replace_all, what, with, reg));
          TRY(auto right, rewrite_predicate(x.right, targets, replace_all, what,
                                            with, reg));
          return ast::expression{
            ast::binary_expr{std::move(left), x.op, std::move(right)}};
        }
        case ast::binary_op::else_: {
          auto const* cond = try_as<ast::binary_expr>(x.left);
          if (not cond or cond->op != ast::binary_op::if_) {
            return None{};
          }
          TRY(auto then_branch,
              rewrite_predicate(cond->left, targets, replace_all, what, with,
                                reg));
          TRY(auto cond_branch,
              rewrite_predicate(cond->right, targets, replace_all, what, with,
                                reg));
          TRY(auto else_branch, rewrite_predicate(x.right, targets, replace_all,
                                                  what, with, reg));
          auto guarded = ast::expression{
            ast::binary_expr{std::move(then_branch), ast::binary_op::if_,
                             std::move(cond_branch)}};
          return ast::expression{ast::binary_expr{
            std::move(guarded), ast::binary_op::else_, std::move(else_branch)}};
        }
        case ast::binary_op::eq:
        case ast::binary_op::neq:
        case ast::binary_op::in:
          return rewrite_atom(x, targets, replace_all, what, with, reg);
        case ast::binary_op::add:
        case ast::binary_op::sub:
        case ast::binary_op::mul:
        case ast::binary_op::div:
        case ast::binary_op::gt:
        case ast::binary_op::geq:
        case ast::binary_op::lt:
        case ast::binary_op::leq:
        case ast::binary_op::if_:
          return None{};
      }
      TENZIR_UNREACHABLE();
    },
    [](auto const&) -> Option<ast::expression> {
      return None{};
    });
}

struct replace : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "replace";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReplaceArgs, Replace, ReplaceNova>{};
    d.parallelizable();
    auto what = d.named("what", &ReplaceArgs::what);
    auto with = d.named("with", &ReplaceArgs::with);
    auto fields = d.optional_variadic("fields", &ReplaceArgs::path, "field");
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto what_value, ctx.get(what));
      TRY(auto with_value, ctx.get(with));
      if (not type::infer(what_value.inner)) {
        diagnostic::error("failed to infer type of `what`")
          .primary(what_value.source)
          .emit(ctx);
      }
      if (not type::infer(with_value.inner)) {
        diagnostic::error("failed to infer type of `with`")
          .primary(with_value.source)
          .emit(ctx);
      }
      return {};
    });
    // `replace` rewrites values in place: it neither drops, creates, nor
    // reorders events, and fields keep their identity. Target roots stay
    // projected because `replace` checks every ancestor along each target path.
    // For scalar non-null `what` and scalar/null `with`, affected predicates in
    // the scalar-comparison grammar rewrite and fold in place; unaffected
    // predicates pass unchanged; everything else stays behind `replace`.
    return d.optimize(
      [=](DescribeCtx& ctx, ir::OptimizeRequest req) -> Optimization {
        auto what_value = ctx.get(what);
        auto with_value = ctx.get(with);
        TENZIR_ASSERT(what_value);
        TENZIR_ASSERT(with_value);
        auto targets = std::vector<ast::field_path>{};
        for (auto& field : ctx.get_all(fields)) {
          TENZIR_ASSERT(field);
          targets.push_back(std::move(*field));
        }
        auto const replace_all
          = targets.empty()
            or std::ranges::any_of(targets, [](ast::field_path const& path) {
                 return path.path().empty();
               });
        auto roots = std::vector<ast::field_path>{};
        auto projection = std::move(req.projection);
        if (replace_all) {
          projection = None{};
        } else {
          for (auto const& target : targets) {
            auto root = root_field_path(target);
            ir::add_to_projection(projection, root);
            roots.push_back(std::move(root));
          }
        }
        auto const touched = ast::ExprRefs{.field_paths = std::move(roots)};
        auto const eligible = is_scalar(what_value->inner)
                              and not is<caf::none_t>(what_value->inner)
                              and is_scalar(with_value->inner);
        auto split = ir::split_filter_result{};
        if (not eligible) {
          if (replace_all) {
            split.dependent = std::move(req.filter);
          } else {
            split
              = ir::split_filter_by_dependents(std::move(req.filter), touched);
          }
        } else {
          split = ir::split_filter_by_rewrite(
            std::move(req.filter),
            [&](ast::expression const& expr) -> Option<ast::expression> {
              auto refs = ast::collect_refs(expr);
              if (not refs) {
                return None{};
              }
              if (replace_all ? refs->field_paths.empty()
                              : not refs->overlaps(touched)) {
                return expr;
              }
              return rewrite_predicate(expr, targets, replace_all,
                                       what_value->inner, with_value->inner,
                                       ctx.reg());
            });
        }
        // A carried limit counts events after the whole filter chain. If we
        // keep predicates behind us, upstream can no longer honor it.
        auto limit = split.dependent.empty() ? req.limit : Option<uint64_t>{};
        return {
          .order = req.order,
          .filter_upstream = std::move(split.independent),
          .filter_self = std::move(split.dependent),
          .limit_upstream = limit,
          .projection_upstream = std::move(projection),
        };
      });
  }
};

} // namespace

} // namespace tenzir::plugins::replace

TENZIR_REGISTER_PLUGIN(tenzir::plugins::replace::replace)
