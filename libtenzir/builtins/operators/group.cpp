//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/async.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/view3.hpp>

#include <unordered_map>

namespace tenzir::plugins::group {

namespace {

struct GroupArgs {
  ast::expression over;
  located<ir::pipeline> pipe;
  let_id let;
};

class Group final : public Operator<table_slice, table_slice> {
public:
  explicit Group(GroupArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    auto keys = eval(args_.over, input, ctx);
    auto rows = input.rows();
    // Build an ordered list of unique group keys and a boolean row-mask per
    // group. We only materialize each key once on first encounter.
    auto group_order = std::vector<data>{};
    auto group_masks
      = std::unordered_map<data, std::vector<bool>>{};
    for (auto row = int64_t{0}; row < keys.length(); ++row) {
      auto key = materialize(keys.view3_at(row));
      auto [it, inserted]
        = group_masks.try_emplace(key, std::vector<bool>(rows, false));
      if (inserted) {
        group_order.push_back(key);
      }
      it->second[static_cast<size_t>(row)] = true;
    }
    // For each group, extract a sub-slice via Arrow filter and route it.
    for (auto& key : group_order) {
      auto& mask = group_masks.at(key);
      // Build the BooleanArray mask at the Arrow level — no series_builder.
      auto builder = arrow::BooleanBuilder{arrow_memory_pool()};
      check(builder.Reserve(static_cast<int64_t>(rows)));
      for (auto v : mask) {
        check(builder.Append(v));
      }
      auto arr = finish(builder);
      auto sub_slice = filter(input, *arr);
      auto sub = ctx.get_sub(make_view(key));
      if (not sub) {
        auto key_kind = match(
          key,
          [](const pattern&) -> ast::constant::kind {
            TENZIR_UNREACHABLE();
          },
          []<class T>(const T& v) -> ast::constant::kind {
            return v;
          });
        auto env = substitute_ctx::env_t{};
        env[args_.let] = std::move(key_kind);
        auto sub_ctx = substitute_ctx{ctx, &env};
        auto copy = args_.pipe.inner;
        if (not copy.substitute(sub_ctx, true)) {
          continue;
        }
        sub = co_await ctx.spawn_sub(key, std::move(copy), tag_v<table_slice>);
      }
      TENZIR_ASSERT(sub);
      std::ignore
        = co_await as<SubHandle<table_slice>>(*sub).push(std::move(sub_slice));
    }
  }

  auto snapshot(Serde& serde) -> void override {
    TENZIR_UNUSED(serde);
  }

private:
  GroupArgs args_;
};

class group_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "group";
  }

  auto describe() const -> Description override {
    auto d = Describer<GroupArgs, Group>{};
    d.positional("over", &GroupArgs::over, "expr");
    d.pipeline(&GroupArgs::pipe, {{"group", &GroupArgs::let}});
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::group

TENZIR_REGISTER_PLUGIN(tenzir::plugins::group::group_plugin)
