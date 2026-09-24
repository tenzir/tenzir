//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/async.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/materialize.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/view3.hpp>

#include <arrow/compute/api.h>

#include <unordered_map>
#include <unordered_set>

namespace tenzir::plugins::group {

namespace {

struct GroupArgs {
  ast::expression over;
  located<ir::pipeline> pipe;
  let_id let;
};

struct RowGroup {
  data key;
  std::vector<int64_t> rows;
};

auto make_row_groups(multi_series const& keys) -> std::vector<RowGroup> {
  auto result = std::vector<RowGroup>{};
  auto group_lookup = std::unordered_map<data, size_t>{};
  for (auto row = int64_t{0}; row < keys.length(); ++row) {
    auto key = materialize(keys.view3_at(row));
    auto [it, inserted] = group_lookup.try_emplace(key, result.size());
    if (inserted) {
      result.push_back(RowGroup{std::move(key), {}});
    }
    result[it->second].rows.push_back(row);
  }
  return result;
}

auto constant_from_key(data const& key) -> ast::constant::kind {
  return match(
    key,
    [](pattern const&) -> ast::constant::kind {
      TENZIR_UNREACHABLE();
    },
    []<class T>(T const& value) -> ast::constant::kind {
      return value;
    });
}

class GroupBase {
public:
  explicit GroupBase(GroupArgs args) : args_{std::move(args)} {
  }

protected:
  auto snapshot_impl(Serde& serde) -> void {
    serde("seen_keys", seen_keys_);
  }

  auto process_impl(table_slice input, OpCtx& ctx) -> Task<void> {
    auto keys = eval(args_.over, input, ctx);
    auto groups = make_row_groups(keys);
    for (auto& group : groups) {
      auto sub_slice = take_rows(input, group.rows);
      auto sub = Option<AnySubHandle&>{};
      if (seen_keys_.contains(group.key)) {
        sub = ctx.get_sub(make_view(group.key));
        if (not sub) {
          continue;
        }
      } else {
        auto copy = args_.pipe.inner;
        copy.bind(args_.let, constant_from_key(group.key));
        sub = co_await ctx.plan_and_spawn_sub<table_slice>(group.key,
                                                           std::move(copy));
        if (not sub) {
          continue;
        }
        seen_keys_.emplace(group.key);
      }
      TENZIR_ASSERT(sub);
      std::ignore
        = co_await as<SubHandle<table_slice>>(*sub).push(std::move(sub_slice));
    }
  }

  GroupArgs args_;
  std::unordered_set<data> seen_keys_;
};

template <class Output>
class Group;

template <>
class Group<table_slice> final : public Operator<table_slice, table_slice>,
                                 private GroupBase {
public:
  explicit Group(GroupArgs args) : GroupBase{std::move(args)} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    co_await process_impl(std::move(input), ctx);
  }

  auto snapshot(Serde& serde) -> void override {
    snapshot_impl(serde);
  }
};

template <>
class Group<void> final : public Operator<table_slice, void>,
                          private GroupBase {
public:
  explicit Group(GroupArgs args) : GroupBase{std::move(args)} {
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    co_await process_impl(std::move(input), ctx);
  }

  auto snapshot(Serde& serde) -> void override {
    snapshot_impl(serde);
  }
};

struct NovaGroup {
  data key;
  std::vector<nova::storage::Index> rows;
};

class GroupNovaBase {
public:
  explicit GroupNovaBase(GroupArgs args) : args_{std::move(args)} {
  }

  auto start_impl(OpCtx& ctx) -> Task<void> {
    auto evaluator = nova::Evaluator::make(
      std::move(args_.over), nova::InstantiateCtx{ctx.dh(), ctx.reg()});
    if (evaluator) {
      evaluator_.emplace(std::move(*evaluator));
    }
    co_return;
  }

  auto process_impl(nova::Events input, OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(evaluator_);
    auto keys = evaluator_->eval(input, nova::EvalCtx{ctx.dh()});
    auto groups = std::vector<NovaGroup>{};
    auto group_lookup = std::unordered_map<data, size_t>{};
    for (auto row : nova::storage::true_bits(input.mask)) {
      auto key = nova::materialize(keys.get(row));
      auto [it, inserted] = group_lookup.try_emplace(key, groups.size());
      if (inserted) {
        groups.push_back(NovaGroup{std::move(key), {}});
      }
      groups[it->second].rows.push_back(row);
    }
    for (auto& group : groups) {
      auto sub = Option<AnySubHandle&>{};
      if (seen_keys_.contains(group.key)) {
        sub = ctx.get_sub(make_view(group.key));
        if (not sub) {
          continue;
        }
      } else {
        auto copy = args_.pipe.inner;
        copy.bind(args_.let, constant_from_key(group.key));
        sub = co_await ctx.plan_and_spawn_sub<nova::Events>(group.key,
                                                            std::move(copy));
        if (not sub) {
          continue;
        }
        seen_keys_.emplace(group.key);
      }
      TENZIR_ASSERT(sub);
      auto mask = nova::storage::BitMap::Mutable{input.length()};
      for (auto row : group.rows) {
        mask.set(row, true);
      }
      auto events
        = nova::Events{input.data, std::move(mask).finish(), input.meta};
      std::ignore
        = co_await as<SubHandle<nova::Events>>(*sub).push(std::move(events));
    }
  }

  auto snapshot_impl(Serde& serde) -> void {
    serde("seen_keys", seen_keys_);
  }

  GroupArgs args_;
  Option<nova::Evaluator> evaluator_;
  std::unordered_set<data> seen_keys_;
};

class GroupEvents final : public Operator<nova::Events, nova::Events>,
                          private GroupNovaBase {
public:
  explicit GroupEvents(GroupArgs args) : GroupNovaBase{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return start_impl(ctx);
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    return process_impl(std::move(input), ctx);
  }

  auto snapshot(Serde& serde) -> void override {
    snapshot_impl(serde);
  }
};

class GroupEventsSink final : public Operator<nova::Events, void>,
                              private GroupNovaBase {
public:
  explicit GroupEventsSink(GroupArgs args) : GroupNovaBase{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return start_impl(ctx);
  }

  auto process(nova::Events input, OpCtx& ctx) -> Task<void> override {
    return process_impl(std::move(input), ctx);
  }

  auto snapshot(Serde& serde) -> void override {
    snapshot_impl(serde);
  }
};

class group_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "group";
  }

  auto describe() const -> Description override {
    auto d = Describer<GroupArgs, Group<table_slice>, Group<void>, GroupEvents,
                       GroupEventsSink>{};
    auto over = d.positional("over", &GroupArgs::over, "expr");
    auto pipe = d.pipeline(&GroupArgs::pipe, SubOptimize::from_downstream,
                           {{"group", &GroupArgs::let}});
    d.parallelizable();
    d.partition_keys([](const GroupArgs& args) -> std::vector<ast::expression> {
      return {args.over};
    });
    d.spawner([pipe]<class Input>(DescribeCtx& ctx)
                -> failure_or<Option<SpawnWith<GroupArgs, Input>>> {
      if constexpr (std::same_as<Input, table_slice>) {
        TRY(auto pipe, ctx.get(pipe));
        TRY(auto output, pipe.inner.infer_type(tag_v<table_slice>, ctx));
        return match(
          output,
          [](tag<table_slice>)
            -> failure_or<Option<SpawnWith<GroupArgs, Input>>> {
            return [](GroupArgs args) {
              return Group<table_slice>{std::move(args)};
            };
          },
          [](tag<void>) -> failure_or<Option<SpawnWith<GroupArgs, Input>>> {
            return [](GroupArgs args) {
              return Group<void>{std::move(args)};
            };
          },
          [&](
            tag<chunk_ptr>) -> failure_or<Option<SpawnWith<GroupArgs, Input>>> {
            diagnostic::error("subpipeline must not produce bytes")
              .primary(pipe.source)
              .emit(ctx);
            return failure::promise();
          },
          [&](tag<nova::Events>)
            -> failure_or<Option<SpawnWith<GroupArgs, Input>>> {
            diagnostic::error("subpipeline must not produce nova_events")
              .primary(pipe.source)
              .emit(ctx);
            return failure::promise();
          },
          [](tag<FileHandle>)
            -> failure_or<Option<SpawnWith<GroupArgs, Input>>> {
            // Files only feed readers, so no subpipeline produces them.
            TENZIR_UNREACHABLE();
          });
      } else if constexpr (std::same_as<Input, nova::Events>) {
        TRY(auto pipe, ctx.get(pipe));
        TRY(auto output, pipe.inner.infer_type(tag_v<nova::Events>, ctx));
        return match(
          output,
          [](tag<nova::Events>)
            -> failure_or<Option<SpawnWith<GroupArgs, Input>>> {
            return [](GroupArgs args) {
              return GroupEvents{std::move(args)};
            };
          },
          [](tag<void>) -> failure_or<Option<SpawnWith<GroupArgs, Input>>> {
            return [](GroupArgs args) {
              return GroupEventsSink{std::move(args)};
            };
          },
          [&](
            tag<chunk_ptr>) -> failure_or<Option<SpawnWith<GroupArgs, Input>>> {
            diagnostic::error("subpipeline must not produce bytes")
              .primary(pipe.source)
              .emit(ctx);
            return failure::promise();
          },
          [&](tag<table_slice>)
            -> failure_or<Option<SpawnWith<GroupArgs, Input>>> {
            diagnostic::error("subpipeline must not produce legacy events")
              .primary(pipe.source)
              .emit(ctx);
            return failure::promise();
          },
          [](tag<FileHandle>)
            -> failure_or<Option<SpawnWith<GroupArgs, Input>>> {
            // Files only feed readers, so no subpipeline produces them.
            TENZIR_UNREACHABLE();
          });
      } else {
        return {};
      }
    });
    return d.optimize(
      [over](DescribeCtx& ctx, ir::OptimizeRequest req) -> Optimization {
        if (auto value = ctx.get(over)) {
          ir::add_refs_to_projection(req.projection, *value);
        }
        return {
          .order = EventOrder::ordered,
          .filter_self = std::move(req.filter),
          .projection_upstream = std::move(req.projection),
        };
      });
  }
};

} // namespace

} // namespace tenzir::plugins::group

TENZIR_REGISTER_PLUGIN(tenzir::plugins::group::group_plugin)
