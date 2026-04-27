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
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/view3.hpp>

#include <arrow/compute/api.h>

#include <unordered_set>
#include <unordered_map>

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

auto take_rows(table_slice const& input, std::vector<int64_t> const& rows)
  -> table_slice {
  TENZIR_ASSERT(not rows.empty());
  auto builder = arrow::Int64Builder{arrow_memory_pool()};
  check(builder.Reserve(detail::narrow<int64_t>(rows.size())));
  for (auto row : rows) {
    check(builder.Append(row));
  }
  auto indices = finish(builder);
  auto datum = check(arrow::compute::Take(to_record_batch(input), indices));
  TENZIR_ASSERT(datum.kind() == arrow::Datum::Kind::RECORD_BATCH);
  auto result = table_slice{datum.record_batch(), input.schema()};
  result.offset(input.offset());
  result.import_time(input.import_time());
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
        auto key_kind = constant_from_key(group.key);
        auto env = substitute_ctx::env_t{};
        env[args_.let] = std::move(key_kind);
        auto sub_ctx = substitute_ctx{ctx, &env};
        auto copy = args_.pipe.inner;
        if (not copy.substitute(sub_ctx, true)) {
          continue;
        }
        seen_keys_.emplace(group.key);
        sub = co_await ctx.spawn_sub(group.key, std::move(copy),
                                     tag_v<table_slice>);
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

class group_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "group";
  }

  auto describe() const -> Description override {
    auto d = Describer<GroupArgs>{};
    d.positional("over", &GroupArgs::over, "expr");
    auto pipe = d.pipeline(&GroupArgs::pipe, {{"group", &GroupArgs::let}});
    d.spawner([pipe]<class Input>(DescribeCtx& ctx)
                -> failure_or<Option<SpawnWith<GroupArgs, Input>>> {
      if constexpr (std::same_as<Input, table_slice>) {
        TRY(auto pipe, ctx.get(pipe));
        TRY(auto output, pipe.inner.infer_type(tag_v<table_slice>, ctx));
        if (not output) {
          diagnostic::error("subpipeline must not produce bytes")
            .primary(pipe.source)
            .emit(ctx);
          return failure::promise();
        }
        return match(
          *output,
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
          [&](tag<chunk_ptr>)
            -> failure_or<Option<SpawnWith<GroupArgs, Input>>> {
            diagnostic::error("subpipeline must not produce bytes")
              .primary(pipe.source)
              .emit(ctx);
            return failure::promise();
          });
      } else {
        return {};
      }
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::group

TENZIR_REGISTER_PLUGIN(tenzir::plugins::group::group_plugin)
