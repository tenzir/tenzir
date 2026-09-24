//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concepts.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/error.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/eval_util.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/nova/materialize.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/set.hpp>

#include <arrow/type.h>
#include <tsl/robin_map.h>

#include <algorithm>
#include <utility>
#include <vector>

namespace tenzir::plugins::enumerate {

namespace {

auto default_output_field() -> ast::field_path {
  auto out = ast::field_path::try_from(
    ast::root_field{ast::identifier{"#", location::unknown}});
  TENZIR_ASSERT(out);
  return std::move(*out);
}

struct EnumerateArgs {
  ast::field_path out = default_output_field();
  Option<ast::expression> group;

  friend auto inspect(auto& f, EnumerateArgs& x) -> bool {
    return f.object(x).fields(f.field("out", x.out), f.field("group", x.group));
  }
};

using GroupMap = tsl::robin_map<data, int64_t>;

auto find_group(GroupMap& groups, data key) -> GroupMap::iterator {
  auto it = groups.find(key);
  match(
    key,
    [&]<class T>(T const& x)
      requires concepts::integer<T>
    {
      using OtherType
        = std::conditional_t<std::same_as<T, int64_t>, uint64_t, int64_t>;
      if (it == groups.end() and std::in_range<OtherType>(x)) {
        it = groups.find(data_view{static_cast<OtherType>(x)});
      }
    },
    [](auto const&) {});
  if (it == groups.end()) {
    it = groups.emplace_hint(it, std::move(key), int64_t{0});
  }
  return it;
}

class Enumerate final : public Operator<table_slice, table_slice> {
public:
  explicit Enumerate(EnumerateArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto builder = int64_type::make_arrow_builder(arrow_memory_pool());
    check(builder->Reserve(detail::narrow_cast<int64_t>(input.rows())));
    if (args_.group) {
      for (auto const& result : eval(*args_.group, input, ctx)) {
        for (auto const& value : result.values()) {
          auto it = find_group(groups_, materialize(value));
          check(builder->Append(it.value()++));
        }
      }
    } else {
      for (auto i = int64_t{0}; i < detail::narrow<int64_t>(input.rows());
           ++i) {
        check(builder->Append(next_id_++));
      }
    }
    auto output = assign(args_.out, series{int64_type{}, finish(*builder)},
                         input, ctx, assign_position::front);
    co_await push(std::move(output));
  }

  auto snapshot(Serde& serde) -> void override {
    serde("next_id", next_id_);
    serde("groups", groups_);
  }

private:
  EnumerateArgs args_;
  int64_t next_id_ = 0;
  GroupMap groups_;
};

class EnumerateNova final : public Operator<nova::Events, nova::Events> {
public:
  explicit EnumerateNova(EnumerateArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (args_.out.path().empty()) {
      diagnostic::error("enumerate output must be a field path")
        .primary(args_.out)
        .emit(ctx);
      co_return;
    }
    if (args_.group) {
      auto evaluator = nova::Evaluator::make(
        std::move(*args_.group), nova::InstantiateCtx{ctx.dh(), ctx.reg()});
      if (not evaluator) {
        co_return;
      }
      group_.emplace(std::move(*evaluator));
    }
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    auto builder = nova::ArrayBuilder<nova::Int>{};
    auto keys = Option<nova::Array<nova::Data>>{};
    if (group_) {
      keys = group_->eval(input, nova::EvalCtx{ctx.dh()});
    }
    nova::storage::for_each_true(input.mask, [&](auto row) {
      builder.skip_n(row - builder.length());
      auto& counter
        = keys ? find_group(groups_, nova::materialize(keys->get(row))).value()
               : next_id_;
      builder.data(counter++);
    });
    builder.skip_n(input.length() - builder.length());
    input.data
      = nova::assign_nested_field(std::move(input.data), args_.out.path(),
                                  {builder.finish(), input.mask}, ctx.dh(),
                                  nova::FieldPosition::front);
    co_await push(std::move(input));
  }

  auto snapshot(Serde& serde) -> void override {
    serde("next_id", next_id_);
    serde("groups", groups_);
  }

private:
  EnumerateArgs args_;
  Option<nova::Evaluator> group_;
  int64_t next_id_ = 0;
  GroupMap groups_;
};

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "enumerate";
  }

  auto describe() const -> Description override {
    auto d = Describer<EnumerateArgs, Enumerate, EnumerateNova>{};
    auto out = d.optional_positional("out", &EnumerateArgs::out);
    auto group = d.named("group", &EnumerateArgs::group, "any");
    // `enumerate` numbers events by arrival order (per group). Predicates
    // must not cross it, since filtering upstream would change the assigned
    // indices, and ordered input is required for the same reason.
    return d.optimize(
      [=](DescribeCtx& ctx, ir::OptimizeRequest req) -> Optimization {
        // The output field is produced here; all other fields pass through.
        auto projection = std::move(req.projection);
        if (projection) {
          auto out_path = ctx.get(out);
          if (not out_path and not ctx.get_location(out)) {
            out_path = EnumerateArgs{}.out;
          }
          if (out_path) {
            std::erase_if(*projection, [&](const ast::field_path& path) {
              return ir::is_field_path_prefix(*out_path, path);
            });
            // A nested assignment observes its existing parent and warns when
            // a scalar must be replaced by an implicit record. Retain the
            // target so projection cannot hide that diagnostic.
            if (out_path->path().size() > 1) {
              ir::add_to_projection(projection, *out_path);
            }
          } else {
            projection = None{};
          }
        }
        // Grouping reads its key expression from the input.
        if (ctx.get_location(group)) {
          if (auto expr = ctx.get(group)) {
            ir::add_refs_to_projection(projection, *expr);
          } else {
            projection = None{};
          }
        }
        // All predicates stay behind us. A carried limit still passes through
        // when there are none: `enumerate` is 1:1, so the first N outputs stem
        // from exactly the first N inputs.
        auto limit = req.filter.empty() ? req.limit : Option<uint64_t>{};
        return {
          .order = EventOrder::ordered,
          .filter_self = std::move(req.filter),
          .limit_upstream = limit,
          .projection_upstream = std::move(projection),
        };
      });
  }
};

} // namespace

} // namespace tenzir::plugins::enumerate

TENZIR_REGISTER_PLUGIN(tenzir::plugins::enumerate::Plugin)
