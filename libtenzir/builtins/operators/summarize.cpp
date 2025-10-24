//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_time_utils.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/convertible/data.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/detail/zip_iterator.hpp>
#include <tenzir/error.hpp>
#include <tenzir/hash/hash_append.hpp>
#include <tenzir/operator_control_plane.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/registry.hpp>
#include <tenzir/type.hpp>

#include <arrow/compute/api_scalar.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <caf/expected.hpp>
#include <tsl/robin_map.h>

#include <algorithm>
#include <ranges>
#include <string_view>
#include <utility>
#include <variant>

namespace tenzir::plugins::summarize {

namespace {

/// The key by which aggregations are grouped. Essentially, this is a vector of
/// data. We create a new type here to support a custom hash and equality
/// operation to support lookups with non-materialized keys.
struct group_by_key : std::vector<data> {
  using vector::vector;
};

/// A view on a group-by key.
struct group_by_key_view : std::vector<data_view> {
  using vector::vector;

  /// Materializes a view on a group-by key.
  /// @param views The group-by key view to materialize.
  friend group_by_key materialize(const group_by_key_view& views) {
    auto result = group_by_key{};
    result.reserve(views.size());
    for (const auto& view : views) {
      result.push_back(materialize(view));
    }
    return result;
  }
};

/// The hash functor for enabling use of *group_by_key* as a key in unordered
/// map data structures with transparent lookup.
struct group_by_key_hash {
  size_t operator()(const group_by_key& x) const noexcept {
    auto hasher = xxh64{};
    for (const auto& value : x) {
      hash_append(hasher, make_view(value));
    }
    return hasher.finish();
  }

  size_t operator()(const group_by_key_view& x) const noexcept {
    auto hasher = xxh64{};
    for (const auto& value : x) {
      hash_append(hasher, value);
    }
    return hasher.finish();
  }
};

/// The equality functor for enabling use of *group_by_key* as a key in
/// unordered map data structures with transparent lookup.
struct group_by_key_equal {
  using is_transparent = void;

  bool
  operator()(const group_by_key_view& x, const group_by_key& y) const noexcept {
    return std::equal(x.begin(), x.end(), y.begin(), y.end(),
                      [](const auto& lhs, const auto& rhs) {
                        return lhs == make_view(rhs);
                      });
  }

  bool
  operator()(const group_by_key& x, const group_by_key_view& y) const noexcept {
    return std::equal(x.begin(), x.end(), y.begin(), y.end(),
                      [](const auto& lhs, const auto& rhs) {
                        return make_view(lhs) == rhs;
                      });
  }

  bool operator()(const group_by_key& x, const group_by_key& y) const noexcept {
    return x == y;
  }

  bool operator()(const group_by_key_view& x,
                  const group_by_key_view& y) const noexcept {
    return x == y;
  }
};

struct aggregate_t {
  std::optional<ast::field_path> dest;
  ast::function_call call;

  friend auto inspect(auto& f, aggregate_t& x) -> bool {
    return f.object(x).fields(f.field("dest", x.dest), f.field("call", x.call));
  }
};

struct group_t {
  std::optional<ast::field_path> dest;
  ast::field_path expr;

  friend auto inspect(auto& f, group_t& x) -> bool {
    return f.object(x).fields(f.field("dest", x.dest), f.field("expr", x.expr));
  }
};

struct config {
  std::vector<aggregate_t> aggregates;
  std::vector<group_t> groups;

  /// Because we allow mixing aggregates and groups and want to emit them in the
  /// same order, we need to store some additional information, unless we use
  /// something like `vector<variant<aggregate_t, ast::selector>>` instead. But
  /// that makes it more tricky to `zip`. If the index is positive, it
  /// corresponds to `aggregates`, otherwise `groups[-index - 1]`.
  std::vector<int64_t> indices;

  friend auto inspect(auto& f, config& x) -> bool {
    return f.object(x).fields(f.field("aggregates", x.aggregates),
                              f.field("groups", x.groups),
                              f.field("indices", x.indices));
  }
};

template <class Value>
using group_map
  = tsl::robin_map<group_by_key, Value, group_by_key_hash, group_by_key_equal>;

struct bucket2 {
  std::vector<std::unique_ptr<aggregation_instance>> aggregations{};
};

class implementation2 {
public:
  explicit implementation2(const config& cfg, session ctx)
    : cfg_{cfg}, ctx_{ctx} {
  }

  auto make_bucket() -> std::unique_ptr<bucket2> {
    auto bucket = std::make_unique<bucket2>();
    for (const auto& aggr : cfg_.aggregates) {
      // We already checked the cast and instantiation before.
      const auto* fn
        = dynamic_cast<const aggregation_plugin*>(&ctx_.reg().get(aggr.call));
      TENZIR_ASSERT(fn);
      bucket->aggregations.push_back(
        fn->make_aggregation(aggregation_plugin::invocation{aggr.call}, ctx_)
          .unwrap());
    }
    return bucket;
  }

  void add(const table_slice& slice) {
    auto group_values = std::vector<multi_series>{};
    for (auto& group : cfg_.groups) {
      group_values.push_back(eval(group.expr.inner(), slice, ctx_));
    }
    auto key = group_by_key_view{};
    key.resize(cfg_.groups.size());
    auto update_group = [&](bucket2& group, int64_t begin, int64_t end) {
      for (auto&& aggr : group.aggregations) {
        aggr->update(subslice(slice, begin, end), ctx_);
      }
    };
    auto find_or_create_group = [&](int64_t row) -> bucket2* {
      for (auto&& [key_value, group] : detail::zip_equal(key, group_values)) {
        key_value = group.value_at(row);
      }
      auto it = groups_.find(key);
      if (it == groups_.end()) {
        it = groups_.emplace_hint(it, materialize(key), make_bucket());
      }
      return &*it->second;
    };
    auto total_rows = detail::narrow<int64_t>(slice.rows());
    auto current_group = find_or_create_group(0);
    auto current_begin = int64_t{0};
    for (auto row = int64_t{1}; row < total_rows; ++row) {
      auto group = find_or_create_group(row);
      if (current_group != group) {
        update_group(*current_group, current_begin, row);
        current_group = group;
        current_begin = row;
      }
    }
    update_group(*current_group, current_begin, total_rows);
  }

  auto finish() -> std::vector<table_slice> {
    auto emplace = [](record& root, const ast::field_path& sel, data value) {
      if (sel.path().empty()) {
        // TODO
        if (auto rec = try_as<record>(&value)) {
          root = std::move(*rec);
        }
        return;
      }
      auto current = &root;
      for (auto& segment : sel.path()) {
        auto& val = (*current)[segment.id.name];
        if (&segment == &sel.path().back()) {
          val = std::move(value);
        } else {
          current = try_as<record>(&val);
          if (not current) {
            val = record{};
            current = &as<record>(val);
          }
        }
      }
    };
    const auto finish_group = [&](const auto& key, const auto& group) {
      auto result = record{};
      for (auto index : cfg_.indices) {
        if (index >= 0) {
          auto& dest = cfg_.aggregates[index].dest;
          auto value = group->aggregations[index]->get();
          if (dest) {
            emplace(result, *dest, value);
          } else {
            auto& call = cfg_.aggregates[index].call;
            // TODO: Decide and properly implement this.
            auto arg = std::invoke([&]() -> std::string {
              if (call.args.empty()) {
                return "";
              }
              if (call.args.size() > 1) {
                return "...";
              }
              auto sel = ast::field_path::try_from(call.args[0]);
              if (not sel) {
                return "...";
              }
              auto arg = std::string{};
              if (sel->has_this()) {
                arg = "this";
              }
              for (auto& segment : sel->path()) {
                // TODO: This is wrong if the path contains special characters.
                if (not arg.empty()) {
                  arg += '.';
                }
                arg += segment.id.name;
              }
              return arg;
            });
            result.emplace(fmt::format("{}({})", call.fn.path[0].name, arg),
                           value);
          }
        } else {
          index = -index - 1;
          auto& group = cfg_.groups[index];
          auto& dest = group.dest ? *group.dest : group.expr;
          auto& value = key[index];
          emplace(result, dest, value);
        }
      }
      return result;
    };
    // Special case: if there are no configured groups, and no groups were
    // created because we didn't get any input events, then we create a new
    // bucket and just finish it. That way, `from [] | summarize count()` will
    // return a single event showing a count of zero.
    if (cfg_.groups.empty() and groups_.empty()) {
      auto b = series_builder{};
      b.data(finish_group(group_by_key{}, make_bucket()));
      return b.finish_as_table_slice();
    }
    // TODO: Group by schema again to make this more efficient.
    auto b = series_builder{};
    for (const auto& [key, group] : groups_) {
      b.data(finish_group(key, group));
    }
    return b.finish_as_table_slice();
  }

private:
  const config& cfg_;
  session ctx_;
  group_map<std::unique_ptr<bucket2>> groups_;
};

class summarize_operator2 final : public crtp_operator<summarize_operator2> {
public:
  summarize_operator2() = default;

  explicit summarize_operator2(config cfg) : cfg_{std::move(cfg)} {
  }

  auto name() const -> std::string override {
    return "tql2.summarize";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // TODO: Do not create a new session here.
    auto info = cfg_.groups.size() == 0 and cfg_.aggregates.size() == 1
                and cfg_.aggregates[0].dest
                and cfg_.aggregates[0].dest->path().size() == 1
                and cfg_.aggregates[0].dest->path()[0].id.name == "foo";
    if (info) {
      TENZIR_WARN("spawning summarize");
    }
    ctrl.self().attach_functor([info] {
      if (info) {
        TENZIR_WARN("destroying summarize");
      }
    });
    auto provider = session_provider::make(ctrl.diagnostics());
    auto impl = implementation2{cfg_, provider.as_session()};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      impl.add(slice);
    }
    for (auto slice : impl.finish()) {
      co_yield std::move(slice);
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return optimize_result{std::nullopt, event_order::unordered, copy()};
  }

  friend auto inspect(auto& f, summarize_operator2& x) -> bool {
    return f.apply(x.cfg_);
  }

private:
  config cfg_;
};

class plugin2 final : public operator_plugin2<summarize_operator2> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto cfg = config{};
    auto add_aggregate = [&](std::optional<ast::field_path> dest,
                             ast::function_call call) {
      // TODO: Improve this and try to forward function handle directly.
      auto fn = dynamic_cast<const aggregation_plugin*>(&ctx.reg().get(call));
      if (not fn) {
        diagnostic::error("function does not support aggregations")
          .primary(call.fn)
          .hint("if you want to group by this, use assignment before")
          .docs("https://docs.tenzir.com/operators/summarize")
          .emit(ctx);
        return;
      }
      // We test the arguments by making and discarding it. This is a bit
      // hacky and should be improved in the future.
      if (fn->make_aggregation(aggregation_plugin::invocation{call}, ctx)) {
        auto index = detail::narrow<int64_t>(cfg.aggregates.size());
        cfg.indices.push_back(index);
        cfg.aggregates.emplace_back(std::move(dest), std::move(call));
      }
    };
    auto add_group
      = [&](std::optional<ast::field_path> dest, ast::field_path expr) {
          auto index = -detail::narrow<int64_t>(cfg.groups.size()) - 1;
          cfg.indices.push_back(index);
          cfg.groups.emplace_back(std::move(dest), std::move(expr));
        };
    for (auto& arg : inv.args) {
      arg.match(
        [&](ast::function_call& arg) {
          add_aggregate(std::nullopt, std::move(arg));
        },
        [&](ast::assignment& arg) {
          auto left = std::get_if<ast::field_path>(&arg.left);
          if (not left) {
            // TODO
            diagnostic::error("expected data selector, not meta")
              .primary(arg.left)
              .emit(ctx);
            return;
          }
          arg.right.match(
            [&](ast::function_call& right) {
              add_aggregate(std::move(*left), std::move(right));
            },
            [&](auto&) {
              auto right = ast::field_path::try_from(arg.right);
              if (right) {
                add_group(std::move(*left), std::move(*right));
              } else {
                diagnostic::error(
                  "expected selector or aggregation function call")
                  .primary(arg.right)
                  .emit(ctx);
              }
            });
        },
        [&](auto&) {
          auto selector = ast::field_path::try_from(arg);
          if (selector) {
            add_group(std::nullopt, std::move(*selector));
          } else {
            diagnostic::error(
              "expected selector, assignment or aggregation function call")
              .primary(arg)
              .emit(ctx);
          }
        });
    }
    return std::make_unique<summarize_operator2>(std::move(cfg));
  }
};

} // namespace

} // namespace tenzir::plugins::summarize

TENZIR_REGISTER_PLUGIN(tenzir::plugins::summarize::plugin2)
