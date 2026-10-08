//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/array.hpp"
#include "tenzir/nova/eval_util.hpp"
#include "tenzir/nova/events.hpp"

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/error.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/type.hpp>

#include <arrow/type.h>
#include <fmt/format.h>

#include <string_view>

namespace tenzir::plugins::drop {

namespace {

/// The configuration of a project pipeline operator.
struct configuration {
  /// The key suffixes of the fields to drop.
  std::vector<std::string> fields = {};

  /// The key suffixes of the schemas to drop.
  std::vector<std::string> schemas = {};

  /// Support type inspection for easy parsing with convertible.
  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return detail::apply_all(f, x.fields, x.schemas);
  }

  /// Enable parsing from a record via convertible.
  static inline const record_type& schema() noexcept {
    static auto result = record_type{
      {"fields", list_type{string_type{}}},
      {"schemas", list_type{string_type{}}},
    };
    return result;
  }
};

struct DropArgs {
  std::vector<ast::field_path> fields;
};

class Drop final : public Operator<table_slice, table_slice> {
public:
  explicit Drop(DropArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto result = tenzir::drop(input, args_.fields, ctx.dh());
    co_await push(std::move(result));
  }

private:
  DropArgs args_;
};

class DropNova final : public Operator<nova::Events, nova::Events> {
public:
  explicit DropNova(DropArgs args)
    : fields_{std::move(args.fields)},
      drop_tree_{nova::DropTree::make(fields_)} {
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    input.data = drop_tree_.apply(std::move(input.data), input.mask);
    co_await push(std::move(input));
  }

private:
  std::vector<ast::field_path> fields_;
  nova::DropTree drop_tree_;
};

class plugin2 final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "drop";
  }

  auto describe() const -> Description override {
    auto d = Describer<DropArgs, Drop, DropNova>{};
    d.parallelizable();
    auto fields = d.variadic("fields", &DropArgs::fields, "field");
    d.validate([=](DescribeCtx& ctx) -> Empty {
      auto values = ctx.get_all(fields);
      // Fields that are dropped for real, after removing redundant entries.
      // Dropping a record also drops everything within it, so a path that is a
      // prefix of an earlier one replaces it.
      auto effective = std::vector<ast::field_path>{};
      for (auto& value : values) {
        if (not value) {
          continue;
        }
        if (value->path().empty()) {
          diagnostic::error("cannot drop `this`").primary(*value).emit(ctx);
          continue;
        }
        auto redundant = false;
        for (auto& previous : effective) {
          const auto previous_covers
            = ir::is_field_path_prefix(previous, *value);
          const auto value_covers = ir::is_field_path_prefix(*value, previous);
          if (previous_covers and value_covers) {
            diagnostic::warning("field `{}` may only be dropped once",
                                value->path().back().id.name)
              .primary(previous)
              .primary(*value)
              .emit(ctx);
            redundant = true;
            break;
          }
          if (previous_covers) {
            diagnostic::warning("ignoring dropped field within record")
              .primary(*value, "ignoring this field")
              .secondary(previous, "because it is already dropped here")
              .emit(ctx);
            redundant = true;
            break;
          }
          if (value_covers) {
            diagnostic::warning("ignoring dropped field within dropped record")
              .primary(previous, "ignoring this field")
              .secondary(*value, "because it is already dropped here")
              .emit(ctx);
            previous = std::move(*value);
            redundant = true;
            break;
          }
        }
        if (not redundant) {
          effective.push_back(std::move(*value));
        }
      }
      return {};
    });
    return d.optimize(
      [=](DescribeCtx& ctx, ir::OptimizeRequest req) -> Optimization {
        auto dropped = std::vector<ast::field_path>{};
        for (auto& field : ctx.get_all(fields)) {
          TENZIR_ASSERT(field);
          dropped.push_back(std::move(*field));
        }
        // Fields other than the dropped ones pass through unchanged, so the
        // downstream projection remains valid upstream. The dropped fields stay
        // projected: `drop` resolves them and warns when they are missing, so
        // upstream must still materialize them.
        auto projection = std::move(req.projection);
        for (auto const& path : dropped) {
          ir::add_to_projection(projection, path);
        }
        // A dropped field reads as `null` downstream. A record that contains a
        // dropped field has no equivalent upstream.
        auto split = ir::split_filter_by_substitution(
          std::move(req.filter),
          [&](ast::field_path const& field) -> Option<ast::expression> {
            for (auto const& path : dropped) {
              if (ir::is_field_path_prefix(path, field)) {
                return ast::constant{caf::none, field.get_location()};
              }
            }
            for (auto const& path : dropped) {
              if (ir::is_field_path_prefix(field, path)) {
                return None{};
              }
            }
            return field.inner();
          });
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

} // namespace tenzir::plugins::drop

TENZIR_REGISTER_PLUGIN(tenzir::plugins::drop::plugin2)
