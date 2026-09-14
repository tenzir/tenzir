//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/error.hpp>
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
    auto result = tenzir::drop(input, args_.fields, ctx.dh(), true);
    co_await push(std::move(result));
  }

private:
  DropArgs args_;
};

class plugin2 final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "drop";
  }

  auto describe() const -> Description override {
    auto d = Describer<DropArgs, Drop>{};
    d.parallelizable();
    auto fields = d.variadic("fields", &DropArgs::fields, "field");
    d.validate([=](DescribeCtx& ctx) -> Empty {
      auto values = ctx.get_all(fields);
      for (auto& value : values) {
        if (not value) {
          continue;
        }
        if (value->path().empty()) {
          diagnostic::error("cannot drop `this`").primary(*value).emit(ctx);
        }
      }
      return {};
    });
    // Fields other than the dropped ones pass through unchanged, so the
    // downstream projection remains valid upstream. The dropped fields stay
    // projected: `drop` resolves them and warns when they are missing, so
    // upstream must still materialize them.
    return d.field_local(
      [=](DescribeCtx& ctx) -> Option<std::vector<ast::field_path>> {
        auto touched_fields = std::vector<ast::field_path>{};
        for (auto& field : ctx.get_all(fields)) {
          TENZIR_ASSERT(field);
          touched_fields.push_back(std::move(*field));
        }
        return touched_fields;
      });
  }
};

} // namespace

} // namespace tenzir::plugins::drop

TENZIR_REGISTER_PLUGIN(tenzir::plugins::drop::plugin2)
