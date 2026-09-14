//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/file.hpp>
#include <tenzir/format_utils.hpp>
#include <tenzir/glob.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/scope_linked.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/exec.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/resolve.hpp>
#include <tenzir/tql2/set.hpp>

#include <arrow/filesystem/api.h>
#include <arrow/util/future.h>
#include <arrow/util/uri.h>
#include <caf/actor_from_state.hpp>
#include <caf/mail_cache.hpp>

#include <ranges>

namespace tenzir::plugins::from {

namespace {

class From final : public Operator<void, table_slice> {
public:
  explicit From(std::vector<ast::expression> events)
    : events_{std::move(events)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    read_bytes_counter_ = ctx.make_counter(
      MetricsLabel{
        "operator",
        "from_events",
      },
      MetricsDirection::read, MetricsVisibility::internal_, MetricsUnit::bytes);
    read_events_counter_ = ctx.make_counter(
      MetricsLabel{
        "operator",
        "from_events",
      },
      MetricsDirection::read, MetricsVisibility::internal_,
      MetricsUnit::events);
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return {};
  }

  auto process_task(Any, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_ASSERT(next_ < events_.size());
    auto result = const_eval_series(events_[next_], ctx);
    if (not result) {
      co_await assert_cancelled();
    }
    auto cast = std::move(result).unwrap().as<record_type>();
    if (not cast) {
      diagnostic::error("expected `record`").primary(events_[next_]).emit(ctx);
      co_return;
    }
    auto schema = tenzir::type{"tenzir.from", cast->type};
    auto slice = table_slice{
      record_batch_from_struct_array(schema.to_arrow_schema(), *cast->array),
      schema};
    auto const bytes = slice.approx_bytes();
    auto const rows = slice.rows();
    co_await push(std::move(slice));
    read_bytes_counter_.add(bytes);
    read_events_counter_.add(rows);
    next_ += 1;
  }

  auto state() -> OperatorState override {
    if (next_ == events_.size()) {
      return OperatorState::done;
    }
    return OperatorState::normal;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("next", next_);
  }

private:
  size_t next_ = 0;
  std::vector<ast::expression> events_;
  MetricsCounter read_bytes_counter_;
  MetricsCounter read_events_counter_;
};

class from_ir final : public ir::Operator {
public:
  from_ir() = default;

  from_ir(std::vector<ast::expression> events, location self)
    : events_{std::move(events)}, self_{self} {
  }

  auto name() const -> std::string override {
    return "from_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TENZIR_UNUSED(instantiate);
    for (auto& event : events_) {
      TRY(event.substitute(ctx));
    }
    return {};
  }

  auto spawn(element_type_tag input) const -> AnyOperator override {
    TENZIR_ASSERT(input.is<void>());
    return From{events_}.with_name("from");
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override {
    if (input.is_not<void>()) {
      diagnostic::error("expected void, got {}", input).primary(self_).emit(dh);
      return failure::promise();
    }
    return tag_v<table_slice>;
  }

  friend auto inspect(auto& f, from_ir& x) -> bool {
    return f.object(x).fields(f.field("events", x.events_),
                              f.field("self", x.self_));
  }

private:
  std::vector<ast::expression> events_;
  location self_;
};

class from_plugin2 final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "from";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    for (auto& arg : inv.args) {
      TRY(arg.bind(ctx));
    }
    return from_ir{std::move(inv.args), inv.op.get_location()};
  }
};

} // namespace

} // namespace tenzir::plugins::from

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::from_plugin2)
TENZIR_REGISTER_PLUGIN(tenzir::inspection_plugin<
                       tenzir::ir::Operator, tenzir::plugins::from::from_ir>);
