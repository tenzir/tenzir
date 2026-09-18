//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::pass {

namespace {

struct PassArgs {
  // No arguments needed for pass operator
};

class PassTableSlice final : public Operator<table_slice, table_slice> {
public:
  explicit PassTableSlice(PassArgs /*args*/) {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await push(std::move(input));
  }
};

class PassChunk final : public Operator<chunk_ptr, chunk_ptr> {
public:
  explicit PassChunk(PassArgs /*args*/) {
  }

  auto process(chunk_ptr input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await push(std::move(input));
  }
};

class PassEvents final : public Operator<nova::Events, nova::Events> {
public:
  explicit PassEvents(PassArgs /*args*/) {
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await push(std::move(input));
  }
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "pass";
  }

  auto describe() const -> Description override {
    auto d = Describer<PassArgs, PassTableSlice, PassChunk, PassEvents>{};
    return d.optimize(
      [](DescribeCtx&, ir::OptimizeRequest req) -> Optimization {
        return {
          .order = req.order,
          .filter_upstream = std::move(req.filter),
          .drop = true,
          .limit_upstream = req.limit,
          .projection_upstream = std::move(req.projection),
        };
      });
  }
};

} // namespace

} // namespace tenzir::plugins::pass

TENZIR_REGISTER_PLUGIN(tenzir::plugins::pass::plugin)
