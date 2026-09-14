//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/task.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::block {

namespace {

struct BlockArgs {
  tenzir::duration duration = {};
};

template <class T>
class Block final : public Operator<T, T> {
public:
  explicit Block(BlockArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await sleep_for(args_.duration);
  }

  auto process(T input, Push<T>& push, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await push(std::move(input));
  }

private:
  BlockArgs args_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "_block";
  }

  auto describe() const -> Description override {
    auto d = Describer<BlockArgs, Block<table_slice>, Block<chunk_ptr>>{};
    d.positional("duration", &BlockArgs::duration);
    // `_block` only delays its input; events pass through unchanged, so
    // downstream hints pass through as well.
    return d.transparent();
  }
};

} // namespace

} // namespace tenzir::plugins::block

TENZIR_REGISTER_PLUGIN(tenzir::plugins::block::plugin)
