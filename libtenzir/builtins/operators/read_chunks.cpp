//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/as_bytes.hpp>
#include <tenzir/async/pusher.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>

namespace tenzir::plugins::read_chunks {

namespace {

struct ReadChunksArgs {
  location operator_location;
};

class ReadChunks final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadChunks(ReadChunksArgs args) : args_{args} {
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    if (input->size() == 0) {
      co_return;
    }
    builder_.record().field("data", as_bytes(input));
    co_await pusher_.push(builder_.yield_ready(type_name), push);
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_await pusher_.wait();
    co_return {};
  }

  auto process_task(Any, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    co_await pusher_.push(builder_.yield_ready(type_name), push);
  }

  auto finalize(Push<table_slice>& push, OpCtx&)
    -> Task<FinalizeBehavior> override {
    if (builder_.length() > 0) {
      co_await push(builder_.finish_assert_one_slice(type_name));
    }
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    if (builder_.length() > 0) {
      co_await push(builder_.finish_assert_one_slice(type_name));
    }
  }

private:
  constexpr static auto type_name = "tenzir.chunk";

  ReadChunksArgs args_;
  series_builder builder_;
  SeriesPusher pusher_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.read_chunks";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadChunksArgs, ReadChunks>{};
    d.operator_location(&ReadChunksArgs::operator_location);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::read_chunks

TENZIR_REGISTER_PLUGIN(tenzir::plugins::read_chunks::plugin)
