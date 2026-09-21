//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/as_bytes.hpp>
#include <tenzir/async/pusher.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>

namespace tenzir::plugins::read_chunks {

namespace {

struct ReadChunksArgs {
  location operator_location;
};

template <class Output>
class ReadChunks final : public Operator<chunk_ptr, Output> {
  using Builder
    = std::conditional_t<std::same_as<Output, nova::Events>,
                         nova::ArrayBuilder<nova::Record>, series_builder>;

public:
  explicit ReadChunks(ReadChunksArgs args) : args_{args} {
  }

  auto process(chunk_ptr input, Push<Output>& push, OpCtx&)
    -> Task<void> override {
    builder_.record().field("data").data(blob_view{as_bytes(input)});
    co_await push_ready(push);
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if constexpr (std::same_as<Output, nova::Events>) {
      co_await timeout_.wait();
    } else {
      co_await pusher_.wait();
    }
    co_return {};
  }

  auto process_task(Any, Push<Output>& push, OpCtx&) -> Task<void> override {
    co_await push_ready(push);
  }

  auto finalize(Push<Output>& push, OpCtx&) -> Task<FinalizeBehavior> override {
    co_await flush(push);
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<Output>& push, OpCtx&) -> Task<void> override {
    co_await flush(push);
  }

private:
  auto push_ready(Push<Output>& push) -> Task<void> {
    if constexpr (std::same_as<Output, nova::Events>) {
      if (static_cast<uint64_t>(builder_.length())
            >= defaults::import::table_slice_size
          or timeout_.poll(builder_.length())) {
        co_await flush(push);
      }
    } else {
      co_await pusher_.push(builder_.yield_ready(type_name), push);
    }
  }

  auto flush(Push<Output>& push) -> Task<void> {
    if (builder_.length() == 0) {
      co_return;
    }
    if constexpr (std::same_as<Output, nova::Events>) {
      auto data = builder_.finish();
      builder_ = Builder{};
      auto length = data.length();
      timeout_.reset();
      co_await push(
        nova::Events{std::move(data), nova::storage::BitMap{length, true},
                     nova::Events::Meta::make_empty(length, type_name)});
    } else {
      co_await push(builder_.finish_assert_one_slice(type_name));
    }
  }

  constexpr static auto type_name = "tenzir.chunk";

  ReadChunksArgs args_;
  Builder builder_;
  SeriesPusher pusher_;
  BatchTimeout timeout_{defaults::import::batch_timeout};
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_chunks";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadChunksArgs, ReadChunks<table_slice>,
                       ReadChunks<nova::Events>>{};
    d.operator_location(&ReadChunksArgs::operator_location);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::read_chunks

TENZIR_REGISTER_PLUGIN(tenzir::plugins::read_chunks::plugin)
