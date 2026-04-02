//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/as_bytes.hpp"
#include "tenzir/async.hpp"
#include "tenzir/async/notify.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/substitute_ctx.hpp"

#include <folly/ScopeGuard.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/ViaIfAsync.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncPipe.h>
#include <folly/net/NetworkSocket.h>

#include <cerrno>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <memory>
#include <unistd.h>
#include <utility>
#include <vector>

namespace tenzir::plugins::from_stdin {

namespace {

constexpr auto buffer_size = size_t{1} << 20; // 1 MiB
constexpr auto queue_capacity = uint32_t{16};

using ChunkQueue = folly::coro::BoundedQueue<chunk_ptr>;

struct FromStdinArgs {
  located<ir::pipeline> pipe;
};

struct ReadCB : folly::AsyncReader::ReadCallback {
  // NOTE: The callback may be triggered multiple times before the read loop
  // resumes, so we queue all chunks instead of keeping only the latest one.
  std::deque<chunk_ptr> chunks;
  Notify notify;
  bool done = false;

  auto isBufferMovable() noexcept -> bool override {
    return true;
  }

  auto maxBufferSize() const -> size_t override {
    return buffer_size;
  }

  void
  readBufferAvailable(std::unique_ptr<folly::IOBuf> buf) noexcept override {
    auto range = buf->coalesce();
    auto view = as_bytes(range);
    chunks.push_back(chunk::make(view, [buf = std::move(buf)]() noexcept {}));
    notify.notify_one();
  }

  void getReadBuffer(void**, size_t*) noexcept override {
    TENZIR_ASSERT(false, "unreachable with isBufferMovable=true");
  }

  void readDataAvailable(size_t) noexcept override {
    TENZIR_ASSERT(false, "unreachable with isBufferMovable=true");
  }

  void readEOF() noexcept override {
    done = true;
    notify.notify_one();
  }

  void readErr(folly::AsyncSocketException const&) noexcept override {
    done = true;
    notify.notify_one();
  }
};

class FromStdin final : public Operator<void, table_slice> {
public:
  explicit FromStdin(FromStdinArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto pipe = args_.pipe.inner;
    if (not pipe.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      diagnostic::error("failed to substitute pipeline")
        .primary(args_.pipe)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    co_await ctx.spawn_sub<chunk_ptr>(caf::none, std::move(pipe));
    ctx.spawn_task(folly::coro::co_withExecutor(
      ctx.io_executor(), read_stdin(chunk_queue_, ctx.dh())));
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
    }
    if (stdin_closed_) {
      // Wait for the sub-pipeline to flush all remaining output.
      co_await sub_finished_->wait();
      co_return chunk_ptr{};
    }
    co_return co_await chunk_queue_->dequeue();
  }

  auto process_task(Any result, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_ASSERT(result.has_value());
    auto sub = ctx.get_sub(caf::none);
    if (not sub) {
      done_ = true;
      co_return;
    }
    auto chunk = std::move(result).as<chunk_ptr>();
    if (not chunk) {
      if (stdin_closed_) {
        // Sub-pipeline finished flushing.
        done_ = true;
        co_return;
      }
      auto& pipeline = as<SubHandle<chunk_ptr>>(*sub);
      co_await pipeline.close();
      stdin_closed_ = true;
      co_return;
    }
    auto& pipeline = as<SubHandle<chunk_ptr>>(*sub);
    auto push_result = co_await pipeline.push(std::move(chunk));
    if (push_result.is_err()) {
      done_ = true;
      co_return;
    }
  }

  auto finish_sub(SubKeyView, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    sub_finished_->notify_one();
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  static auto read_stdin(std::shared_ptr<ChunkQueue> queue,
                         diagnostic_handler& dh) -> Task<void> {
    const auto fail = [&](auto&& emit) -> Task<void> {
      emit();
      co_await queue->enqueue(chunk_ptr{});
    };
    // Set stdin to non-blocking for event-driven IO.
    auto orig_flags = ::fcntl(STDIN_FILENO, F_GETFL, 0);
    if (orig_flags < 0) {
      auto err = errno;
      co_await fail([&] {
        diagnostic::error("failed to get stdin flags")
          .note("errno {}: {}", err, std::strerror(err))
          .emit(dh);
      });
      co_return;
    }
    auto rc = ::fcntl(STDIN_FILENO, F_SETFL, orig_flags | O_NONBLOCK);
    if (rc < 0) {
      auto err = errno;
      co_await fail([&] {
        diagnostic::error("failed to enable non-blocking mode")
          .note("errno {}: {}", err, std::strerror(err))
          .emit(dh);
      });
      co_return;
    }
    auto guard = folly::makeGuard([orig_flags] {
      ::fcntl(STDIN_FILENO, F_SETFL, orig_flags);
    });
    auto* evb = folly::getGlobalIOExecutor()->getEventBase();
    auto reader = folly::AsyncPipeReader::newReader(
      evb, folly::NetworkSocket::fromFd(STDIN_FILENO));
    // Prevent AsyncPipeReader cleanup from closing the process-global stdin.
    reader->setCloseCallback([](folly::NetworkSocket) {});
    auto cb = ReadCB{};
    reader->setReadCB(&cb);
    if (not reader->isHandlerRegistered()) {
      reader->setReadCB(nullptr);
      co_await fail([&] {
        diagnostic::error("stdin is not eventable")
          .note("from_stdin requires eventable stdin")
          .emit(dh);
      });
      co_return;
    }
    // The callback object lives on this coroutine frame, so unregister it on
    // every exit path before `cb` is destroyed.
    auto cb_guard = folly::makeGuard([&reader] {
      reader->setReadCB(nullptr);
    });
    while (true) {
      while (not cb.chunks.empty()) {
        auto item = std::move(cb.chunks.front());
        cb.chunks.pop_front();
        if (not queue->try_enqueue(std::move(item))) {
          // Queue is full — pause the reader to apply backpressure.
          reader->setReadCB(nullptr);
          co_await queue->enqueue(std::move(item));
          reader->setReadCB(&cb);
          if (not reader->isHandlerRegistered()) {
            reader->setReadCB(nullptr);
            co_await fail([&] {
              diagnostic::error("async stdin re-registration failed").emit(dh);
            });
            co_return;
          }
        }
      }
      if (cb.done) {
        break;
      }
      // Check again to avoid waiting if callback pushed while processing.
      if (cb.done or not cb.chunks.empty()) {
        continue;
      }
      co_await cb.notify.wait();
    }
    co_await queue->enqueue(chunk_ptr{});
  }

  FromStdinArgs args_;
  bool done_ = false;
  bool stdin_closed_ = false;
  mutable std::shared_ptr<Notify> sub_finished_ = std::make_shared<Notify>();
  mutable std::shared_ptr<ChunkQueue> chunk_queue_
    = std::make_shared<ChunkQueue>(queue_capacity);
};

class FromStdinPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_stdin";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromStdinArgs, FromStdin>{};
    auto pipe_arg = d.pipeline(&FromStdinArgs::pipe);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto pipe, ctx.get(pipe_arg));
      auto output = pipe.inner.infer_type(tag_v<chunk_ptr>, ctx);
      if (output.is_error()) {
        return {};
      }
      // XXX: When can output be nullopt?
      // FIXME: `spawn_sub` cannot spawn pipelines returning void.
      if (*output and (*output)->is_not<table_slice>()) {
        diagnostic::error("pipeline must return events")
          .primary(pipe.source.subloc(0, 1))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::from_stdin

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_stdin::FromStdinPlugin)
