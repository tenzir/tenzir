//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"

#include <tenzir/detail/posix.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/substitute_ctx.hpp>

#include <folly/coro/Baton.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncPipe.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/net/NetworkSocket.h>

#include <fcntl.h>
#include <string>
#include <unistd.h>

namespace tenzir::plugins::to_stdout {

namespace {

constexpr auto error_queue_capacity = uint32_t{1};

struct ToStdoutArgs {
  location self;
  located<ir::pipeline> pipe;
};

class StdoutFlagsGuard {
public:
  StdoutFlagsGuard() = default;
  StdoutFlagsGuard(StdoutFlagsGuard const&) = delete;
  auto operator=(StdoutFlagsGuard const&) -> StdoutFlagsGuard& = delete;
  StdoutFlagsGuard(StdoutFlagsGuard&& other) noexcept
    : orig_flags_{std::exchange(other.orig_flags_, -1)} {
  }
  auto operator=(StdoutFlagsGuard&& other) noexcept -> StdoutFlagsGuard& {
    if (this != &other) {
      restore();
      orig_flags_ = std::exchange(other.orig_flags_, -1);
    }
    return *this;
  }
  ~StdoutFlagsGuard() {
    restore();
  }

  auto activate(int orig_flags) -> void {
    TENZIR_ASSERT(orig_flags_ < 0);
    orig_flags_ = orig_flags;
  }

  auto restore() -> void {
    if (orig_flags_ >= 0) {
      ::fcntl(STDOUT_FILENO, F_SETFL, orig_flags_);
      orig_flags_ = -1;
    }
  }

private:
  int orig_flags_ = -1;
};

auto write_chunk(folly::AsyncPipeWriter& writer, chunk_ptr chunk)
  -> Task<Option<std::string>> {
  class WriteCallback final : public folly::AsyncWriter::WriteCallback {
  public:
    WriteCallback(folly::coro::Baton& baton, Option<std::string>& error)
      : baton_{baton}, error_{error} {
    }

    void writeSuccess() noexcept override {
      baton_.post();
      delete this;
    }

    void writeErr(size_t,
                  folly::AsyncSocketException const& ex) noexcept override {
      error_ = ex.what();
      baton_.post();
      delete this;
    }

  private:
    folly::coro::Baton& baton_;
    Option<std::string>& error_;
  };

  TENZIR_ASSERT(chunk);
  auto baton = folly::coro::Baton{};
  auto error = Option<std::string>{};
  writer.write(folly::IOBuf::copyBuffer(chunk->data(), chunk->size()),
               new WriteCallback{baton, error});
  co_await baton;
  co_return error;
}

class ToStdout final : public Operator<table_slice, void> {
public:
  explicit ToStdout(ToStdoutArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto pipe = std::move(args_.pipe.inner);
    if (not pipe.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      diagnostic::error("failed to substitute pipeline")
        .primary(args_.pipe)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto orig_flags = ::fcntl(STDOUT_FILENO, F_GETFL, 0);
    if (orig_flags < 0) {
      diagnostic::error("failed to get stdout flags: {}",
                        detail::describe_errno())
        .primary(args_.self)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    if (::fcntl(STDOUT_FILENO, F_SETFL, orig_flags | O_NONBLOCK) < 0) {
      diagnostic::error("failed to enable non-blocking mode for stdout: {}",
                        detail::describe_errno())
        .primary(args_.self)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    stdout_flags_.activate(orig_flags);
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    writer_ = folly::AsyncPipeWriter::newWriter(
      evb_, folly::NetworkSocket::fromFd(STDOUT_FILENO));
    // Avoid closing the process-global stdout when the writer shuts down.
    writer_->setCloseCallback([](folly::NetworkSocket) {});
    co_await ctx.spawn_sub<table_slice>(caf::none, std::move(pipe));
    TENZIR_ASSERT(ctx.get_sub(caf::none));
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    auto* error = result.try_as<std::string>();
    TENZIR_ASSERT(error);
    diagnostic::error("failed to write to stdout: {}", *error)
      .primary(args_.self)
      .emit(ctx);
    done_ = true;
    co_return;
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (done_) {
      co_return;
    }
    auto sub = ctx.get_sub(caf::none);
    if (not sub) {
      done_ = true;
      co_return;
    }
    auto& pipeline = as<SubHandle<table_slice>>(*sub);
    auto push_result = co_await pipeline.push(std::move(input));
    if (push_result.is_err()) {
      done_ = true;
    }
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (done_ or not chunk or chunk->size() == 0 or not writer_) {
      co_return;
    }
    auto error = co_await folly::coro::co_withExecutor(
      evb_, write_chunk(*writer_, std::move(chunk)));
    if (error) {
      auto write_error = std::move(*error);
      std::ignore = queue_->try_enqueue(std::move(write_error));
    }
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (auto sub = ctx.get_sub(caf::none)) {
      auto& pipeline = as<SubHandle<table_slice>>(*sub);
      co_await pipeline.close();
      co_return FinalizeBehavior::continue_;
    }
    writer_.reset();
    stdout_flags_.restore();
    co_return FinalizeBehavior::done;
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    writer_.reset();
    stdout_flags_.restore();
    done_ = true;
    co_return;
  }

private:
  using ErrorQueue = folly::coro::BoundedQueue<std::string>;

  ToStdoutArgs args_;
  folly::EventBase* evb_ = nullptr;
  StdoutFlagsGuard stdout_flags_;
  folly::AsyncPipeWriter::UniquePtr writer_;
  // `process_sub()` reports the first stdout write error back to the main
  // loop, which owns the operator lifecycle state.
  mutable Box<ErrorQueue> queue_{std::in_place, error_queue_capacity};
  bool done_ = false;
};

class ToStdoutPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "to_stdout";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToStdoutArgs, ToStdout>{};
    d.operator_location(&ToStdoutArgs::self);
    auto pipe_arg = d.pipeline(&ToStdoutArgs::pipe);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto pipe, ctx.get(pipe_arg));
      auto output = pipe.inner.infer_type(tag_v<table_slice>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (not *output or (*output)->is_not<chunk_ptr>()) {
        diagnostic::error("pipeline must return bytes")
          .primary(pipe.source.subloc(0, 1))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::to_stdout

TENZIR_REGISTER_PLUGIN(tenzir::plugins::to_stdout::ToStdoutPlugin)
