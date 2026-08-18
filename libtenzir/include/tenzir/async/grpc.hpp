//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async/semaphore.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/box.hpp"
#include "tenzir/option.hpp"

#include <grpcpp/server.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/server_callback.h>

#include <chrono>
#include <functional>
#include <memory>

namespace tenzir {

/// State shared by a gRPC unary callback and its asynchronous consumer.
///
/// Completion is exactly once. The optional concurrency permit remains held
/// until gRPC invokes `OnDone`, rather than merely until `finish` starts
/// completion.
class GrpcServerCall {
public:
  struct Handle {
    Arc<GrpcServerCall> call;
    grpc::ServerUnaryReactor* reactor;
    bool admitted;
  };

  /// Creates a call and its callback reactor.
  ///
  /// The call attempts to acquire exactly one permit. An admitted call retains
  /// the permit until gRPC invokes `OnDone`.
  static auto make(grpc::CallbackServerContext& context,
                   Arc<Semaphore> active_requests_limit) -> Handle;

  GrpcServerCall(GrpcServerCall const&) = delete;
  GrpcServerCall(GrpcServerCall&&) = delete;
  auto operator=(GrpcServerCall const&) -> GrpcServerCall& = delete;
  auto operator=(GrpcServerCall&&) -> GrpcServerCall& = delete;

  /// Starts call completion unless another path completed it first.
  auto finish(grpc::Status status) -> bool;

  /// Returns whether call completion has started.
  auto finished() const -> bool;

private:
  class Reactor;

  GrpcServerCall(grpc::CallbackServerContext& context,
                 Arc<Semaphore> active_requests_limit);

  auto attach(Reactor& reactor) -> void;
  auto release_permit() -> void;
  auto cancel() -> void;

  grpc::CallbackServerContext& context_;
  // SemaphorePermit is non-owning, so retain its semaphore through OnDone.
  Arc<Semaphore> active_requests_limit_;
  Option<SemaphorePermit> permit_;
  Reactor* reactor_ = nullptr;
  // gRPC cancellation callbacks and the asynchronous consumer may race to
  // complete a call. This is the only state they share.
  Atomic<bool> finished_{false};
};

/// Owns the blocking shutdown lifecycle of a running gRPC server.
///
/// `shutdown` transfers the server to a detached worker that calls
/// `Server::Shutdown` and `Server::Wait`. The optional callback runs on that
/// worker after the server stopped. Destruction initiates immediate shutdown
/// when needed and never waits for the worker.
///
/// Access is thread-confined: one coordinating thread must own the handle.
class GrpcServerHandle {
public:
  explicit GrpcServerHandle(std::unique_ptr<grpc::Server> server,
                            std::function<void()> on_stopped = {});
  ~GrpcServerHandle();

  GrpcServerHandle(GrpcServerHandle const&) = delete;
  GrpcServerHandle(GrpcServerHandle&&) = delete;
  auto operator=(GrpcServerHandle const&) -> GrpcServerHandle& = delete;
  auto operator=(GrpcServerHandle&&) -> GrpcServerHandle& = delete;

  /// Starts graceful shutdown with a steady-clock deadline.
  auto shutdown(std::chrono::steady_clock::time_point deadline) -> void;

private:
  Option<Box<grpc::Server>> server_;
  std::function<void()> on_stopped_;
};

} // namespace tenzir
