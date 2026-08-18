//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/grpc.hpp"

#include "tenzir/detail/assert.hpp"

#include <algorithm>
#include <thread>
#include <utility>

namespace tenzir {

class GrpcServerCall::Reactor final : public grpc::ServerUnaryReactor {
public:
  explicit Reactor(Arc<GrpcServerCall> call) : call_{std::move(call)} {
  }

  void OnCancel() override {
    call_->cancel();
  }

  void OnDone() override {
    // Finish only starts completion; the RPC remains active until OnDone.
    call_->release_permit();
    delete this;
  }

private:
  Arc<GrpcServerCall> call_;
};

GrpcServerCall::GrpcServerCall(grpc::CallbackServerContext& context,
                               Arc<Semaphore> active_requests_limit)
  : context_{context},
    active_requests_limit_{std::move(active_requests_limit)},
    permit_{active_requests_limit_->try_acquire()} {
}

auto GrpcServerCall::make(grpc::CallbackServerContext& context,
                          Arc<Semaphore> active_requests_limit) -> Handle {
  auto call
    = Arc<GrpcServerCall>::from_non_null(std::unique_ptr<GrpcServerCall>{
      new GrpcServerCall{context, std::move(active_requests_limit)}});
  auto* reactor = new Reactor{call};
  call->attach(*reactor);
  auto const admitted = call->permit_.has_value();
  return {
    .call = std::move(call),
    .reactor = reactor,
    .admitted = admitted,
  };
}

auto GrpcServerCall::finish(grpc::Status status) -> bool {
  if (finished_.exchange(true, std::memory_order_acq_rel)) {
    return false;
  }
  TENZIR_ASSERT(reactor_);
  reactor_->Finish(std::move(status));
  return true;
}

auto GrpcServerCall::finished() const -> bool {
  return finished_.load(std::memory_order_acquire);
}

auto GrpcServerCall::attach(Reactor& reactor) -> void {
  TENZIR_ASSERT(not reactor_);
  reactor_ = &reactor;
}

auto GrpcServerCall::release_permit() -> void {
  permit_ = None{};
}

auto GrpcServerCall::cancel() -> void {
  auto const deadline_elapsed
    = std::chrono::system_clock::now() >= context_.deadline();
  auto const code = deadline_elapsed ? grpc::StatusCode::DEADLINE_EXCEEDED
                                     : grpc::StatusCode::CANCELLED;
  std::ignore = finish(grpc::Status{code, deadline_elapsed
                                            ? "gRPC request deadline exceeded"
                                            : "gRPC request cancelled"});
}

GrpcServerHandle::GrpcServerHandle(std::unique_ptr<grpc::Server> server,
                                   std::function<void()> on_stopped)
  : server_{std::in_place, Box<grpc::Server>::from_non_null(std::move(server))},
    on_stopped_{std::move(on_stopped)} {
}

GrpcServerHandle::~GrpcServerHandle() {
  shutdown(std::chrono::steady_clock::now());
}

auto GrpcServerHandle::shutdown(std::chrono::steady_clock::time_point deadline)
  -> void {
  if (not server_) {
    return;
  }
  auto const remaining = std::max(deadline - std::chrono::steady_clock::now(),
                                  std::chrono::steady_clock::duration::zero());
  auto const system_deadline
    = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
      std::chrono::system_clock::now() + remaining);
  auto server = std::exchange(server_, None{});
  auto on_stopped = std::exchange(on_stopped_, {});
  std::thread{[server = std::move(server), on_stopped = std::move(on_stopped),
               system_deadline]() mutable {
    (*server)->Shutdown(system_deadline);
    (*server)->Wait();
    server = None{};
    if (on_stopped) {
      on_stopped();
    }
  }}.detach();
}

} // namespace tenzir
