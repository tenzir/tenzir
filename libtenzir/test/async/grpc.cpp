//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/grpc.hpp"

#include "tenzir/atomic.hpp"
#include "tenzir/test/test.hpp"

#include <grpcpp/server_builder.h>

#include <latch>
#include <thread>

using namespace tenzir;

namespace {

struct TestServer {
  TestServer() {
    auto builder = grpc::ServerBuilder{};
    completion_queue = builder.AddCompletionQueue();
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port);
    server = builder.BuildAndStart();
    if (server) {
      poller = std::thread{[this] {
        auto* tag = static_cast<void*>(nullptr);
        auto ok = false;
        while (completion_queue->Next(&tag, &ok)) {
        }
      }};
    }
  }

  ~TestServer() {
    completion_queue->Shutdown();
    if (poller.joinable()) {
      poller.join();
    }
  }

  std::unique_ptr<grpc::ServerCompletionQueue> completion_queue;
  std::unique_ptr<grpc::Server> server;
  std::thread poller;
  int port = 0;
};

} // namespace

TEST("gRPC server call holds its permit through OnDone") {
  auto permits = Arc<Semaphore>{std::in_place, 1u};
  auto first_context = grpc::CallbackServerContext{};
  auto first = GrpcServerCall::make(first_context, permits);
  CHECK(first.admitted);
  CHECK_EQUAL(permits->available_permits(), 0u);

  auto second_context = grpc::CallbackServerContext{};
  auto second = GrpcServerCall::make(second_context, permits);
  CHECK(not second.admitted);
  CHECK_EQUAL(permits->available_permits(), 0u);

  CHECK(first.call->finish(grpc::Status::OK));
  CHECK(not first.call->finish(grpc::Status::OK));
  CHECK_EQUAL(permits->available_permits(), 0u);
  first.reactor->OnDone();
  CHECK_EQUAL(permits->available_permits(), 1u);

  CHECK(second.call->finish(grpc::Status{grpc::StatusCode::UNAVAILABLE, {}}));
  second.reactor->OnDone();
  CHECK_EQUAL(permits->available_permits(), 1u);
}

TEST("gRPC server cancellation finishes a call exactly once") {
  auto permits = Arc<Semaphore>{std::in_place, 1u};
  auto context = grpc::CallbackServerContext{};
  auto handle = GrpcServerCall::make(context, permits);
  handle.reactor->OnCancel();
  CHECK(handle.call->finished());
  CHECK(not handle.call->finish(grpc::Status::OK));
  CHECK_EQUAL(permits->available_permits(), 0u);
  handle.reactor->OnDone();
  CHECK_EQUAL(permits->available_permits(), 1u);
}

TEST("gRPC server shutdown is one shot") {
  auto test_server = TestServer{};
  REQUIRE(test_server.server);
  REQUIRE_NOT_EQUAL(test_server.port, 0);
  auto stopped = std::latch{1};
  auto callbacks = Atomic<size_t>{0};
  {
    auto handle
      = GrpcServerHandle{std::move(test_server.server), [&] {
                           callbacks.fetch_add(1, std::memory_order_relaxed);
                           stopped.count_down();
                         }};
    auto const deadline
      = std::chrono::steady_clock::now() + std::chrono::seconds{1};
    handle.shutdown(deadline);
    handle.shutdown(deadline);
  }
  stopped.wait();
  CHECK_EQUAL(callbacks.load(std::memory_order_relaxed), 1u);
}

TEST("gRPC server handle initiates shutdown when dropped") {
  auto test_server = TestServer{};
  REQUIRE(test_server.server);
  REQUIRE_NOT_EQUAL(test_server.port, 0);
  auto stopped = std::latch{1};
  {
    auto handle = GrpcServerHandle{std::move(test_server.server), [&] {
                                     stopped.count_down();
                                   }};
  }
  stopped.wait();
}
