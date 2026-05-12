//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/mail.hpp"

#include "tenzir/test/test.hpp"

#include <caf/actor_from_state.hpp>
#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/send.hpp>
#include <caf/typed_response_promise.hpp>
#include <folly/CancellationToken.h>
#include <folly/OperationCancelled.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/WithCancellation.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <thread>

namespace tenzir {

namespace {

using MailTestActor = caf::typed_actor<caf::result<int32_t>(int32_t)>;

struct DelayedResponderControl {
  std::promise<void> received;
};

struct DelayedResponderState {
  [[maybe_unused]] static constexpr auto name = "delayed-mail-responder";

  DelayedResponderState(MailTestActor::pointer self,
                        std::shared_ptr<DelayedResponderControl> control)
    : self{self}, control{std::move(control)} {
  }

  auto make_behavior() -> MailTestActor::behavior_type {
    return {
      [this](int32_t) -> caf::result<int32_t> {
        response = self->make_response_promise<int32_t>();
        control->received.set_value();
        return response;
      },
    };
  }

  MailTestActor::pointer self;
  std::shared_ptr<DelayedResponderControl> control;
  caf::typed_response_promise<int32_t> response;
};

struct TestSystem {
  caf::actor_system_config config;
  caf::actor_system system{config};

  TestSystem() = default;
  TestSystem(const TestSystem&) = delete;
  TestSystem(TestSystem&&) = delete;
  TestSystem& operator=(const TestSystem&) = delete;
  TestSystem& operator=(TestSystem&&) = delete;
  ~TestSystem() = default;
};

} // namespace

TEST("async_mail returns response") {
  auto ts = TestSystem{};
  auto& system = ts.system;
  auto responder
    = system.spawn([](MailTestActor::pointer) -> MailTestActor::behavior_type {
        return {
          [](int32_t value) -> caf::result<int32_t> {
            return value + 1;
          },
        };
      });
  auto result = folly::coro::blockingWait(
    async_mail(int32_t{41}).request(responder, caf::infinite));
  require(result.has_value());
  check_eq(*result, int32_t{42});
  caf::anon_send_exit(responder, caf::exit_reason::user_shutdown);
}

TEST("async_mail cancellation wakes without response") {
  using namespace std::chrono_literals;
  auto ts = TestSystem{};
  auto& system = ts.system;
  auto control = std::make_shared<DelayedResponderControl>();
  auto received = control->received.get_future();
  auto responder
    = system.spawn(caf::actor_from_state<DelayedResponderState>, control);
  auto cancel = folly::CancellationSource{};
  auto cancelled = std::atomic<bool>{false};
  auto unexpected = std::atomic<bool>{false};
  auto done = std::promise<void>{};
  auto done_future = done.get_future();
  auto waiter = std::thread{[&] {
    try {
      std::ignore = folly::coro::blockingWait(folly::coro::co_withCancellation(
        cancel.getToken(),
        async_mail(int32_t{42}).request(responder, caf::infinite)));
    } catch (folly::OperationCancelled const&) {
      cancelled.store(true, std::memory_order_relaxed);
    } catch (...) {
      unexpected.store(true, std::memory_order_relaxed);
    }
    done.set_value();
  }};
  check_eq(received.wait_for(1s), std::future_status::ready);
  cancel.requestCancellation();
  auto status = done_future.wait_for(250ms);
  check_eq(status, std::future_status::ready);
  caf::anon_send_exit(responder, caf::exit_reason::user_shutdown);
  if (status != std::future_status::ready) {
    check_eq(done_future.wait_for(1s), std::future_status::ready);
  }
  waiter.join();
  check(cancelled.load(std::memory_order_relaxed));
  check(not unexpected.load(std::memory_order_relaxed));
}

} // namespace tenzir
