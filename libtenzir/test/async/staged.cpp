//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/staged.hpp"

#include "tenzir/async/channel.hpp"

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/executors/GlobalExecutor.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

#include <set>
#include <vector>

namespace tenzir {

namespace {

template <class T>
class SenderPush final : public Push<OperatorMsg<T>> {
public:
  explicit SenderPush(Sender<OperatorMsg<T>> sender)
    : sender_{std::move(sender)} {
  }

  auto operator()(OperatorMsg<T> x) -> Task<void> override {
    return sender_.send(std::move(x));
  }

private:
  Sender<OperatorMsg<T>> sender_;
};

template <class T>
class ReceiverPull final : public Pull<OperatorMsg<T>> {
public:
  explicit ReceiverPull(Receiver<OperatorMsg<T>> receiver)
    : receiver_{std::move(receiver)} {
  }

  auto operator()() -> Task<Option<OperatorMsg<T>>> override {
    return receiver_.recv();
  }

private:
  Receiver<OperatorMsg<T>> receiver_;
};

auto factory() {
  return [](ChannelId = ChannelId{}) {
    auto [sender, receiver] = channel<OperatorMsg<int>>(128);
    return PushPull<OperatorMsg<int>>{
      Box<Push<OperatorMsg<int>>>{SenderPush<int>{std::move(sender)}},
      Box<Pull<OperatorMsg<int>>>{ReceiverPull<int>{std::move(receiver)}},
    };
  };
}

// A lane that applies `f` to each data element and forwards signals.
template <class F>
auto map_lane(Box<Pull<OperatorMsg<int>>> input,
              Box<Push<OperatorMsg<int>>> output, F f) -> Task<void> {
  while (auto msg = co_await (*input)()) {
    co_await co_match(
      std::move(*msg),
      [&](int value) -> Task<void> {
        co_await (*output)(OperatorMsg<int>{f(value)});
      },
      [&](Signal signal) -> Task<void> {
        co_await (*output)(OperatorMsg<int>{signal});
      });
  }
  // Drop `output` to close the edge downstream.
  output = {};
}

template <class G>
auto run(G&& g) -> void {
  folly::coro::blockingWait(folly::coro::co_withExecutor(
    folly::getGlobalCPUExecutor(), std::forward<G>(g)()));
}

// Feeds `values` then an end-of-data into `push`, then drops it.
auto feed(Box<Push<OperatorMsg<int>>> push, std::vector<int> values)
  -> Task<void> {
  for (auto value : values) {
    co_await (*push)(OperatorMsg<int>{value});
  }
  push = {};
}

// Drains `pull` into a vector of data values, ignoring signals.
auto drain(Box<Pull<OperatorMsg<int>>> pull) -> Task<std::vector<int>> {
  auto result = std::vector<int>{};
  while (auto msg = co_await (*pull)()) {
    if (auto* value = try_as<int>(*msg)) {
      result.push_back(*value);
    }
  }
  co_return result;
}

} // namespace

TEST("flat plan runs a serial chain") {
  run([&]() -> Task<void> {
    auto in = factory()();
    auto out = factory()();
    // A single stage of three doubling operators.
    auto plan = PhysicalPlan::flat(3);
    auto values = std::vector<int>{};
    co_await folly::coro::collectAll(
      run_stages<int>(
        plan, std::move(in.pull), std::move(out.push),
        [](size_t, size_t, Box<Pull<OperatorMsg<int>>> input,
           Box<Push<OperatorMsg<int>>> output) -> Task<void> {
          return map_lane(std::move(input), std::move(output), [](int x) {
            return x + 1;
          });
        },
        factory(), PipeId{"flat"}),
      feed(std::move(in.push), {1, 2, 3}), [&]() -> Task<void> {
        values = co_await drain(std::move(out.pull));
      }());
    // Three lanes in one serial stage means each value passes through the same
    // single lane once; with a flat plan there is exactly one lane, so `+1`.
    check_eq(values, (std::vector<int>{2, 3, 4}));
  });
}

TEST("widened stage scatters and gathers preserving multiset") {
  run([&]() -> Task<void> {
    auto in = factory()();
    auto out = factory()();
    // Serial source stage, widened middle stage (degree 4), serial sink stage.
    auto plan = PhysicalPlan::from_plan(plan_result{
      .stages = {
        {.begin = 0, .end = 1, .degree = 1, .distribution = SingleDistribution{}},
        {.begin = 1, .end = 2, .degree = 4, .distribution = AnyDistribution{}},
        {.begin = 2, .end = 3, .degree = 1, .distribution = SingleDistribution{}},
      },
    });
    auto values = std::multiset<int>{};
    auto input = std::vector<int>{};
    for (auto i = 0; i < 100; ++i) {
      input.push_back(i);
    }
    co_await folly::coro::collectAll(
      run_stages<int>(
        plan, std::move(in.pull), std::move(out.push),
        [](size_t, size_t, Box<Pull<OperatorMsg<int>>> in,
           Box<Push<OperatorMsg<int>>> out) -> Task<void> {
          return map_lane(std::move(in), std::move(out), [](int x) {
            return x * 10;
          });
        },
        factory(), PipeId{"wide"}),
      feed(std::move(in.push), input), [&]() -> Task<void> {
        auto drained = co_await drain(std::move(out.pull));
        values = std::multiset<int>(drained.begin(), drained.end());
      }());
    // Each of the three stages multiplies by 10, so 0..99 becomes 0,1000,..
    auto expected = std::multiset<int>{};
    for (auto i = 0; i < 100; ++i) {
      expected.insert(i * 1000);
    }
    check(values == expected);
    check_eq(values.size(), size_t{100});
  });
}

} // namespace tenzir
