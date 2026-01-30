//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/error.hpp>

#include <caf/expected.hpp>
#include <fmt/format.h>
#include <folly/Unit.h>
#include <folly/experimental/coro/SemiFutureAwaitable.h>
#include <folly/experimental/coro/Task.h>
#include <folly/futures/Promise.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>
#include <folly/portability/Fcntl.h>
#include <folly/portability/Unistd.h>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>

#if defined(__linux__)
#  include <sys/eventfd.h>
#endif

#if FOLLY_HAS_COROUTINES

namespace tenzir::plugins::kafka {

namespace detail {

struct wakeup_fd {
  int read = -1;
  int write = -1;
};

inline caf::error posix_error(std::string_view operation, int err) {
  const auto code = std::error_code(err, std::generic_category());
  return caf::make_error(ec::unspecified,
                         fmt::format("{}: {}", operation, code.message()));
}

inline caf::expected<wakeup_fd> make_wakeup_fd() {
#  if defined(__linux__)
  const auto fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (fd == -1) {
    return posix_error("eventfd", errno);
  }
  return wakeup_fd{fd, -1};
#  else
  int fds[2] = {-1, -1};
  if (::pipe(fds) == -1) {
    return posix_error("pipe", errno);
  }
  auto set_non_blocking = [](int fd) -> caf::error {
    auto flags = ::fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
      return posix_error("fcntl(F_GETFL)", errno);
    }
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
      return posix_error("fcntl(F_SETFL)", errno);
    }
    if (::fcntl(fd, F_SETFD, FD_CLOEXEC) == -1) {
      return posix_error("fcntl(F_SETFD)", errno);
    }
    return caf::error{};
  };
  if (auto err = set_non_blocking(fds[0])) {
    ::close(fds[0]);
    ::close(fds[1]);
    return err;
  }
  if (auto err = set_non_blocking(fds[1])) {
    ::close(fds[0]);
    ::close(fds[1]);
    return err;
  }
  return wakeup_fd{fds[0], fds[1]};
#  endif
}

inline void close_wakeup_fd(const wakeup_fd& fd) {
  if (fd.read != -1) {
    ::close(fd.read);
  }
  if (fd.write != -1) {
    ::close(fd.write);
  }
}

inline void drain_fd(int fd) noexcept {
  if (fd == -1) {
    return;
  }
  std::uint8_t buffer[64];
  for (;;) {
    const auto n = ::read(fd, buffer, sizeof(buffer));
    if (n > 0) {
      continue;
    }
    if (n == 0) {
      break;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      break;
    }
    break;
  }
}

} // namespace detail

/// Integrates librdkafka's I/O events with folly coroutines.
///
/// The class forwards wakeups from the consumer queue to a folly::EventBase
/// and exposes a coroutine interface that returns the next message without
/// blocking the event loop.
class async_consumer_queue final : private folly::EventHandler {
public:
  /// Creates an asynchronous queue helper for the given consumer.
  ///
  /// The caller must invoke this on the same thread that owns `event_base`.
  static caf::expected<std::unique_ptr<async_consumer_queue>>
  make(folly::EventBase& event_base, RdKafka::KafkaConsumer& consumer) {
    const auto client = consumer.c_ptr();
    if (client == nullptr) {
      return caf::make_error(ec::unspecified,
                             "kafka consumer is missing underlying handle");
    }
    auto* queue = rd_kafka_queue_get_consumer(client);
    if (queue == nullptr) {
      return caf::make_error(ec::unspecified,
                             "failed to acquire consumer queue");
    }
    auto fds = detail::make_wakeup_fd();
    if (! fds) {
      rd_kafka_queue_destroy(queue);
      return std::move(fds.error());
    }
    auto result = std::unique_ptr<async_consumer_queue>{
      new async_consumer_queue(event_base, consumer, client, queue, *fds)};
    return result;
  }

  ~async_consumer_queue() override {
    request_stop();
    unregisterHandler();
    disable_events();
    detail::close_wakeup_fd(wakeup_fd_);
    if (queue_ != nullptr) {
      rd_kafka_queue_destroy(queue_);
    }
  }

  async_consumer_queue(const async_consumer_queue&) = delete;
  async_consumer_queue& operator=(const async_consumer_queue&) = delete;
  async_consumer_queue(async_consumer_queue&&) = delete;
  async_consumer_queue& operator=(async_consumer_queue&&) = delete;

  /// Retrieves the next message without blocking the calling thread.
  ///
  /// When the queue is stopped or shut down this returns nullptr.
  [[nodiscard]] folly::coro::Task<std::unique_ptr<RdKafka::Message>> next() {
    while (! stopped_) {
      if (auto message = try_consume()) {
        co_return message;
      }
      co_await wait_for_notification();
      if (stopped_) {
        break;
      }
    }
    co_return nullptr;
  }

  /// Requests shutdown and releases outstanding waiters.
  void request_stop() {
    if (stopped_) {
      return;
    }
    stopped_ = true;
    if (waiter_) {
      auto promise = std::move(*waiter_);
      waiter_.reset();
      promise.setValue(folly::Unit{});
    }
    disable_events();
  }

private:
  async_consumer_queue(folly::EventBase& event_base,
                       RdKafka::KafkaConsumer& consumer, rd_kafka_t* client,
                       rd_kafka_queue_t* queue, detail::wakeup_fd wakeup_fd)
    : folly::EventHandler(&event_base),
      consumer_{&consumer},
      client_{client},
      queue_{queue},
      wakeup_fd_{wakeup_fd} {
    static constexpr std::uint64_t token = 1;
    rd_kafka_queue_io_event_enable(queue_, wakeup_fd_.read, &token,
                                   sizeof(token));
    registerHandler(EventHandler::READ | EventHandler::PERSIST);
  }

  void handlerReady(uint16_t events) noexcept override {
    if ((events & EventHandler::READ) == 0) {
      return;
    }
    detail::drain_fd(wakeup_fd_.read);
    rd_kafka_poll(client_, 0);
    if (stopped_) {
      return;
    }
    if (waiter_) {
      auto promise = std::move(*waiter_);
      waiter_.reset();
      promise.setValue(folly::Unit{});
    } else {
      ++pending_notifications_;
    }
  }

  folly::coro::Task<void> wait_for_notification() {
    while (! stopped_) {
      if (pending_notifications_ > 0) {
        --pending_notifications_;
        co_return;
      }
      waiter_.emplace();
      auto future = waiter_->getSemiFuture();
      co_await std::move(future);
      // The waiter may have been satisfied by a stop request.
      if (! waiter_) {
        co_return;
      }
      waiter_.reset();
      co_return;
    }
    co_return;
  }

  [[nodiscard]] std::unique_ptr<RdKafka::Message> try_consume() {
    auto* message = consumer_->consume(0);
    if (! message) {
      return nullptr;
    }
    std::unique_ptr<RdKafka::Message> guard{message};
    if (message->err() == RdKafka::ERR__TIMED_OUT) {
      guard.reset();
      return nullptr;
    }
    return guard;
  }

  void disable_events() noexcept {
    if (queue_ != nullptr) {
      rd_kafka_queue_io_event_enable(queue_, -1, nullptr, 0);
    }
  }

  RdKafka::KafkaConsumer* consumer_ = nullptr;
  rd_kafka_t* client_ = nullptr;
  rd_kafka_queue_t* queue_ = nullptr;
  detail::wakeup_fd wakeup_fd_{};
  bool stopped_ = false;
  std::size_t pending_notifications_ = 0;
  std::optional<folly::Promise<folly::Unit>> waiter_;
};

} // namespace tenzir::plugins::kafka

#endif // FOLLY_HAS_COROUTINES
