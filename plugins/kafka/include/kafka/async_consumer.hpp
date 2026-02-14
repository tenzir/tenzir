//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/async.hpp>
#include <tenzir/box.hpp>
#include <tenzir/logger.hpp>

#include <fmt/format.h>
#include <folly/Unit.h>
#include <folly/coro/Task.h>
#include <folly/futures/Future.h>
#include <folly/futures/Promise.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>
#include <folly/portability/Fcntl.h>
#include <folly/portability/Unistd.h>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#if defined(__linux__)
#  include <sys/eventfd.h>
#endif

namespace tenzir::plugins::kafka {

namespace detail {

/// File descriptors used to wake the queue waiter from librdkafka callbacks.
struct WakeupFd {
  int read = -1;
  int write = -1;
};

/// RAII wrapper for a raw librdkafka queue handle.
class QueueHandle {
public:
  static auto from_consumer(rd_kafka_t* client) -> QueueHandle {
    return QueueHandle{rd_kafka_queue_get_consumer(client)};
  }

  QueueHandle() = default;
  explicit QueueHandle(rd_kafka_queue_t* queue) : queue_{queue} {
  }

  ~QueueHandle() {
    reset();
  }

  QueueHandle(QueueHandle const&) = delete;
  auto operator=(QueueHandle const&) -> QueueHandle& = delete;

  QueueHandle(QueueHandle&& other) noexcept
    : queue_{std::exchange(other.queue_, nullptr)} {
  }

  auto operator=(QueueHandle&& other) noexcept -> QueueHandle& {
    if (this == &other) {
      return *this;
    }
    reset();
    queue_ = std::exchange(other.queue_, nullptr);
    return *this;
  }

  explicit operator bool() const {
    return queue_ != nullptr;
  }

  [[nodiscard]] auto get() const -> rd_kafka_queue_t* {
    return queue_;
  }

  [[nodiscard]] auto release() -> rd_kafka_queue_t* {
    return std::exchange(queue_, nullptr);
  }

private:
  auto reset() -> void {
    if (queue_ != nullptr) {
      rd_kafka_queue_destroy(queue_);
      queue_ = nullptr;
    }
  }

  rd_kafka_queue_t* queue_ = nullptr;
};

/// Formats a readable POSIX error string for diagnostics.
inline auto posix_error(std::string_view operation, int err) -> std::string {
  auto const code = std::error_code(err, std::generic_category());
  return fmt::format("{}: {}", operation, code.message());
}

/// Creates a non-blocking wakeup fd (eventfd on Linux, pipe elsewhere).
inline auto make_wakeup_fd() -> Result<WakeupFd, std::string> {
#if defined(__linux__)
  auto const fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (fd == -1) {
    return Err{posix_error("eventfd", errno)};
  }
  return WakeupFd{fd, fd};
#else
  int fds[2] = {-1, -1};
  if (::pipe(fds) == -1) {
    return Err{posix_error("pipe", errno)};
  }
  auto set_non_blocking = [](int fd) -> std::string {
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
    return std::string{};
  };
  if (auto err = set_non_blocking(fds[0]); not err.empty()) {
    ::close(fds[0]);
    ::close(fds[1]);
    return Err{std::move(err)};
  }
  if (auto err = set_non_blocking(fds[1]); not err.empty()) {
    ::close(fds[0]);
    ::close(fds[1]);
    return Err{std::move(err)};
  }
  return WakeupFd{fds[0], fds[1]};
#endif
}

/// Closes wakeup descriptors once; Linux eventfd uses the same descriptor twice.
inline auto close_wakeup_fd(WakeupFd const& fd) -> void {
  if (fd.read != -1 and fd.read == fd.write) {
    ::close(fd.read);
    return;
  }
  if (fd.read != -1) {
    ::close(fd.read);
  }
  if (fd.write != -1) {
    ::close(fd.write);
  }
}

/// Drains all pending bytes from the wakeup fd.
inline auto drain_fd(int fd) noexcept -> void {
  if (fd == -1) {
    return;
  }
  std::uint8_t buffer[64];
  for (;;) {
    auto const n = ::read(fd, buffer, sizeof(buffer));
    if (n > 0) {
      continue;
    }
    if (n == 0) {
      break;
    }
    if (errno == EAGAIN or errno == EWOULDBLOCK) {
      break;
    }
    break;
  }
}

} // namespace detail

/// Integrates librdkafka queue wakeups with folly coroutines.
class AsyncConsumerQueue final : private folly::EventHandler {
public:
  /// Result of waiting for the next message.
  struct NextResult {
    std::shared_ptr<RdKafka::Message> message;
    bool timed_out = false;
  };

  /// Returns how a queue-notification wait completed.
  enum class NotificationWaitResult { notified, timed_out, stopped };

  // Construction stays funneled through this factory: it performs fallible
  // setup (consumer handle lookup, queue acquisition, and wakeup-fd creation)
  // before constructing the object. Keeping the constructor non-public avoids
  // bypassing these checks and creating a partially initialized queue wrapper.
  static auto
  make(folly::EventBase& event_base, RdKafka::KafkaConsumer& consumer)
    -> Result<Box<AsyncConsumerQueue>, std::string> {
    auto const client = consumer.c_ptr();
    if (client == nullptr) {
      return Err{std::string{"kafka consumer is missing underlying handle"}};
    }
    auto queue = detail::QueueHandle::from_consumer(client);
    if (not queue) {
      return Err{std::string{"failed to acquire consumer queue"}};
    }
    auto fds = detail::make_wakeup_fd();
    if (fds.is_err()) {
      return Err{std::move(fds).unwrap_err()};
    }
    auto wakeup_fd = std::move(fds).unwrap();
    auto result = Box<AsyncConsumerQueue>::from_unique_ptr(
      std::unique_ptr<AsyncConsumerQueue>{new AsyncConsumerQueue(
        event_base, consumer, queue.release(), wakeup_fd)});
    return result;
  }

  ~AsyncConsumerQueue() override {
    request_stop();
    unregisterHandler();
    disable_events();
    detail::close_wakeup_fd(wakeup_fd_);
    if (queue_ != nullptr) {
      rd_kafka_queue_destroy(queue_);
    }
  }

  AsyncConsumerQueue(AsyncConsumerQueue const&) = delete;
  auto operator=(AsyncConsumerQueue const&) -> AsyncConsumerQueue& = delete;
  AsyncConsumerQueue(AsyncConsumerQueue&&) = delete;
  auto operator=(AsyncConsumerQueue&&) -> AsyncConsumerQueue& = delete;

  /// Retrieves the next message without blocking the caller thread.
  [[nodiscard]] auto next(std::optional<std::chrono::milliseconds> timeout
                          = std::nullopt) -> folly::coro::Task<NextResult> {
    while (not is_stopped()) {
      auto token = co_await folly::coro::co_current_cancellation_token;
      if (token.isCancellationRequested()) {
        request_stop();
        break;
      }
      if (auto message = try_consume()) {
        co_return NextResult{.message = std::move(message)};
      }
      auto wait_result = co_await wait_for_notification(timeout);
      if (wait_result == NotificationWaitResult::timed_out) {
        co_return NextResult{.message = nullptr, .timed_out = true};
      }
      if (wait_result == NotificationWaitResult::stopped) {
        break;
      }
    }
    co_return NextResult{.message = nullptr, .timed_out = false};
  }

  auto request_stop() -> void {
    auto promise = std::optional<folly::Promise<folly::Unit>>{};
    {
      auto guard = std::scoped_lock{state_mutex_};
      if (stopped_) {
        return;
      }
      stopped_ = true;
      if (waiter_) {
        promise = std::move(waiter_);
        waiter_.reset();
      }
    }
    if (promise) {
      promise->setValue(folly::Unit{});
    }
    disable_events();
  }

private:
  AsyncConsumerQueue(folly::EventBase& event_base,
                     RdKafka::KafkaConsumer& consumer, rd_kafka_queue_t* queue,
                     detail::WakeupFd wakeup_fd)
    : folly::EventHandler(&event_base),
      consumer_{&consumer},
      queue_{queue},
      wakeup_fd_{wakeup_fd} {
    static constexpr auto token = std::uint64_t{1};
    rd_kafka_queue_io_event_enable(queue_, wakeup_fd_.write, &token,
                                   sizeof(token));
    initHandler(&event_base, folly::NetworkSocket::fromFd(wakeup_fd_.read));
    registerHandler(EventHandler::READ | EventHandler::PERSIST);
  }

  auto handlerReady(uint16_t events) noexcept -> void override {
    if ((events & EventHandler::READ) == 0) {
      return;
    }
    detail::drain_fd(wakeup_fd_.read);
    // NOTE: Do not call rd_kafka_poll() here. With the high-level consumer,
    // recursive queue serving can assert in librdkafka.
    auto promise = std::optional<folly::Promise<folly::Unit>>{};
    {
      auto guard = std::scoped_lock{state_mutex_};
      if (stopped_) {
        return;
      }
      if (waiter_) {
        promise = std::move(waiter_);
        waiter_.reset();
      } else {
        ++pending_notifications_;
      }
    }
    if (promise) {
      promise->setValue(folly::Unit{});
    }
  }

  /// Waits for a queue wakeup, optionally timing out to unblock idle flushes.
  auto wait_for_notification(std::optional<std::chrono::milliseconds> timeout)
    -> folly::coro::Task<NotificationWaitResult> {
    while (true) {
      auto future = folly::SemiFuture<folly::Unit>{};
      {
        auto guard = std::scoped_lock{state_mutex_};
        if (stopped_) {
          co_return NotificationWaitResult::stopped;
        }
        if (pending_notifications_ > 0) {
          --pending_notifications_;
          co_return NotificationWaitResult::notified;
        }
        waiter_.emplace();
        future = waiter_->getSemiFuture();
      }
      if (timeout) {
        try {
          co_await std::move(future).within(*timeout);
        } catch (const folly::FutureTimeout&) {
          auto guard = std::scoped_lock{state_mutex_};
          if (stopped_) {
            co_return NotificationWaitResult::stopped;
          }
          if (waiter_) {
            waiter_.reset();
            co_return NotificationWaitResult::timed_out;
          }
          // A concurrent wakeup consumed `waiter_` while timeout fired.
          co_return NotificationWaitResult::notified;
        }
      } else {
        co_await std::move(future);
      }
      auto guard = std::scoped_lock{state_mutex_};
      if (stopped_) {
        co_return NotificationWaitResult::stopped;
      }
      co_return NotificationWaitResult::notified;
    }
  }

  /// Returns whether a stop was requested.
  auto is_stopped() const -> bool {
    auto guard = std::scoped_lock{state_mutex_};
    return stopped_;
  }

  /// Polls librdkafka without blocking and wraps a returned message.
  [[nodiscard]] auto try_consume() -> std::shared_ptr<RdKafka::Message> {
    auto* message = consumer_->consume(0);
    if (not message) {
      return nullptr;
    }
    auto guard = std::shared_ptr<RdKafka::Message>{message};
    if (message->err() == RdKafka::ERR__TIMED_OUT) {
      guard.reset();
      return nullptr;
    }
    return guard;
  }

  /// Unhooks librdkafka queue wakeups from the eventfd.
  auto disable_events() noexcept -> void {
    if (queue_ != nullptr) {
      rd_kafka_queue_io_event_enable(queue_, -1, nullptr, 0);
    }
  }

  RdKafka::KafkaConsumer* consumer_ = nullptr;
  rd_kafka_queue_t* queue_ = nullptr;
  detail::WakeupFd wakeup_fd_{};
  // Invariant: `waiter_` and `pending_notifications_` are protected by
  // `state_mutex_`, and at most one waiter exists at a time.
  mutable std::mutex state_mutex_;
  bool stopped_ = false;
  std::size_t pending_notifications_ = 0;
  std::optional<folly::Promise<folly::Unit>> waiter_;
};

} // namespace tenzir::plugins::kafka
