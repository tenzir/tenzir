//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/curl.hpp"

#include "tenzir/async/notify.hpp"

#include <folly/CancellationToken.h>
#include <folly/coro/Baton.h>
#include <folly/coro/Task.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>

#include <curl/curl.h>

#include <deque>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace tenzir {

struct CurlBodySource::Impl {
  explicit Impl(size_t capacity) : capacity{capacity} {
    TENZIR_ASSERT(capacity > 0);
  }

  auto push(chunk_ptr chunk) -> Task<bool> {
    TENZIR_ASSERT(chunk);
    while (true) {
      auto resume = std::function<void()>{};
      {
        auto lock = std::unique_lock{mutex};
        if (aborted or closed) {
          co_return false;
        }
        if (buffered.size() < capacity) {
          buffered.push_back(std::move(chunk));
          if (paused) {
            paused = false;
            resume = resume_callback;
          }
          lock.unlock();
          data_ready.notify_one();
          if (resume) {
            resume();
          }
          co_return true;
        }
      }
      co_await space_available.wait();
    }
  }

  auto close() -> void {
    auto resume = std::function<void()>{};
    {
      auto lock = std::lock_guard{mutex};
      if (closed or aborted) {
        return;
      }
      closed = true;
      if (paused) {
        paused = false;
        resume = resume_callback;
      }
    }
    data_ready.notify_one();
    space_available.notify_one();
    if (resume) {
      resume();
    }
  }

  auto abort() -> void {
    auto resume = std::function<void()>{};
    {
      auto lock = std::lock_guard{mutex};
      if (aborted) {
        return;
      }
      aborted = true;
      if (paused) {
        paused = false;
        resume = resume_callback;
      }
    }
    data_ready.notify_one();
    space_available.notify_one();
    if (resume) {
      resume();
    }
  }

  auto read(std::span<std::byte> buffer) -> size_t {
    auto notify_space = false;
    auto written = size_t{0};
    {
      auto lock = std::lock_guard{mutex};
      if (aborted) {
        return CURL_READFUNC_ABORT;
      }
      if (buffered.empty()) {
        if (closed) {
          return 0;
        }
        paused = true;
        return CURL_READFUNC_PAUSE;
      }
      while (written < buffer.size() and not buffered.empty()) {
        auto const& front = buffered.front();
        TENZIR_ASSERT(front);
        auto remaining = front->size() - front_offset;
        auto count = std::min(buffer.size() - written, remaining);
        std::memcpy(buffer.data() + written, front->data() + front_offset,
                    count);
        written += count;
        front_offset += count;
        if (front_offset == front->size()) {
          buffered.pop_front();
          front_offset = 0;
          notify_space = true;
        }
      }
    }
    if (notify_space) {
      space_available.notify_one();
    }
    return written;
  }

  auto wait_until_ready() -> Task<void> {
    while (true) {
      {
        auto lock = std::lock_guard{mutex};
        if (aborted or closed or not buffered.empty()) {
          co_return;
        }
      }
      co_await data_ready.wait();
    }
  }

  auto set_resume_callback(std::function<void()> callback) -> void {
    auto lock = std::lock_guard{mutex};
    resume_callback = std::move(callback);
  }

  std::mutex mutex;
  Notify data_ready;
  Notify space_available;
  std::deque<chunk_ptr> buffered;
  size_t capacity = 0;
  size_t front_offset = 0;
  bool closed = false;
  bool aborted = false;
  bool paused = false;
  std::function<void()> resume_callback;
};

struct CurlBodySink::Impl {
  explicit Impl(size_t capacity) : capacity{capacity} {
    TENZIR_ASSERT(capacity > 0);
  }

  auto pop() -> Task<Option<chunk_ptr>> {
    while (true) {
      auto chunk = chunk_ptr{};
      auto resume = std::function<void()>{};
      {
        auto lock = std::unique_lock{mutex};
        if (not buffered.empty()) {
          chunk = std::move(buffered.front());
          buffered.pop_front();
          if (paused) {
            paused = false;
            resume = resume_callback;
          }
        } else if (closed or aborted) {
          co_return None{};
        }
      }
      if (chunk) {
        if (resume) {
          resume();
        }
        co_return chunk;
      }
      co_await data_available.wait();
    }
  }

  auto close() -> void {
    auto lock = std::lock_guard{mutex};
    closed = true;
    data_available.notify_one();
  }

  auto abort() -> void {
    auto resume = std::function<void()>{};
    {
      auto lock = std::lock_guard{mutex};
      if (aborted) {
        return;
      }
      aborted = true;
      if (paused) {
        paused = false;
        resume = resume_callback;
      }
    }
    data_available.notify_one();
    if (resume) {
      resume();
    }
  }

  auto write(std::span<const std::byte> buffer) -> size_t {
    auto lock = std::lock_guard{mutex};
    if (aborted) {
      return CURL_WRITEFUNC_ERROR;
    }
    if (buffered.size() >= capacity) {
      paused = true;
      return CURL_WRITEFUNC_PAUSE;
    }
    buffered.push_back(chunk::copy(buffer));
    data_available.notify_one();
    return buffer.size();
  }

  auto set_resume_callback(std::function<void()> callback) -> void {
    auto lock = std::lock_guard{mutex};
    resume_callback = std::move(callback);
  }

  std::mutex mutex;
  Notify data_available;
  std::deque<chunk_ptr> buffered;
  size_t capacity = 0;
  bool closed = false;
  bool aborted = false;
  bool paused = false;
  std::function<void()> resume_callback;
};

CurlBodySource::CurlBodySource(size_t capacity)
  : impl_{std::in_place, capacity} {
}

CurlBodySource::~CurlBodySource() = default;

auto CurlBodySource::push(chunk_ptr chunk) -> Task<bool> {
  co_return co_await impl_->push(std::move(chunk));
}

auto CurlBodySource::close() -> void {
  impl_->close();
}

auto CurlBodySource::wait_until_ready() -> Task<void> {
  co_await impl_->wait_until_ready();
}

auto CurlBodySource::abort() -> void {
  impl_->abort();
}

auto CurlBodySource::read(std::span<std::byte> buffer) -> size_t {
  return impl_->read(buffer);
}

auto CurlBodySource::set_resume_callback(std::function<void()> callback)
  -> void {
  impl_->set_resume_callback(std::move(callback));
}

CurlBodySink::CurlBodySink(size_t capacity)
  : impl_{std::in_place, capacity} {
}

CurlBodySink::~CurlBodySink() = default;

auto CurlBodySink::pop() -> Task<Option<chunk_ptr>> {
  co_return co_await impl_->pop();
}

auto CurlBodySink::abort() -> void {
  impl_->abort();
}

auto CurlBodySink::close() -> void {
  impl_->close();
}

auto CurlBodySink::write(std::span<const std::byte> buffer) -> size_t {
  return impl_->write(buffer);
}

auto CurlBodySink::set_resume_callback(std::function<void()> callback)
  -> void {
  impl_->set_resume_callback(std::move(callback));
}

namespace {

class PerformState;

class SocketHandler final : private folly::EventHandler {
public:
  SocketHandler(PerformState& owner, curl_socket_t socket);
  ~SocketHandler() override;

  auto update(int what) -> void;

private:
  auto handlerReady(uint16_t events) noexcept -> void override;

  PerformState& owner_;
  curl_socket_t socket_{};
};

class TimerHandler final : private folly::AsyncTimeout {
public:
  TimerHandler(folly::EventBase* evb, PerformState& owner);
  ~TimerHandler() override;

  auto update(long timeout_ms) -> void;

private:
  auto timeoutExpired() noexcept -> void override;

  PerformState& owner_;
};

class PerformState {
public:
  PerformState(folly::Executor::KeepAlive<folly::IOExecutor> executor,
               curl::easy& easy, CurlBodyHandlers handlers)
    : executor_{std::move(executor)},
      evb_{executor_->getEventBase()},
      easy_{easy},
      handlers_{handlers},
      timer_{evb_, *this} {
    TENZIR_ASSERT(evb_ != nullptr);
  }

  auto start() -> void {
    if (handlers_.source) {
      handlers_.source->set_resume_callback([this]() {
        request_resume_send();
      });
      auto code = easy_.set([this](std::span<std::byte> buffer) -> size_t {
        return read_from_source(buffer);
      });
      if (code != curl::easy::code::ok) {
        complete(curl::to_error(code));
        return;
      }
    }
    if (handlers_.sink) {
      handlers_.sink->set_resume_callback([this]() {
        request_resume_recv();
      });
      auto code = easy_.set_write_result_callback(
        [this](std::span<const std::byte> buffer) -> size_t {
          return write_to_sink(buffer);
        });
      if (code != curl::easy::code::ok) {
        complete(curl::to_error(code));
        return;
      }
    }
    auto code = multi_.set_socket_callback(&socket_callback, this);
    if (code != curl::multi::code::ok) {
      complete(curl::to_error(code));
      return;
    }
    code = multi_.set_timer_callback(&timer_callback, this);
    if (code != curl::multi::code::ok) {
      complete(curl::to_error(code));
      return;
    }
    code = multi_.add(easy_);
    if (code != curl::multi::code::ok) {
      complete(curl::to_error(code));
      return;
    }
    added_ = true;
    drive(CURL_SOCKET_TIMEOUT, 0);
  }

  auto wait() -> Task<void> {
    co_await done_;
  }

  auto result() && -> caf::error {
    return std::move(result_);
  }

  auto on_socket_ready(curl_socket_t socket, uint16_t events) -> void {
    auto action = 0;
    if ((events & folly::EventHandler::READ) != 0) {
      action |= CURL_CSELECT_IN;
    }
    if ((events & folly::EventHandler::WRITE) != 0) {
      action |= CURL_CSELECT_OUT;
    }
    drive(socket, action);
  }

  auto on_timeout() -> void {
    drive(CURL_SOCKET_TIMEOUT, 0);
  }

private:
  static auto socket_callback(CURL*, curl_socket_t socket, int what,
                              void* clientp, void*) -> int {
    auto* self = static_cast<PerformState*>(clientp);
    return self->update_socket(socket, what);
  }

  static auto timer_callback(CURLM*, long timeout_ms, void* clientp) -> int {
    auto* self = static_cast<PerformState*>(clientp);
    return self->update_timer(timeout_ms);
  }

  auto update_socket(curl_socket_t socket, int what) -> int {
    if (completed_) {
      return 0;
    }
    if (what == CURL_POLL_REMOVE) {
      socket_handlers_.erase(socket);
      return 0;
    }
    auto [it, inserted]
      = socket_handlers_.try_emplace(socket, std::in_place, *this, socket);
    std::ignore = inserted;
    it->second->update(what);
    return 0;
  }

  auto update_timer(long timeout_ms) -> int {
    if (completed_) {
      return 0;
    }
    timer_.update(timeout_ms);
    return 0;
  }

  auto read_from_source(std::span<std::byte> buffer) -> size_t {
    TENZIR_ASSERT(handlers_.source);
    auto result = handlers_.source->read(buffer);
    if (result == CURL_READFUNC_PAUSE) {
      paused_send_ = true;
    }
    return result;
  }

  auto write_to_sink(std::span<const std::byte> buffer) -> size_t {
    TENZIR_ASSERT(handlers_.sink);
    auto result = handlers_.sink->write(buffer);
    if (result == CURL_WRITEFUNC_PAUSE) {
      paused_recv_ = true;
    }
    return result;
  }

  auto request_resume_send() -> void {
    evb_->runInEventBaseThread([this]() {
      resume_send();
    });
  }

  auto request_resume_recv() -> void {
    evb_->runInEventBaseThread([this]() {
      resume_recv();
    });
  }

  auto resume_send() -> void {
    if (completed_ or not paused_send_) {
      return;
    }
    paused_send_ = false;
    auto code = easy_.pause(current_pause_mask());
    if (code != curl::easy::code::ok) {
      complete(curl::to_error(code));
      return;
    }
    drive(CURL_SOCKET_TIMEOUT, 0);
  }

  auto resume_recv() -> void {
    if (completed_ or not paused_recv_) {
      return;
    }
    paused_recv_ = false;
    auto code = easy_.pause(current_pause_mask());
    if (code != curl::easy::code::ok) {
      complete(curl::to_error(code));
      return;
    }
    drive(CURL_SOCKET_TIMEOUT, 0);
  }

  auto current_pause_mask() const -> int {
    auto mask = 0;
    if (paused_send_) {
      mask |= CURLPAUSE_SEND;
    }
    if (paused_recv_) {
      mask |= CURLPAUSE_RECV;
    }
    return mask;
  }

  auto drive(curl_socket_t socket, int ev_bitmask) -> void {
    if (completed_) {
      return;
    }
    auto [code, running_handles] = multi_.socket_action(socket, ev_bitmask);
    std::ignore = running_handles;
    if (code != curl::multi::code::ok) {
      complete(curl::to_error(code));
      return;
    }
    for (auto result : multi_.info_read()) {
      if (result != curl::easy::code::ok) {
        complete(curl::to_error(result));
        return;
      }
      complete({});
      return;
    }
  }

  auto cleanup() -> void {
    if (handlers_.source) {
      handlers_.source->set_resume_callback({});
      handlers_.source->abort();
    }
    if (handlers_.sink) {
      handlers_.sink->set_resume_callback({});
      handlers_.sink->close();
    }
    timer_.update(-1);
    socket_handlers_.clear();
    std::ignore = multi_.set_socket_callback(nullptr, nullptr);
    std::ignore = multi_.set_timer_callback(nullptr, nullptr);
    if (added_) {
      std::ignore = multi_.remove(easy_);
      added_ = false;
    }
  }

  auto complete(caf::error result) -> void {
    if (completed_) {
      return;
    }
    completed_ = true;
    result_ = std::move(result);
    cleanup();
    done_.post();
  }

  folly::Executor::KeepAlive<folly::IOExecutor> executor_;
  folly::EventBase* evb_ = nullptr;
  curl::easy& easy_;
  CurlBodyHandlers handlers_;
  curl::multi multi_;
  std::unordered_map<curl_socket_t, Box<SocketHandler>> socket_handlers_;
  TimerHandler timer_;
  folly::coro::Baton done_;
  caf::error result_;
  bool added_ = false;
  bool completed_ = false;
  bool paused_send_ = false;
  bool paused_recv_ = false;

  friend class SocketHandler;
  friend class TimerHandler;
};

SocketHandler::SocketHandler(PerformState& owner, curl_socket_t socket)
  : folly::EventHandler{owner.evb_}, owner_{owner}, socket_{socket} {
  initHandler(owner.evb_, folly::NetworkSocket::fromFd(socket));
}

SocketHandler::~SocketHandler() {
  if (isHandlerRegistered()) {
    unregisterHandler();
  }
}

auto SocketHandler::update(int what) -> void {
  if (isHandlerRegistered()) {
    unregisterHandler();
  }
  auto events = uint16_t{folly::EventHandler::PERSIST};
  if (what == CURL_POLL_IN or what == CURL_POLL_INOUT) {
    events |= folly::EventHandler::READ;
  }
  if (what == CURL_POLL_OUT or what == CURL_POLL_INOUT) {
    events |= folly::EventHandler::WRITE;
  }
  if ((events & (folly::EventHandler::READ | folly::EventHandler::WRITE)) != 0) {
    registerHandler(events);
  }
}

auto SocketHandler::handlerReady(uint16_t events) noexcept -> void {
  owner_.on_socket_ready(socket_, events);
}

TimerHandler::TimerHandler(folly::EventBase* evb, PerformState& owner)
  : folly::AsyncTimeout{evb}, owner_{owner} {
}

TimerHandler::~TimerHandler() {
  cancelTimeout();
}

auto TimerHandler::update(long timeout_ms) -> void {
  cancelTimeout();
  if (timeout_ms >= 0) {
    scheduleTimeout(std::chrono::milliseconds{timeout_ms});
  }
}

auto TimerHandler::timeoutExpired() noexcept -> void {
  owner_.on_timeout();
}

} // namespace

auto perform_curl(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                  curl::easy& handle, CurlBodyHandlers handlers)
  -> Task<caf::error> {
  co_return co_await folly::coro::co_withExecutor(
    executor,
    [executor = std::move(executor), &handle, handlers]() mutable
      -> Task<caf::error> {
      auto state = PerformState{std::move(executor), handle, handlers};
      state.start();
      co_await state.wait();
      co_return std::move(state).result();
    }());
}

} // namespace tenzir
