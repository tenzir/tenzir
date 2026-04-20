//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/curl.hpp"

#include "tenzir/async/notify.hpp"
#include "tenzir/panic.hpp"

#include <curl/curl.h>
#include <folly/CancellationToken.h>
#include <folly/coro/Baton.h>
#include <folly/coro/Error.h>
#include <folly/coro/Task.h>
#include <folly/coro/WithCancellation.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>

#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <tuple>
#include <unordered_map>
#include <utility>

namespace tenzir {

namespace {

enum class CurlPerformOutcome {
  success,
  local_abort,
};

using CurlPerformResult = Result<CurlPerformOutcome, std::string>;

class CurlUploadBody {
public:
  explicit CurlUploadBody(size_t capacity);
  ~CurlUploadBody();

  CurlUploadBody(CurlUploadBody const&) = delete;
  auto operator=(CurlUploadBody const&) -> CurlUploadBody& = delete;
  CurlUploadBody(CurlUploadBody&&) = delete;
  auto operator=(CurlUploadBody&&) -> CurlUploadBody& = delete;

  auto push(chunk_ptr chunk) -> Task<bool>;
  auto close() -> void;
  auto wait_until_ready() -> Task<bool>;
  auto is_aborted() -> bool;
  auto abort() -> void;
  auto terminate() -> void;
  auto read(std::span<std::byte> buffer) -> size_t;
  auto set_resume_callback(std::function<void()> callback) -> void;

private:
  struct Impl;
  Box<Impl> impl_;
};

class CurlDownloadBody {
public:
  explicit CurlDownloadBody(size_t capacity);
  ~CurlDownloadBody();

  CurlDownloadBody(CurlDownloadBody const&) = delete;
  auto operator=(CurlDownloadBody const&) -> CurlDownloadBody& = delete;
  CurlDownloadBody(CurlDownloadBody&&) = delete;
  auto operator=(CurlDownloadBody&&) -> CurlDownloadBody& = delete;

  auto pop() -> Task<Option<chunk_ptr>>;
  auto is_aborted() -> bool;
  auto abort() -> void;
  auto close() -> void;
  auto write(std::span<const std::byte> buffer) -> size_t;
  auto set_resume_callback(std::function<void()> callback) -> void;

private:
  struct Impl;
  Box<Impl> impl_;
};

struct CurlUploadBody::Impl {
  enum class State {
    open,
    closed,
    aborted,
    terminated,
    aborted_terminated,
  };

  struct Transition {
    bool changed = false;
    std::function<void()> resume;
  };

  explicit Impl(size_t capacity) : capacity{capacity} {
    TENZIR_ASSERT(capacity > 0);
  }

  auto push(chunk_ptr chunk) -> Task<bool> {
    TENZIR_ASSERT(chunk);
    while (true) {
      auto resume = std::function<void()>{};
      {
        auto lock = std::unique_lock{mutex};
        if (state != State::open) {
          co_return false;
        }
        if (buffered.size() < capacity) {
          buffered.push_back(std::move(chunk));
          resume = take_resume_callback();
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
    auto transition = transition_to_closed();
    if (not transition.changed) {
      return;
    }
    data_ready.notify_one();
    space_available.notify_one();
    if (transition.resume) {
      transition.resume();
    }
  }

  auto abort() -> void {
    auto transition = transition_to_aborted();
    if (not transition.changed) {
      return;
    }
    data_ready.notify_one();
    space_available.notify_one();
    if (transition.resume) {
      transition.resume();
    }
  }

  auto terminate() -> void {
    auto transition = transition_to_terminated();
    if (not transition.changed) {
      return;
    }
    data_ready.notify_one();
    space_available.notify_one();
    if (transition.resume) {
      transition.resume();
    }
  }

  auto read(std::span<std::byte> buffer) -> size_t {
    auto notify_space = false;
    auto written = size_t{0};
    {
      auto lock = std::lock_guard{mutex};
      if (is_aborted_state()) {
        return CURL_READFUNC_ABORT;
      }
      if (buffered.empty()) {
        if (state != State::open) {
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

  auto wait_until_ready() -> Task<bool> {
    while (true) {
      {
        auto lock = std::lock_guard{mutex};
        if (is_aborted_state()) {
          co_return false;
        }
        if (not buffered.empty()) {
          co_return true;
        }
        if (state != State::open) {
          co_return false;
        }
      }
      co_await data_ready.wait();
    }
  }

  auto is_aborted() -> bool {
    auto lock = std::lock_guard{mutex};
    return is_aborted_state();
  }

  auto set_resume_callback(std::function<void()> callback) -> void {
    auto lock = std::lock_guard{mutex};
    resume_callback = std::move(callback);
  }

  auto is_aborted_state() const -> bool {
    return state == State::aborted or state == State::aborted_terminated;
  }

  auto take_resume_callback() -> std::function<void()> {
    if (not paused) {
      return {};
    }
    paused = false;
    return resume_callback;
  }

  auto transition_to_closed() -> Transition {
    auto lock = std::lock_guard{mutex};
    if (state != State::open) {
      return {};
    }
    state = State::closed;
    return Transition{.changed = true, .resume = take_resume_callback()};
  }

  auto transition_to_aborted() -> Transition {
    auto lock = std::lock_guard{mutex};
    if (is_aborted_state() or state == State::terminated) {
      return {};
    }
    state = State::aborted;
    return Transition{.changed = true, .resume = take_resume_callback()};
  }

  auto transition_to_terminated() -> Transition {
    auto lock = std::lock_guard{mutex};
    if (state == State::terminated or state == State::aborted_terminated) {
      return {};
    }
    state = is_aborted_state() ? State::aborted_terminated : State::terminated;
    return Transition{.changed = true, .resume = take_resume_callback()};
  }

  std::mutex mutex;
  Notify data_ready;
  Notify space_available;
  std::deque<chunk_ptr> buffered;
  size_t capacity = 0;
  size_t front_offset = 0;
  State state = State::open;
  bool paused = false;
  std::function<void()> resume_callback;
};

struct CurlDownloadBody::Impl {
  enum class State {
    open,
    closed,
    aborted,
  };

  struct Transition {
    bool changed = false;
    std::function<void()> resume;
  };

  explicit Impl(size_t capacity) : capacity{capacity} {
    TENZIR_ASSERT(capacity > 0);
  }

  auto pop() -> Task<Option<chunk_ptr>> {
    while (true) {
      auto chunk = chunk_ptr{};
      auto resume = std::function<void()>{};
      {
        auto lock = std::unique_lock{mutex};
        if (state == State::aborted) {
          co_return None{};
        }
        if (not buffered.empty()) {
          chunk = std::move(buffered.front());
          buffered.pop_front();
          resume = take_resume_callback();
        } else if (state == State::closed) {
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
    auto transition = transition_to_closed();
    if (not transition.changed) {
      return;
    }
    data_available.notify_one();
  }

  auto abort() -> void {
    auto transition = transition_to_aborted();
    if (not transition.changed) {
      return;
    }
    data_available.notify_one();
    if (transition.resume) {
      transition.resume();
    }
  }

  auto is_aborted() -> bool {
    auto lock = std::lock_guard{mutex};
    return state == State::aborted;
  }

  auto write(std::span<const std::byte> buffer) -> size_t {
    auto lock = std::lock_guard{mutex};
    if (state == State::aborted) {
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

  auto take_resume_callback() -> std::function<void()> {
    if (not paused) {
      return {};
    }
    paused = false;
    return resume_callback;
  }

  auto transition_to_closed() -> Transition {
    auto lock = std::lock_guard{mutex};
    if (state != State::open) {
      return {};
    }
    state = State::closed;
    return Transition{.changed = true};
  }

  auto transition_to_aborted() -> Transition {
    auto lock = std::lock_guard{mutex};
    if (state == State::aborted) {
      return {};
    }
    state = State::aborted;
    buffered.clear();
    return Transition{.changed = true, .resume = take_resume_callback()};
  }

  std::mutex mutex;
  Notify data_available;
  std::deque<chunk_ptr> buffered;
  size_t capacity = 0;
  State state = State::open;
  bool paused = false;
  std::function<void()> resume_callback;
};

CurlUploadBody::CurlUploadBody(size_t capacity)
  : impl_{std::in_place, capacity} {
}

CurlUploadBody::~CurlUploadBody() = default;

auto CurlUploadBody::push(chunk_ptr chunk) -> Task<bool> {
  co_return co_await impl_->push(std::move(chunk));
}

auto CurlUploadBody::close() -> void {
  impl_->close();
}

auto CurlUploadBody::wait_until_ready() -> Task<bool> {
  co_return co_await impl_->wait_until_ready();
}

auto CurlUploadBody::is_aborted() -> bool {
  return impl_->is_aborted();
}

auto CurlUploadBody::abort() -> void {
  impl_->abort();
}

auto CurlUploadBody::terminate() -> void {
  impl_->terminate();
}

auto CurlUploadBody::read(std::span<std::byte> buffer) -> size_t {
  return impl_->read(buffer);
}

auto CurlUploadBody::set_resume_callback(std::function<void()> callback)
  -> void {
  impl_->set_resume_callback(std::move(callback));
}

CurlDownloadBody::CurlDownloadBody(size_t capacity)
  : impl_{std::in_place, capacity} {
}

CurlDownloadBody::~CurlDownloadBody() = default;

auto CurlDownloadBody::pop() -> Task<Option<chunk_ptr>> {
  co_return co_await impl_->pop();
}

auto CurlDownloadBody::is_aborted() -> bool {
  return impl_->is_aborted();
}

auto CurlDownloadBody::abort() -> void {
  impl_->abort();
}

auto CurlDownloadBody::close() -> void {
  impl_->close();
}

auto CurlDownloadBody::write(std::span<const std::byte> buffer) -> size_t {
  return impl_->write(buffer);
}

auto CurlDownloadBody::set_resume_callback(std::function<void()> callback)
  -> void {
  impl_->set_resume_callback(std::move(callback));
}

auto render_curl_error(curl::easy::code code) -> std::string {
  return std::string{"!! unspecified: curl: "}
         + std::string{curl::to_string(code)};
}

auto render_curl_error(curl::multi::code code) -> std::string {
  return std::string{"!! unspecified: curl: "}
         + std::string{curl::to_string(code)};
}

auto curl_perform_failure(curl::easy::code code) -> CurlPerformResult {
  return Err{render_curl_error(code)};
}

auto curl_perform_failure(curl::multi::code code) -> CurlPerformResult {
  return Err{render_curl_error(code)};
}

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

class PerformState final : public std::enable_shared_from_this<PerformState> {
public:
  PerformState(folly::Executor::KeepAlive<folly::IOExecutor> executor,
               curl::easy& easy, CurlUploadBody* upload_body,
               CurlDownloadBody* download_body)
    : executor_{std::move(executor)},
      evb_{executor_->getEventBase()},
      easy_{easy},
      upload_body_{upload_body},
      download_body_{download_body},
      timer_{evb_, *this} {
    TENZIR_ASSERT(evb_ != nullptr);
  }

  auto start() -> void {
    if (upload_body_) {
      auto weak = weak_from_this();
      upload_body_->set_resume_callback([weak]() {
        if (auto self = weak.lock()) {
          self->request_resume(CURLPAUSE_SEND);
        }
      });
      auto code = easy_.set([this](std::span<std::byte> buffer) -> size_t {
        return read_from_source(buffer);
      });
      if (code != curl::easy::code::ok) {
        complete(curl_perform_failure(code));
        return;
      }
    }
    if (download_body_) {
      auto weak = weak_from_this();
      download_body_->set_resume_callback([weak]() {
        if (auto self = weak.lock()) {
          self->request_resume(CURLPAUSE_RECV);
        }
      });
      auto code = easy_.set_write_result_callback(
        [this](std::span<const std::byte> buffer) -> size_t {
          return write_to_sink(buffer);
        });
      if (code != curl::easy::code::ok) {
        complete(curl_perform_failure(code));
        return;
      }
    }
    auto code = multi_.set_socket_callback(&socket_callback, this);
    if (code != curl::multi::code::ok) {
      complete(curl_perform_failure(code));
      return;
    }
    code = multi_.set_timer_callback(&timer_callback, this);
    if (code != curl::multi::code::ok) {
      complete(curl_perform_failure(code));
      return;
    }
    code = multi_.add(easy_);
    if (code != curl::multi::code::ok) {
      complete(curl_perform_failure(code));
      return;
    }
    added_ = true;
    drive(CURL_SOCKET_TIMEOUT, 0);
  }

  auto wait() -> Task<void> {
    auto& token = co_await folly::coro::co_current_cancellation_token;
    auto callback = folly::CancellationCallback{token, [&]() noexcept {
                                                  request_cancel();
                                                }};
    co_await done_;
    if (cancelled_) {
      co_yield folly::coro::co_stopped_may_throw;
    }
  }

  auto take_result() -> CurlPerformResult {
    TENZIR_ASSERT(result_);
    return std::move(result_).unwrap();
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

  auto is_completed() const -> bool {
    return static_cast<bool>(result_);
  }

  auto update_socket(curl_socket_t socket, int what) -> int {
    if (is_completed()) {
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
    if (is_completed()) {
      return 0;
    }
    timer_.update(timeout_ms);
    return 0;
  }

  auto read_from_source(std::span<std::byte> buffer) -> size_t {
    TENZIR_ASSERT(upload_body_);
    auto result = upload_body_->read(buffer);
    if (result == CURL_READFUNC_PAUSE) {
      pause_mask_ |= CURLPAUSE_SEND;
    }
    return result;
  }

  auto write_to_sink(std::span<const std::byte> buffer) -> size_t {
    TENZIR_ASSERT(download_body_);
    auto result = download_body_->write(buffer);
    if (result == CURL_WRITEFUNC_PAUSE) {
      pause_mask_ |= CURLPAUSE_RECV;
    }
    return result;
  }

  auto request_resume(int pause_bit) -> void {
    auto self = shared_from_this();
    evb_->runInEventBaseThread([self = std::move(self), pause_bit]() {
      self->resume(pause_bit);
    });
  }

  auto request_cancel() -> void {
    auto self = shared_from_this();
    evb_->runInEventBaseThread([self = std::move(self)]() {
      self->cancel();
    });
  }

  auto resume(int pause_bit) -> void {
    if (is_completed() or (pause_mask_ & pause_bit) == 0) {
      return;
    }
    pause_mask_ &= ~pause_bit;
    auto code = easy_.pause(pause_mask_);
    if (code != curl::easy::code::ok) {
      complete(curl_perform_failure(code));
      return;
    }
    drive(CURL_SOCKET_TIMEOUT, 0);
  }

  auto drive(curl_socket_t socket, int ev_bitmask) -> void {
    if (is_completed()) {
      return;
    }
    auto [code, running_handles] = multi_.socket_action(socket, ev_bitmask);
    std::ignore = running_handles;
    if (code != curl::multi::code::ok) {
      complete(curl_perform_failure(code));
      return;
    }
    for (auto result : multi_.info_read()) {
      if (result != curl::easy::code::ok) {
        complete(curl_perform_failure(result));
        return;
      }
      complete(CurlPerformOutcome::success);
      return;
    }
  }

  auto cancel() -> void {
    if (is_completed()) {
      return;
    }
    cancelled_ = true;
    if (upload_body_) {
      upload_body_->abort();
    }
    if (download_body_) {
      download_body_->abort();
    }
    complete(CurlPerformOutcome::success);
  }

  auto cleanup() -> void {
    if (upload_body_) {
      upload_body_->set_resume_callback({});
      upload_body_->terminate();
    }
    if (download_body_) {
      download_body_->set_resume_callback({});
      download_body_->close();
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

  auto complete(CurlPerformResult result) -> void {
    if (is_completed()) {
      return;
    }
    result_ = std::move(result);
    cleanup();
    done_.post();
  }

  folly::Executor::KeepAlive<folly::IOExecutor> executor_;
  folly::EventBase* evb_ = nullptr;
  curl::easy& easy_;
  CurlUploadBody* upload_body_ = nullptr;
  CurlDownloadBody* download_body_ = nullptr;
  curl::multi multi_;
  std::unordered_map<curl_socket_t, Box<SocketHandler>> socket_handlers_;
  TimerHandler timer_;
  folly::coro::Baton done_;
  Option<CurlPerformResult> result_ = None{};
  bool added_ = false;
  bool cancelled_ = false;
  int pause_mask_ = 0;

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
  if ((events & (folly::EventHandler::READ | folly::EventHandler::WRITE))
      != 0) {
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

auto perform_curl_impl(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                       curl::easy& handle, CurlUploadBody* upload_body,
                       CurlDownloadBody* download_body)
  -> Task<CurlPerformResult> {
  auto state_executor = executor;
  auto task = [state_executor = std::move(state_executor), &handle, upload_body,
               download_body]() mutable -> Task<CurlPerformResult> {
    auto state = std::make_shared<PerformState>(
      std::move(state_executor), handle, upload_body, download_body);
    state->start();
    co_await state->wait();
    co_return state->take_result();
  };
  co_return co_await folly::coro::co_withExecutor(std::move(executor), task());
}

auto perform_curl_transfer(
  folly::Executor::KeepAlive<folly::IOExecutor> executor, curl::easy& handle)
  -> Task<CurlPerformResult> {
  co_return co_await perform_curl_impl(std::move(executor), handle, nullptr,
                                       nullptr);
}

auto perform_curl_upload_transfer(
  folly::Executor::KeepAlive<folly::IOExecutor> executor, curl::easy& handle,
  CurlUploadBody& body) -> Task<CurlPerformResult> {
  auto should_start_transfer = false;
  try {
    should_start_transfer = co_await body.wait_until_ready();
  } catch (folly::OperationCancelled const&) {
    body.abort();
    throw;
  }
  if (body.is_aborted()) {
    co_return CurlPerformOutcome::local_abort;
  }
  if (not should_start_transfer) {
    co_return CurlPerformOutcome::success;
  }
  auto result
    = co_await perform_curl_impl(std::move(executor), handle, &body, nullptr);
  if (body.is_aborted()) {
    co_return CurlPerformOutcome::local_abort;
  }
  co_return result;
}

auto perform_curl_download_transfer(
  folly::Executor::KeepAlive<folly::IOExecutor> executor, curl::easy& handle,
  CurlDownloadBody& body) -> Task<CurlPerformResult> {
  if (body.is_aborted()) {
    co_return CurlPerformOutcome::local_abort;
  }
  auto result
    = co_await perform_curl_impl(std::move(executor), handle, nullptr, &body);
  if (body.is_aborted()) {
    co_return CurlPerformOutcome::local_abort;
  }
  co_return result;
}

auto perform_curl_duplex_transfer(
  folly::Executor::KeepAlive<folly::IOExecutor> executor, curl::easy& handle,
  CurlUploadBody& upload_body, CurlDownloadBody& download_body)
  -> Task<CurlPerformResult> {
  if (upload_body.is_aborted() or download_body.is_aborted()) {
    co_return CurlPerformOutcome::local_abort;
  }
  auto result = co_await perform_curl_impl(std::move(executor), handle,
                                           &upload_body, &download_body);
  if (upload_body.is_aborted() or download_body.is_aborted()) {
    co_return CurlPerformOutcome::local_abort;
  }
  co_return result;
}

struct TransferControl;

struct CurlSessionState final
  : public std::enable_shared_from_this<CurlSessionState> {
  explicit CurlSessionState(
    folly::Executor::KeepAlive<folly::IOExecutor> executor)
    : executor{std::move(executor)} {
  }

  auto begin_transfer() -> std::shared_ptr<TransferControl>;

  auto release() -> void {
    auto lock = std::lock_guard{mutex};
    busy = false;
  }

  auto is_busy() const -> bool {
    auto lock = std::lock_guard{mutex};
    return busy;
  }

  folly::Executor::KeepAlive<folly::IOExecutor> executor;
  curl::easy easy;

private:
  mutable std::mutex mutex;
  bool busy = false;
};

struct TransferControl final {
  explicit TransferControl(std::shared_ptr<CurlSessionState> session)
    : session{std::move(session)} {
  }

  auto mark_waiting() -> void {
    auto lock = std::lock_guard{mutex};
    if (finished) {
      panic("curl transfer already completed");
    }
    if (waiting) {
      panic("curl transfer wait() called more than once");
    }
    waiting = true;
  }

  auto cancel() -> void {
    cancellation.requestCancellation();
    auto should_finish = false;
    {
      auto lock = std::lock_guard{mutex};
      should_finish = not waiting;
    }
    if (should_finish) {
      finish();
    }
  }

  auto finish() -> void {
    auto should_release = false;
    {
      auto lock = std::lock_guard{mutex};
      if (finished) {
        return;
      }
      finished = true;
      should_release = true;
    }
    if (should_release) {
      session->release();
    }
  }

  auto is_finished() const -> bool {
    auto lock = std::lock_guard{mutex};
    return finished;
  }

  std::shared_ptr<CurlSessionState> session;
  folly::CancellationSource cancellation;

private:
  mutable std::mutex mutex;
  bool waiting = false;
  bool finished = false;
};

auto CurlSessionState::begin_transfer() -> std::shared_ptr<TransferControl> {
  auto lock = std::lock_guard{mutex};
  if (busy) {
    panic("curl session already has an active transfer");
  }
  busy = true;
  return std::make_shared<TransferControl>(shared_from_this());
}

struct TransferFinishGuard {
  ~TransferFinishGuard() {
    control->finish();
  }

  std::shared_ptr<TransferControl> control;
};

auto to_public_result(CurlPerformResult result) -> CurlResult {
  if (result.is_err()) {
    return Err{CurlError{.message = std::move(result).unwrap_err()}};
  }
  auto outcome = std::move(result).unwrap();
  auto kind = outcome == CurlPerformOutcome::local_abort
                ? CurlCompletionKind::local_abort
                : CurlCompletionKind::finished;
  return CurlCompletion{.kind = kind};
}

auto wait_for_transfer(std::shared_ptr<TransferControl> control,
                       Task<CurlPerformResult> task) -> Task<CurlResult> {
  control->mark_waiting();
  auto guard = TransferFinishGuard{control};
  auto token = folly::cancellation_token_merge(
    co_await folly::coro::co_current_cancellation_token,
    control->cancellation.getToken());
  auto result
    = co_await folly::coro::co_withCancellation(token, std::move(task));
  co_return to_public_result(std::move(result));
}

} // namespace

struct CurlPerformTransfer::Impl {
  explicit Impl(std::shared_ptr<TransferControl> control)
    : control{std::move(control)} {
  }

  ~Impl() {
    if (not control->is_finished()) {
      control->cancel();
    }
  }

  std::shared_ptr<TransferControl> control;
};

CurlPerformTransfer::CurlPerformTransfer(Box<Impl> impl)
  : impl_{std::move(impl)} {
}

CurlPerformTransfer::~CurlPerformTransfer() = default;

CurlPerformTransfer::CurlPerformTransfer(CurlPerformTransfer&&) noexcept
  = default;

auto CurlPerformTransfer::operator=(CurlPerformTransfer&&) noexcept
  -> CurlPerformTransfer& = default;

auto CurlPerformTransfer::wait() -> Task<CurlResult> {
  auto control = impl_->control;
  auto& session = *control->session;
  co_return co_await wait_for_transfer(
    control, perform_curl_transfer(session.executor, session.easy));
}

auto CurlPerformTransfer::cancel() -> void {
  impl_->control->cancel();
}

struct CurlSendTransfer::Impl {
  Impl(std::shared_ptr<TransferControl> control, size_t capacity)
    : control{std::move(control)}, body{std::in_place, capacity} {
  }

  ~Impl() {
    if (not control->is_finished()) {
      body->abort();
      control->cancel();
    }
  }

  std::shared_ptr<TransferControl> control;
  Box<CurlUploadBody> body;
};

CurlSendTransfer::CurlSendTransfer(Box<Impl> impl) : impl_{std::move(impl)} {
}

CurlSendTransfer::~CurlSendTransfer() = default;

CurlSendTransfer::CurlSendTransfer(CurlSendTransfer&&) noexcept = default;

auto CurlSendTransfer::operator=(CurlSendTransfer&&) noexcept
  -> CurlSendTransfer& = default;

auto CurlSendTransfer::push(chunk_ptr chunk) -> Task<bool> {
  co_return co_await impl_->body->push(std::move(chunk));
}

auto CurlSendTransfer::close() -> void {
  impl_->body->close();
}

auto CurlSendTransfer::abort() -> void {
  impl_->body->abort();
}

auto CurlSendTransfer::wait() -> Task<CurlResult> {
  auto control = impl_->control;
  auto& session = *control->session;
  co_return co_await wait_for_transfer(
    control,
    perform_curl_upload_transfer(session.executor, session.easy, *impl_->body));
}

auto CurlSendTransfer::cancel() -> void {
  impl_->body->abort();
  impl_->control->cancel();
}

struct CurlReceiveTransfer::Impl {
  Impl(std::shared_ptr<TransferControl> control, size_t capacity)
    : control{std::move(control)}, body{std::in_place, capacity} {
  }

  ~Impl() {
    if (not control->is_finished()) {
      body->abort();
      control->cancel();
    }
  }

  std::shared_ptr<TransferControl> control;
  Box<CurlDownloadBody> body;
};

CurlReceiveTransfer::CurlReceiveTransfer(Box<Impl> impl)
  : impl_{std::move(impl)} {
}

CurlReceiveTransfer::~CurlReceiveTransfer() = default;

CurlReceiveTransfer::CurlReceiveTransfer(CurlReceiveTransfer&&) noexcept
  = default;

auto CurlReceiveTransfer::operator=(CurlReceiveTransfer&&) noexcept
  -> CurlReceiveTransfer& = default;

auto CurlReceiveTransfer::next() -> Task<Option<chunk_ptr>> {
  co_return co_await impl_->body->pop();
}

auto CurlReceiveTransfer::abort() -> void {
  impl_->body->abort();
}

auto CurlReceiveTransfer::wait() -> Task<CurlResult> {
  auto control = impl_->control;
  auto& session = *control->session;
  co_return co_await wait_for_transfer(
    control, perform_curl_download_transfer(session.executor, session.easy,
                                            *impl_->body));
}

auto CurlReceiveTransfer::cancel() -> void {
  impl_->body->abort();
  impl_->control->cancel();
}

struct CurlDuplexTransfer::Impl {
  Impl(std::shared_ptr<TransferControl> control, CurlStreamOptions options)
    : control{std::move(control)},
      upload_body{std::in_place, options.send_buffer_capacity},
      download_body{std::in_place, options.receive_buffer_capacity} {
  }

  ~Impl() {
    if (not control->is_finished()) {
      upload_body->abort();
      download_body->abort();
      control->cancel();
    }
  }

  std::shared_ptr<TransferControl> control;
  Box<CurlUploadBody> upload_body;
  Box<CurlDownloadBody> download_body;
};

CurlDuplexTransfer::CurlDuplexTransfer(Box<Impl> impl)
  : impl_{std::move(impl)} {
}

CurlDuplexTransfer::~CurlDuplexTransfer() = default;

CurlDuplexTransfer::CurlDuplexTransfer(CurlDuplexTransfer&&) noexcept = default;

auto CurlDuplexTransfer::operator=(CurlDuplexTransfer&&) noexcept
  -> CurlDuplexTransfer& = default;

auto CurlDuplexTransfer::push(chunk_ptr chunk) -> Task<bool> {
  co_return co_await impl_->upload_body->push(std::move(chunk));
}

auto CurlDuplexTransfer::close_send() -> void {
  impl_->upload_body->close();
}

auto CurlDuplexTransfer::abort_send() -> void {
  impl_->upload_body->abort();
}

auto CurlDuplexTransfer::next() -> Task<Option<chunk_ptr>> {
  co_return co_await impl_->download_body->pop();
}

auto CurlDuplexTransfer::abort_receive() -> void {
  impl_->download_body->abort();
}

auto CurlDuplexTransfer::wait() -> Task<CurlResult> {
  auto control = impl_->control;
  auto& session = *control->session;
  co_return co_await wait_for_transfer(
    control,
    perform_curl_duplex_transfer(session.executor, session.easy,
                                 *impl_->upload_body, *impl_->download_body));
}

auto CurlDuplexTransfer::cancel() -> void {
  impl_->upload_body->abort();
  impl_->download_body->abort();
  impl_->control->cancel();
}

struct CurlSession::Impl {
  explicit Impl(folly::Executor::KeepAlive<folly::IOExecutor> executor)
    : state{std::make_shared<CurlSessionState>(std::move(executor))} {
  }

  std::shared_ptr<CurlSessionState> state;
};

CurlSession::CurlSession(Box<Impl> impl) : impl_{std::move(impl)} {
}

auto CurlSession::make(folly::Executor::KeepAlive<folly::IOExecutor> executor)
  -> CurlSession {
  return CurlSession{Box<Impl>{std::in_place, std::move(executor)}};
}

CurlSession::~CurlSession() = default;

CurlSession::CurlSession(CurlSession&&) noexcept = default;

auto CurlSession::operator=(CurlSession&&) noexcept -> CurlSession& = default;

auto CurlSession::easy() -> curl::easy& {
  return impl_->state->easy;
}

auto CurlSession::start_perform() -> CurlPerformTransfer {
  return CurlPerformTransfer{Box<CurlPerformTransfer::Impl>{
    std::in_place, impl_->state->begin_transfer()}};
}

auto CurlSession::start_send(CurlStreamOptions options) -> CurlSendTransfer {
  return CurlSendTransfer{
    Box<CurlSendTransfer::Impl>{std::in_place, impl_->state->begin_transfer(),
                                options.send_buffer_capacity}};
}

auto CurlSession::start_receive(CurlStreamOptions options)
  -> CurlReceiveTransfer {
  return CurlReceiveTransfer{Box<CurlReceiveTransfer::Impl>{
    std::in_place, impl_->state->begin_transfer(),
    options.receive_buffer_capacity}};
}

auto CurlSession::start_duplex(CurlStreamOptions options)
  -> CurlDuplexTransfer {
  return CurlDuplexTransfer{Box<CurlDuplexTransfer::Impl>{
    std::in_place, impl_->state->begin_transfer(), options}};
}

auto CurlSession::busy() const -> bool {
  return impl_->state->is_busy();
}

} // namespace tenzir
