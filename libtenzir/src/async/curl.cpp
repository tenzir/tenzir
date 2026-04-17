//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/curl.hpp"

#include "tenzir/async/notify.hpp"

#include <curl/curl.h>
#include <folly/CancellationToken.h>
#include <folly/coro/Baton.h>
#include <folly/coro/Error.h>
#include <folly/coro/Task.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>

#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace tenzir {

namespace detail {

struct CurlBodyAccess {
  static auto wait_until_ready(CurlUploadBody& body) -> Task<bool> {
    co_return co_await body.wait_until_ready();
  }

  static auto read(CurlUploadBody& body, std::span<std::byte> buffer)
    -> size_t {
    return body.read(buffer);
  }

  static auto set_resume_callback(CurlUploadBody& body,
                                  std::function<void()> callback) -> void {
    body.set_resume_callback(std::move(callback));
  }

  static auto terminate(CurlUploadBody& body) -> void {
    body.terminate();
  }

  static auto write(CurlDownloadBody& body, std::span<const std::byte> buffer)
    -> size_t {
    return body.write(buffer);
  }

  static auto set_resume_callback(CurlDownloadBody& body,
                                  std::function<void()> callback) -> void {
    body.set_resume_callback(std::move(callback));
  }
};

} // namespace detail

struct CurlUploadBody::Impl {
  explicit Impl(size_t capacity) : capacity{capacity} {
    TENZIR_ASSERT(capacity > 0);
  }

  auto push(chunk_ptr chunk) -> Task<bool> {
    TENZIR_ASSERT(chunk);
    while (true) {
      auto resume = std::function<void()>{};
      {
        auto lock = std::unique_lock{mutex};
        if (aborted or closed or terminated) {
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
      if (closed or aborted or terminated) {
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
      if (aborted or terminated) {
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

  auto terminate() -> void {
    auto resume = std::function<void()>{};
    {
      auto lock = std::lock_guard{mutex};
      if (terminated) {
        return;
      }
      terminated = true;
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
        if (closed or terminated) {
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
        if (aborted) {
          co_return false;
        }
        if (not buffered.empty()) {
          co_return true;
        }
        if (closed or terminated) {
          co_return false;
        }
      }
      co_await data_ready.wait();
    }
  }

  auto is_aborted() -> bool {
    auto lock = std::lock_guard{mutex};
    return aborted;
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
  bool terminated = false;
  bool paused = false;
  std::function<void()> resume_callback;
};

struct CurlDownloadBody::Impl {
  explicit Impl(size_t capacity) : capacity{capacity} {
    TENZIR_ASSERT(capacity > 0);
  }

  auto pop() -> Task<Option<chunk_ptr>> {
    while (true) {
      auto chunk = chunk_ptr{};
      auto resume = std::function<void()>{};
      {
        auto lock = std::unique_lock{mutex};
        if (aborted) {
          co_return None{};
        }
        if (not buffered.empty()) {
          chunk = std::move(buffered.front());
          buffered.pop_front();
          if (paused) {
            paused = false;
            resume = resume_callback;
          }
        } else if (closed) {
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
      buffered.clear();
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

  auto is_aborted() -> bool {
    auto lock = std::lock_guard{mutex};
    return aborted;
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
      detail::CurlBodyAccess::set_resume_callback(*upload_body_, [weak]() {
        if (auto self = weak.lock()) {
          self->request_resume_send();
        }
      });
      auto code = easy_.set([this](std::span<std::byte> buffer) -> size_t {
        return read_from_source(buffer);
      });
      if (code != curl::easy::code::ok) {
        complete(CurlPerformResult::failure(curl::to_error(code)));
        return;
      }
    }
    if (download_body_) {
      auto weak = weak_from_this();
      detail::CurlBodyAccess::set_resume_callback(*download_body_, [weak]() {
        if (auto self = weak.lock()) {
          self->request_resume_recv();
        }
      });
      auto code = easy_.set_write_result_callback(
        [this](std::span<const std::byte> buffer) -> size_t {
          return write_to_sink(buffer);
        });
      if (code != curl::easy::code::ok) {
        complete(CurlPerformResult::failure(curl::to_error(code)));
        return;
      }
    }
    auto code = multi_.set_socket_callback(&socket_callback, this);
    if (code != curl::multi::code::ok) {
      complete(CurlPerformResult::failure(curl::to_error(code)));
      return;
    }
    code = multi_.set_timer_callback(&timer_callback, this);
    if (code != curl::multi::code::ok) {
      complete(CurlPerformResult::failure(curl::to_error(code)));
      return;
    }
    code = multi_.add(easy_);
    if (code != curl::multi::code::ok) {
      complete(CurlPerformResult::failure(curl::to_error(code)));
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
    TENZIR_ASSERT(upload_body_);
    auto result = detail::CurlBodyAccess::read(*upload_body_, buffer);
    if (result == CURL_READFUNC_PAUSE) {
      paused_send_ = true;
    }
    return result;
  }

  auto write_to_sink(std::span<const std::byte> buffer) -> size_t {
    TENZIR_ASSERT(download_body_);
    auto result = detail::CurlBodyAccess::write(*download_body_, buffer);
    if (result == CURL_WRITEFUNC_PAUSE) {
      paused_recv_ = true;
    }
    return result;
  }

  auto request_resume_send() -> void {
    auto self = shared_from_this();
    evb_->runInEventBaseThread([self = std::move(self)]() {
      self->resume_send();
    });
  }

  auto request_resume_recv() -> void {
    auto self = shared_from_this();
    evb_->runInEventBaseThread([self = std::move(self)]() {
      self->resume_recv();
    });
  }

  auto request_cancel() -> void {
    auto self = shared_from_this();
    evb_->runInEventBaseThread([self = std::move(self)]() {
      self->cancel();
    });
  }

  auto resume_send() -> void {
    if (completed_ or not paused_send_) {
      return;
    }
    paused_send_ = false;
    auto code = easy_.pause(current_pause_mask());
    if (code != curl::easy::code::ok) {
      complete(CurlPerformResult::failure(curl::to_error(code)));
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
      complete(CurlPerformResult::failure(curl::to_error(code)));
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
      complete(CurlPerformResult::failure(curl::to_error(code)));
      return;
    }
    for (auto result : multi_.info_read()) {
      if (result != curl::easy::code::ok) {
        complete(CurlPerformResult::failure(curl::to_error(result)));
        return;
      }
      complete(CurlPerformResult::success());
      return;
    }
  }

  auto cancel() -> void {
    if (completed_) {
      return;
    }
    cancelled_ = true;
    if (upload_body_) {
      upload_body_->abort();
    }
    if (download_body_) {
      download_body_->abort();
    }
    complete(CurlPerformResult::success());
  }

  auto cleanup() -> void {
    if (upload_body_) {
      detail::CurlBodyAccess::set_resume_callback(*upload_body_, {});
      detail::CurlBodyAccess::terminate(*upload_body_);
    }
    if (download_body_) {
      detail::CurlBodyAccess::set_resume_callback(*download_body_, {});
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
  CurlUploadBody* upload_body_ = nullptr;
  CurlDownloadBody* download_body_ = nullptr;
  curl::multi multi_;
  std::unordered_map<curl_socket_t, Box<SocketHandler>> socket_handlers_;
  TimerHandler timer_;
  folly::coro::Baton done_;
  CurlPerformResult result_ = CurlPerformResult::success();
  bool added_ = false;
  bool completed_ = false;
  bool cancelled_ = false;
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

} // namespace

namespace {

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

} // namespace

auto perform_curl(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                  curl::easy& handle) -> Task<CurlPerformResult> {
  co_return co_await perform_curl_impl(std::move(executor), handle, nullptr,
                                       nullptr);
}

auto perform_curl_upload(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                         curl::easy& handle, CurlUploadBody& body)
  -> Task<CurlPerformResult> {
  auto should_start_transfer = false;
  try {
    should_start_transfer
      = co_await detail::CurlBodyAccess::wait_until_ready(body);
  } catch (folly::OperationCancelled const&) {
    body.abort();
    throw;
  }
  if (body.is_aborted()) {
    co_return CurlPerformResult::local_abort();
  }
  if (not should_start_transfer) {
    co_return CurlPerformResult::success();
  }
  auto result
    = co_await perform_curl_impl(std::move(executor), handle, &body, nullptr);
  if (body.is_aborted()) {
    co_return CurlPerformResult::local_abort();
  }
  co_return result;
}

auto perform_curl_download(
  folly::Executor::KeepAlive<folly::IOExecutor> executor, curl::easy& handle,
  CurlDownloadBody& body) -> Task<CurlPerformResult> {
  auto result
    = co_await perform_curl_impl(std::move(executor), handle, nullptr, &body);
  if (body.is_aborted()) {
    co_return CurlPerformResult::local_abort();
  }
  co_return result;
}

} // namespace tenzir
