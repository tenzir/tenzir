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

#include <algorithm>
#include <chrono>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <unordered_map>
#include <utility>

namespace tenzir {

namespace {

using CurlPerformResult = CurlTransferResult;

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

class UploadStream {
public:
  explicit UploadStream(size_t capacity) : capacity_{capacity} {
    TENZIR_ASSERT(capacity > 0);
  }

  auto push(chunk_ptr chunk) -> Task<bool> {
    TENZIR_ASSERT(chunk);
    while (true) {
      auto resume = std::function<void()>{};
      {
        auto lock = std::unique_lock{mutex_};
        if (state_ != State::open) {
          co_return false;
        }
        if (buffered_.size() < capacity_) {
          buffered_.push_back(std::move(chunk));
          resume = take_resume_callback();
          lock.unlock();
          data_ready_.notify_one();
          if (resume) {
            resume();
          }
          co_return true;
        }
      }
      co_await space_available_.wait();
    }
  }

  auto close() -> void {
    wakeup(transition_to_closed());
  }

  auto abort() -> void {
    wakeup(transition_to_aborted());
  }

  auto terminate() -> void {
    wakeup(transition_to_terminated());
  }

  auto wait_until_ready() -> Task<bool> {
    while (true) {
      {
        auto lock = std::lock_guard{mutex_};
        if (is_aborted_state()) {
          co_return false;
        }
        if (not buffered_.empty()) {
          co_return true;
        }
        if (state_ != State::open) {
          co_return false;
        }
      }
      co_await data_ready_.wait();
    }
  }

  auto is_aborted() const -> bool {
    auto lock = std::lock_guard{mutex_};
    return is_aborted_state();
  }

  auto read(std::span<std::byte> buffer) -> size_t {
    auto notify_space = false;
    auto written = size_t{0};
    {
      auto lock = std::lock_guard{mutex_};
      if (is_aborted_state()) {
        return CURL_READFUNC_ABORT;
      }
      if (buffered_.empty()) {
        if (state_ != State::open) {
          return 0;
        }
        paused_ = true;
        return CURL_READFUNC_PAUSE;
      }
      while (written < buffer.size() and not buffered_.empty()) {
        auto const& front = buffered_.front();
        auto remaining = front->size() - front_offset_;
        auto count = std::min(buffer.size() - written, remaining);
        std::memcpy(buffer.data() + written, front->data() + front_offset_,
                    count);
        written += count;
        front_offset_ += count;
        if (front_offset_ == front->size()) {
          buffered_.pop_front();
          front_offset_ = 0;
          notify_space = true;
        }
      }
    }
    if (notify_space) {
      space_available_.notify_one();
    }
    return written;
  }

  auto set_resume_callback(std::function<void()> callback) -> void {
    auto lock = std::lock_guard{mutex_};
    resume_callback_ = std::move(callback);
  }

private:
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

  auto is_aborted_state() const -> bool {
    return state_ == State::aborted or state_ == State::aborted_terminated;
  }

  auto take_resume_callback() -> std::function<void()> {
    if (not paused_) {
      return {};
    }
    paused_ = false;
    return resume_callback_;
  }

  auto transition_to_closed() -> Transition {
    auto lock = std::lock_guard{mutex_};
    if (state_ != State::open) {
      return {};
    }
    state_ = State::closed;
    return {.changed = true, .resume = take_resume_callback()};
  }

  auto transition_to_aborted() -> Transition {
    auto lock = std::lock_guard{mutex_};
    if (is_aborted_state() or state_ == State::terminated) {
      return {};
    }
    state_ = State::aborted;
    return {.changed = true, .resume = take_resume_callback()};
  }

  auto transition_to_terminated() -> Transition {
    auto lock = std::lock_guard{mutex_};
    if (state_ == State::terminated or state_ == State::aborted_terminated) {
      return {};
    }
    state_ = is_aborted_state() ? State::aborted_terminated : State::terminated;
    return {.changed = true, .resume = take_resume_callback()};
  }

  auto wakeup(Transition transition) -> void {
    if (not transition.changed) {
      return;
    }
    data_ready_.notify_one();
    space_available_.notify_one();
    if (transition.resume) {
      transition.resume();
    }
  }

  mutable std::mutex mutex_;
  Notify data_ready_;
  Notify space_available_;
  std::deque<chunk_ptr> buffered_;
  size_t capacity_ = 0;
  size_t front_offset_ = 0;
  State state_ = State::open;
  bool paused_ = false;
  std::function<void()> resume_callback_;
};

class DownloadStream {
public:
  explicit DownloadStream(size_t capacity) : capacity_{capacity} {
    TENZIR_ASSERT(capacity > 0);
  }

  auto pop() -> Task<Option<chunk_ptr>> {
    while (true) {
      auto chunk = chunk_ptr{};
      auto resume = std::function<void()>{};
      {
        auto lock = std::unique_lock{mutex_};
        if (state_ == State::aborted) {
          co_return None{};
        }
        if (not buffered_.empty()) {
          chunk = std::move(buffered_.front());
          buffered_.pop_front();
          resume = take_resume_callback();
        } else if (state_ == State::closed) {
          co_return None{};
        }
      }
      if (chunk) {
        if (resume) {
          resume();
        }
        co_return chunk;
      }
      co_await data_available_.wait();
    }
  }

  auto abort() -> void {
    auto transition = transition_to_aborted();
    if (not transition.changed) {
      return;
    }
    data_available_.notify_one();
    if (transition.resume) {
      transition.resume();
    }
  }

  auto close() -> void {
    if (transition_to_closed().changed) {
      data_available_.notify_one();
    }
  }

  auto is_aborted() const -> bool {
    auto lock = std::lock_guard{mutex_};
    return state_ == State::aborted;
  }

  auto write(std::span<const std::byte> buffer) -> size_t {
    auto lock = std::lock_guard{mutex_};
    if (state_ == State::aborted) {
      return CURL_WRITEFUNC_ERROR;
    }
    if (buffered_.size() >= capacity_) {
      paused_ = true;
      return CURL_WRITEFUNC_PAUSE;
    }
    buffered_.push_back(chunk::copy(buffer));
    data_available_.notify_one();
    return buffer.size();
  }

  auto set_resume_callback(std::function<void()> callback) -> void {
    auto lock = std::lock_guard{mutex_};
    resume_callback_ = std::move(callback);
  }

private:
  enum class State {
    open,
    closed,
    aborted,
  };

  struct Transition {
    bool changed = false;
    std::function<void()> resume;
  };

  auto take_resume_callback() -> std::function<void()> {
    if (not paused_) {
      return {};
    }
    paused_ = false;
    return resume_callback_;
  }

  auto transition_to_closed() -> Transition {
    auto lock = std::lock_guard{mutex_};
    if (state_ != State::open) {
      return {};
    }
    state_ = State::closed;
    return {.changed = true};
  }

  auto transition_to_aborted() -> Transition {
    auto lock = std::lock_guard{mutex_};
    if (state_ == State::aborted) {
      return {};
    }
    state_ = State::aborted;
    buffered_.clear();
    return {.changed = true, .resume = take_resume_callback()};
  }

  mutable std::mutex mutex_;
  Notify data_available_;
  std::deque<chunk_ptr> buffered_;
  size_t capacity_ = 0;
  State state_ = State::open;
  bool paused_ = false;
  std::function<void()> resume_callback_;
};

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
               curl::easy& easy, UploadStream* upload, DownloadStream* download)
    : executor_{std::move(executor)},
      evb_{executor_->getEventBase()},
      easy_{easy},
      upload_{upload},
      download_{download},
      timer_{evb_, *this} {
    TENZIR_ASSERT(evb_ != nullptr);
  }

  auto start() -> void {
    if (upload_) {
      auto weak = weak_from_this();
      upload_->set_resume_callback([weak]() {
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
    if (download_) {
      auto weak = weak_from_this();
      download_->set_resume_callback([weak]() {
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
    TENZIR_ASSERT(upload_);
    auto result = upload_->read(buffer);
    if (result == CURL_READFUNC_PAUSE) {
      pause_mask_ |= CURLPAUSE_SEND;
    }
    return result;
  }

  auto write_to_sink(std::span<const std::byte> buffer) -> size_t {
    TENZIR_ASSERT(download_);
    auto result = download_->write(buffer);
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
      complete(CurlTransferStatus::finished);
      return;
    }
  }

  auto cancel() -> void {
    if (is_completed()) {
      return;
    }
    cancelled_ = true;
    if (upload_) {
      upload_->abort();
    }
    if (download_) {
      download_->abort();
    }
    complete(CurlTransferStatus::finished);
  }

  auto cleanup() -> void {
    if (upload_) {
      upload_->set_resume_callback({});
      upload_->terminate();
    }
    if (download_) {
      download_->set_resume_callback({});
      download_->close();
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
  UploadStream* upload_ = nullptr;
  DownloadStream* download_ = nullptr;
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

auto perform_curl(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                  curl::easy& handle, UploadStream* upload,
                  DownloadStream* download) -> Task<CurlPerformResult> {
  auto state_executor = executor;
  auto task = [state_executor = std::move(state_executor), &handle, upload,
               download]() mutable -> Task<CurlPerformResult> {
    auto state = std::make_shared<PerformState>(std::move(state_executor),
                                                handle, upload, download);
    state->start();
    co_await state->wait();
    co_return state->take_result();
  };
  co_return co_await folly::coro::co_withExecutor(std::move(executor), task());
}

enum class TransferDirection {
  send,
  receive,
};

struct CurlTransferState;

struct CurlSessionState final
  : public std::enable_shared_from_this<CurlSessionState> {
  explicit CurlSessionState(
    folly::Executor::KeepAlive<folly::IOExecutor> executor)
    : executor{std::move(executor)} {
  }

  auto begin(TransferDirection direction, size_t capacity)
    -> std::shared_ptr<CurlTransferState>;

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

struct CurlTransferState final {
  CurlTransferState(std::shared_ptr<CurlSessionState> session,
                    TransferDirection direction, size_t capacity)
    : session{std::move(session)} {
    switch (direction) {
      case TransferDirection::send:
        upload.emplace(std::in_place, capacity);
        break;
      case TransferDirection::receive:
        download.emplace(std::in_place, capacity);
        break;
    }
  }

  auto push(chunk_ptr chunk) -> Task<bool> {
    TENZIR_ASSERT(upload);
    co_return co_await (*upload)->push(std::move(chunk));
  }

  auto close() -> void {
    TENZIR_ASSERT(upload);
    (*upload)->close();
  }

  auto next() -> Task<Option<chunk_ptr>> {
    TENZIR_ASSERT(download);
    co_return co_await (*download)->pop();
  }

  auto abort() -> void {
    if (upload) {
      (*upload)->abort();
    }
    if (download) {
      (*download)->abort();
    }
  }

  auto dispose() -> void {
    abort();
    cancellation.requestCancellation();
    auto release_without_wait = false;
    {
      auto lock = std::lock_guard{mutex};
      release_without_wait = not waiting;
    }
    if (release_without_wait) {
      finish();
    }
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

  auto run() -> Task<CurlPerformResult> {
    if (upload) {
      co_return co_await run_upload();
    }
    TENZIR_ASSERT(download);
    co_return co_await run_download();
  }

  std::shared_ptr<CurlSessionState> session;
  folly::CancellationSource cancellation;

private:
  auto run_upload() -> Task<CurlPerformResult> {
    auto& body = **upload;
    auto should_start_transfer = false;
    try {
      should_start_transfer = co_await body.wait_until_ready();
    } catch (folly::OperationCancelled const&) {
      body.abort();
      throw;
    }
    if (body.is_aborted()) {
      co_return CurlTransferStatus::local_abort;
    }
    if (not should_start_transfer) {
      co_return CurlTransferStatus::finished;
    }
    auto result
      = co_await perform_curl(session->executor, session->easy, &body, nullptr);
    if (body.is_aborted()) {
      co_return CurlTransferStatus::local_abort;
    }
    co_return result;
  }

  auto run_download() -> Task<CurlPerformResult> {
    auto& body = **download;
    if (body.is_aborted()) {
      co_return CurlTransferStatus::local_abort;
    }
    auto result
      = co_await perform_curl(session->executor, session->easy, nullptr, &body);
    if (body.is_aborted()) {
      co_return CurlTransferStatus::local_abort;
    }
    co_return result;
  }

  Option<Box<UploadStream>> upload = None{};
  Option<Box<DownloadStream>> download = None{};
  mutable std::mutex mutex;
  bool waiting = false;
  bool finished = false;
};

auto CurlSessionState::begin(TransferDirection direction, size_t capacity)
  -> std::shared_ptr<CurlTransferState> {
  TENZIR_ASSERT(capacity > 0);
  auto lock = std::lock_guard{mutex};
  if (busy) {
    panic("curl session already has an active transfer");
  }
  busy = true;
  return std::make_shared<CurlTransferState>(shared_from_this(), direction,
                                             capacity);
}

struct TransferFinishGuard {
  ~TransferFinishGuard() {
    state->finish();
  }

  std::shared_ptr<CurlTransferState> state;
};

} // namespace

struct CurlTransfer::Impl {
  explicit Impl(std::shared_ptr<CurlTransferState> state)
    : state{std::move(state)} {
  }

  ~Impl() {
    if (not state->is_finished()) {
      state->dispose();
    }
  }

  std::shared_ptr<CurlTransferState> state;
};

CurlTransfer::CurlTransfer(Box<Impl> impl) : impl_{std::move(impl)} {
}

CurlTransfer::~CurlTransfer() = default;

CurlTransfer::CurlTransfer(CurlTransfer&&) noexcept = default;

auto CurlTransfer::operator=(CurlTransfer&&) noexcept
  -> CurlTransfer& = default;

auto CurlTransfer::push(chunk_ptr chunk) -> Task<bool> {
  co_return co_await impl_->state->push(std::move(chunk));
}

auto CurlTransfer::close() -> void {
  impl_->state->close();
}

auto CurlTransfer::abort() -> void {
  impl_->state->abort();
}

auto CurlTransfer::next() -> Task<Option<chunk_ptr>> {
  co_return co_await impl_->state->next();
}

auto CurlTransfer::wait() -> Task<CurlTransferResult> {
  auto state = impl_->state;
  state->mark_waiting();
  auto guard = TransferFinishGuard{state};
  auto token = folly::cancellation_token_merge(
    co_await folly::coro::co_current_cancellation_token,
    state->cancellation.getToken());
  co_return co_await folly::coro::co_withCancellation(token, state->run());
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

auto CurlSession::start_send(size_t buffer_capacity) -> CurlTransfer {
  return CurlTransfer{Box<CurlTransfer::Impl>{
    std::in_place,
    impl_->state->begin(TransferDirection::send, buffer_capacity)}};
}

auto CurlSession::start_receive(size_t buffer_capacity) -> CurlTransfer {
  return CurlTransfer{Box<CurlTransfer::Impl>{
    std::in_place,
    impl_->state->begin(TransferDirection::receive, buffer_capacity)}};
}

auto CurlSession::busy() const -> bool {
  return impl_->state->is_busy();
}

} // namespace tenzir
