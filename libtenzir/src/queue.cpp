//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"

#include <folly/coro/Mutex.h>
#include <folly/coro/UnboundedQueue.h>

#include <atomic>
#include <coroutine>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <utility>

namespace tenzir::queue_test {

template <class T>
concept Awaitable = requires(T x) {
  { x.await_ready() } -> std::convertible_to<bool>;
  { x.await_suspend(std::coroutine_handle<>{}) };
  { x.await_resume() };
};

// TODO: This should probably be a battle-tested implementation from somewhere
// else.
template <class T>
struct Channel {
  std::mutex mutex_;
  std::deque<T> queue_;
  // TODO: What if this gets destroyed with the waiters inside?
  std::deque<std::coroutine_handle<>> waiters_;
};

template <class T>
class ReceiveFuture {
public:
  explicit ReceiveFuture(Channel<T>& channel) : channel_{channel} {
  }

  struct Impl {
    explicit Impl(Channel<T>& channel) : channel_{channel} {
    }

    auto await_ready() -> bool {
      channel_.mutex_.lock();
      if (channel_.queue_.empty()) {
        // We will unlock the mutex when we get suspended.
        return false;
      }
      result_ = std::move(channel_.queue_.front());
      channel_.queue_.pop_front();
      channel_.mutex_.unlock();
      return true;
    }

    auto await_suspend(std::coroutine_handle<> next) -> void {
      channel_.waiters_.push_back(next);
      channel_.mutex_.unlock();
    }

    auto await_resume() -> T {
      auto result = std::move(result_);
      result_.~T();
      return result;
    }

    Channel<T>& channel_;
    union {
      T result_;
    };
  };

  auto operator co_await() && -> Impl {
    return Impl{channel_};
  }

private:
  Channel<T>& channel_;
};

template <class T>
class Sender {
public:
  explicit Sender(std::shared_ptr<Channel<T>> state)
    : state_{std::move(state)} {
  }

  void send(T value) {
    auto lock = std::scoped_lock{state_->mutex_};
    state_->queue_.push_back(std::move(value));
    if (not state_->waiters_.empty()) {
      state_->waiters_.front().resume();
      // TODO: Assert that the thing is gone.
      state_->waiters_.pop_front();
    }
  }

private:
  std::shared_ptr<Channel<T>> state_;
};

template <class Promise>
class owned_coroutine {
public:
  owned_coroutine(std::coroutine_handle<Promise> handle)
    : handle_{std::move(handle)} {
  }

  owned_coroutine(const owned_coroutine& other) = delete;
  auto operator=(const owned_coroutine& other) -> owned_coroutine& = delete;
  owned_coroutine(owned_coroutine&& other) noexcept {
    *this = std::move(other);
  }
  auto operator=(owned_coroutine&& other) noexcept -> owned_coroutine& {
    if (handle_) {
      handle_.destroy();
    }
    handle_ = std::exchange(other.handle_, nullptr);
    return *this;
  }

  ~owned_coroutine() {
    if (handle_) {
      handle_.destroy();
    }
  }

  auto release() && -> std::coroutine_handle<Promise> {
    return std::exchange(handle_, nullptr);
  }

private:
  std::coroutine_handle<Promise> handle_;
};

template <class T>
class FuturePromise;

template <class T>
class FuturePromiseBase;

/// The future associated with an actual coroutine.
///
/// This can also be considered a "type-erased" future, since you can always go
/// from a concrete future implementation to it by wrapping it in a coroutine.
template <class T>
class [[nodiscard]] Future {
public:
  using promise_type = FuturePromise<T>;

  class AwaitFuture {
  public:
    AwaitFuture(owned_coroutine<promise_type> self)
      : self_{std::move(self).release()} {
    }

    auto await_ready() -> bool {
      return false;
    }

    auto await_suspend(std::coroutine_handle<> next)
      -> std::coroutine_handle<> {
      TENZIR_ASSERT(not self_.promise().continuation_);
      self_.promise().continuation_ = next;
      return next;
    }

    auto await_resume() -> T {
      if constexpr (std::same_as<T, void>) {
        return;
      } else {
        return *std::exchange(self_.promise().result_, std::nullopt);
      }
    }

  private:
    std::coroutine_handle<promise_type> self_;
  };

  /*
    auto consume_response_promise(
      caf::typed_response_promise<T> rp) && -> caf::result<T> {
      // We have to leak ourselves here, right? Or attach to actor?
      auto leaked = new Task{std::move(handle_)};
      // rp.begin();
      return rp;
    }
  */

  auto operator co_await() && -> AwaitFuture {
    return AwaitFuture{std::move(handle_)};
  }

  auto unwrap() && -> owned_coroutine<promise_type> {
    return std::move(handle_);
  }

private:
  friend class FuturePromiseBase<T>;

  explicit Future(owned_coroutine<promise_type> handle)
    : handle_{std::move(handle)} {
  }

  owned_coroutine<promise_type> handle_;
};

template <class T>
class FuturePromiseBase {
public:
  static auto initial_suspend() -> std::suspend_always {
    return {};
  }

  static auto final_suspend() noexcept -> std::suspend_always {
    return {};
  }

  auto get_return_object() -> Future<T> {
    return Future<T>{std::coroutine_handle<FuturePromise<T>>::from_promise(
      static_cast<FuturePromise<T>&>(*this))};
  }

  void unhandled_exception() {
    // TODO
  }

  std::coroutine_handle<> continuation_;
};

template <class T>
class FuturePromise : public FuturePromiseBase<T> {
public:
  void return_value(T value) {
    // TODO
    result_.emplace(std::move(value));
    TENZIR_ASSERT(this->continuation_);
    this->continuation_.resume();
  }

  std::optional<T> result_;
};

template <>
class FuturePromise<void> : public FuturePromiseBase<void> {
public:
  void return_void() {
    TENZIR_ASSERT(this->continuation_);
    this->continuation_.resume();
  }
};

class FireAndForget {
public:
  using promise_type = FireAndForget;

  static auto initial_suspend() -> std::suspend_never {
    // Don't suspend so that we immediately start the coroutine.
    return {};
  }

  static auto final_suspend() noexcept -> std::suspend_never {
    // Automatic cleanup.
    return {};
  }

  auto get_return_object() -> FireAndForget {
    return {};
  }

  void unhandled_exception() {
    // TODO
  }

  void return_void() {
    // no-op
  }
};

template<class Future, class Callback>
void run_with_callback(Future future, Callback callback) {
  (void)std::invoke([](Future future, Callback callback) -> FireAndForget {
    std::invoke(std::move(callback), co_await future);
  }, std::move(future), std::move(callback));
}

template <class T>
class Receiver {
public:
  explicit Receiver(std::shared_ptr<Channel<T>> state)
    : state_{std::move(state)} {
  }

  auto receive() -> ReceiveFuture<T> {
    return ReceiveFuture<T>{*state_};
  }

  template<class Callback>
  auto receive(Callback callback) -> void {
    run_with_callback(receive().operator co_await(), std::move(callback));
  }

private:
  std::shared_ptr<Channel<T>> state_;
};

template<class T>
struct SenderReceiver {
  Sender<T> sender;
  Receiver<T> receiver;
};

template <class T>
auto make_channel() -> SenderReceiver<T> {
  auto channel = std::make_shared<Channel<T>>();
  return {
    Sender{channel},
    Receiver{std::move(channel)},
  };
}

struct RawAsyncMutexImpl {
  std::mutex mutex;
  bool locked{false};
  std::deque<std::coroutine_handle<>> waiters;

  void unlock() {
    auto lock = std::unique_lock{mutex};
    TENZIR_ASSERT(locked);
    if (waiters.empty()) {
      locked = false;
      return;
    }
    // Transfer the lock to the first waiter.
    auto waiter = waiters.front();
    waiters.pop_front();
    lock.unlock();
    waiter.resume();
  }
};

class [[nodiscard]] RawMutexFuture {
public:
  explicit RawMutexFuture(RawAsyncMutexImpl& data) : data_{data} {}

  auto await_ready() -> bool {
    data_.mutex.lock();
    if (data_.locked) {
      return false;
    }
    data_.locked = true;
    return true;
  }

  auto await_suspend(std::coroutine_handle<> next) -> void {
    data_.waiters.push_back(next);
    data_.mutex.unlock();
  }

  auto await_resume() -> void {
    // The resumer does the locking for us.
  }

private:
  RawAsyncMutexImpl& data_;
};

class [[nodiscard]] RawMutexGuard {
public:
  explicit RawMutexGuard(RawAsyncMutexImpl& impl) : impl_{&impl} {
  }

  ~RawMutexGuard() {
    if (impl_) {
      impl_->unlock();
    }
  }

  RawMutexGuard(const RawMutexGuard& other) = delete;
  auto operator=(const RawMutexGuard& other) = delete;
  RawMutexGuard(RawMutexGuard&& other) noexcept : impl_(other.impl_) {
    other.impl_ = nullptr;
  }
  auto operator=(RawMutexGuard&& other) noexcept -> RawMutexGuard& {
    impl_ = other.impl_;
    other.impl_ = nullptr;
    return *this;
  }

private:
  RawAsyncMutexImpl* impl_;
};

class RawMutex {
public:
  auto lock() -> Future<RawMutexGuard> {
    co_await lock_without_guard();
    co_return RawMutexGuard{impl_};
  }

  auto lock_without_guard() -> RawMutexFuture {
    return RawMutexFuture{impl_};
  }

  auto unlock_without_guard() {
    impl_.unlock();
  }

private:
  RawAsyncMutexImpl impl_;
};

template <class T>
class MutexGuard2;

template <class T>
class Mutex2 {
public:
  auto lock() & -> Future<MutexGuard2<T>> {
    co_await raw_.lock_without_guard();
    co_return MutexGuard2<T>{*this};
  }

  auto unsafe_get() & -> T& {
    return data_;
  }

  auto unsafe_raw() -> RawMutex& {
    return raw_;
  }

private:
  RawMutex raw_;
  T data_;
};

template <class T>
class [[nodiscard]] MutexGuard2 {
public:
  // TODO: Private.
  explicit MutexGuard2(Mutex2<T>& mutex) : mutex_{&mutex} {
  }

  ~MutexGuard2() {
    drop();
  }

  MutexGuard2(MutexGuard2&& other) noexcept {
    *this = std::move(other);
  }
  auto operator=(MutexGuard2&& other) noexcept -> MutexGuard2& {
    drop();
    std::swap(mutex_, other.mutex_);
    return *this;
  }
  MutexGuard2(MutexGuard2& other) = delete;
  auto operator=(MutexGuard2& other) = delete;

  // auto operator co_await() && -> T& {
  //   return mutex_.data_;
  // }

  auto operator->() -> T* {
    TENZIR_ASSERT(mutex_);
    return &mutex_->unsafe_get();
  }

  auto get_locked_mutex() -> Mutex2<T>& {
    TENZIR_ASSERT(mutex_);
    return *mutex_;
  }

  void drop() {
    if (mutex_) {
      mutex_->unsafe_raw().unlock_without_guard();
    }
    mutex_ = nullptr;
  }

private:
  Mutex2<T>* mutex_ = nullptr;
};

// class ConditionVariable {
// public:
//   class FutureGuard {
//   public:
//     auto await_ready() -> bool {
//       return false;
//     }

//     auto await_suspend(std::coroutine_handle<> handle) -> void {

//     }

//     auto await_resume() -> MutexGuard<T> {

//     }
//   };

//   template<class T>
//   auto wait(MutexGuard<T> mutex) -> Future<MutexGuard<T>> {
//     co_await FutureGuard{};
//     co_return co_await mutex.get_locked_mutex().lock();
//   }

//   void notify_one() {
//     auto guard = waiters_.lock();
//   }

// private:
//   Mutex<std::deque<std::coroutine_handle<>>> waiters_;
// };

// struct BatchShared {
//   size_t remaining{100};
//   ConditionVariable cv;
// };

// class BatchSender {
// public:
//   auto send(size_t x) -> Future<void> {
//     auto guard = co_await shared_->lock();
//     while (x <= guard->remaining) {
//       guard = co_await guard->cv.wait(std::move(guard));
//     }
//     sender_.send(x);
//     guard->remaining -= x;
//   }

// private:
//   Sender<size_t> sender_;
//   std::shared_ptr<Mutex<BatchShared>> shared_;
// };

template <class T>
class MutexGuard;

template <class T>
class Mutex {
public:
  explicit Mutex(T x) : value_{std::move(x)} {
  }

  auto lock() -> folly::coro::Task<MutexGuard<T>>;

private:
  friend class MutexGuard<T>;

  folly::coro::Mutex mutex_;
  T value_;
};

template <class T>
class MutexGuard {
public:
  ~MutexGuard() noexcept {
    try_unlock();
  }

  MutexGuard(MutexGuard&& other) noexcept {
    *this = std::move(other);
  }
  auto operator=(MutexGuard&& other) noexcept -> MutexGuard& {
    try_unlock();
    locked_ = other.locked_;
    other.locked_ = nullptr;
    return *this;
  }
  MutexGuard(MutexGuard& other) = delete;
  auto operator=(MutexGuard& other) = delete;

  auto operator*() -> T& {
    TENZIR_ASSERT(locked_);
    return locked_->value_;
  }

  auto operator->() -> T* {
    TENZIR_ASSERT(locked_);
    return &locked_->value_;
  }

  auto unlock() -> void {
    TENZIR_ASSERT(locked_);
    locked_->mutex_.unlock();
    locked_ = nullptr;
  }

private:
  auto try_unlock() -> void {
    if (locked_) {
      locked_->mutex_.unlock();
    }
  }

  friend class Mutex<T>;

  explicit MutexGuard(Mutex<T>& mutex) : locked_{&mutex} {
  }

  Mutex<T>* locked_;
};

template <class T>
auto Mutex<T>::lock() -> folly::coro::Task<MutexGuard<T>> {
  co_await mutex_.co_lock();
  co_return MutexGuard<T>{*this};
}

struct BatchShared {
  struct Locked {
    explicit Locked(int limit) : remaining{limit} {
    }

    int remaining;
    std::deque<int> queue;
  };

  explicit BatchShared(int limit) : mutex{Locked{limit}} {
  }

  // TODO: This can surely be written better?
  Mutex<Locked> mutex;
  folly::coro::Baton remaining_increased;
  folly::coro::Baton queue_pushed;
};

class BatchSender {
public:
  BatchSender(std::shared_ptr<BatchShared> shared)
    : shared_{std::move(shared)} {
  }

  auto send(int x) -> folly::coro::Task<void> {
    auto lock = co_await shared_->mutex.lock();
    while (x > lock->remaining) {
      lock.unlock();
      co_await shared_->remaining_increased;
      shared_->remaining_increased.reset();
      lock = co_await shared_->mutex.lock();
    }
    lock->remaining -= x;
    lock->queue.push_back(x);
    shared_->queue_pushed.post();
  }

private:
  std::shared_ptr<BatchShared> shared_;
};

class BatchReceiver {
public:
  BatchReceiver(std::shared_ptr<BatchShared> shared)
    : shared_{std::move(shared)} {
  }

  auto receive() -> folly::coro::Task<int> {
    auto lock = co_await shared_->mutex.lock();
    while (lock->queue.empty()) {
      lock.unlock();
      co_await shared_->queue_pushed;
      shared_->queue_pushed.reset();
      lock = co_await shared_->mutex.lock();
    }
    auto result = std::move(lock->queue.front());
    lock->queue.pop_front();
    lock->remaining += result;
    shared_->remaining_increased.post();
    co_return result;
  }

private:
  std::shared_ptr<BatchShared> shared_;
  // The channel is not shared because the token is not thread-safe.
};

auto make_channel(int limit) -> std::pair<BatchSender, BatchReceiver> {
  auto shared = std::make_shared<BatchShared>(limit);
  auto sender = BatchSender{shared};
  auto receiver = BatchReceiver{std::move(shared)};
  return {std::move(sender), std::move(receiver)};
}

// auto example2(BatchSender sender) -> Future<int> {
//   co_await sender.send(42);
//   co_return 123;
// }

auto example2() -> Future<int> {
  auto [sender, receiver] = make_channel<int>();
  sender.send(42);
  auto n = co_await receiver.receive();
  receiver.receive([](int n) {
    (void)n;
  });
  co_return n;
}

auto example() -> folly::coro::Task<int> {
  auto [sender, receiver] = make_channel(42);
  co_await sender.send(42);
  auto n = co_await receiver.receive();
  co_return n;
}

} // namespace tenzir::queue_test
