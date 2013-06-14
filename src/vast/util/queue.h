#ifndef VAST_UTIL_QUEUE_H
#define VAST_UTIL_QUEUE_H

#include <condition_variable>
#include <mutex>
#include <queue>

namespace vast {
namespace util {

/// A thread-safe `std::queue`.
template <typename T>
class queue : std::queue<T>
{
  queue(queue&) = delete;
  queue& operator=(queue&) = delete;
  typedef std::queue<T> super;

public:
  typedef typename super::value_type value_type;
  typedef typename super::reference reference;
  typedef typename super::const_reference const_reference;

  using super::size;
  using super::empty;

  /// Constructs an empty queue.
  queue() = default;

  /// Pushes a new element to the end of the queue.
  /// @param x The value to push in the queue.
  /// @note The notification occurs *after* the mutex is unlocked, thus the
  /// waiting thread will be able to acquire the mutex without blocking.
  void push(value_type x)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    const bool empty = super::empty();
    super::push(std::move(x));

    lock.unlock();

    if (empty)
      cond_.notify_one();
  }

  /// Pushes a new element to the end of the queue. The element is
  /// constructed in-place, i.e., no copy or move operations are performed.
  /// The constructor of the element is called with exactly the same
  /// arguments, as supplied to the function.
  template <typename... Args>
  void emplace(Args&&... args)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    bool const empty = super::empty();
    super::emplace(std::forward<Args>(args)...);

    lock.unlock();

    if (empty)
      cond_.notify_one();
  }

  /// Gets the top-most element or wait until an element is added. To avoid
  /// exception safety issues when returning data by-value, the queue uses a
  /// reference parameter to receive the result, transferring the ownership
  /// out of the queue. If the copy constructor of type `T` throws, the
  /// element is not removed from the queue.
  ///
  /// @note This function blocks when the queue is empty.
  value_type pop()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    while (super::empty())
      cond_.wait(lock);

    value_type x = std::move(super::front());
    super::pop();

    return x;
  }

  /// Tries to get the top-most queue element.
  /// @param x The reference parameter receiving the result.
  ///
  /// @return `true` if the top-most element could be returned and `false` if the
  /// queue is empty.
  bool try_pop(value_type& x)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (super::empty())
      return false;

    x = std::move(super::front());
    super::pop();

    return true;
  }

private:
  std::mutex mutex_;
  std::condition_variable cond_;
};

} // namespace util
} // namespace vast

#endif
