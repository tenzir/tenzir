#ifndef VAST_UTIL_QUEUE_H
#define VAST_UTIL_QUEUE_H

#include <condition_variable>
#include <mutex>
#include <queue>

namespace vast {
namespace util {

/// A thread-safe @c std::queue.
template <typename T>
class queue : std::queue<T>
{
    typedef std::queue<T> base;

public:
    typedef typename base::value_type value_type;
    typedef typename base::reference reference;
    typedef typename base::const_reference const_reference;

    /// Constructs an empty queue.
    queue()
    {
    }

    queue(queue&) = delete;
    queue& operator=(queue&) = delete;

    /// Pushes a new element to the end of the queue.
    /// @param x The value to push in the queue.
    /// @note The notification occurs @e after the mutex is unlocked, thus the
    /// waiting thread will be able to acquire the mutex without blocking.
    void push(value_type x)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        const bool empty = base::empty();
        base::push(std::move(x));

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
        bool const empty = base::empty();
        base::emplace(std::forward<Args>(args)...);

        lock.unlock();

        if (empty)
            cond_.notify_one();
    }

    /// Gets the top-most element or wait until an element is added. To avoid
    /// exception safety issues when returning data by-value, the queue uses a
    /// reference parameter to receive the result, transferring the ownership
    /// out of the queue. If the copy constructor of type @c T throws, the
    /// element is not removed from the queue.
    /// @note This function blocks when the queue is empty.
    value_type pop()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        while (base::empty())
            cond_.wait(lock);

        value_type x = base::front();
        base::pop();

        return x;
    }

    /// Tries to get the top-most queue element.
    /// @param x The reference parameter to receive the result.
    /// @return @c true if the top-most element could be returned.
    ///         @c false if the queue is empty.
    /// @note This is the @e non-blocking version of pop().
    bool try_pop(value_type& x)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (base::empty())
            return false;

        x = base::front();
        base::pop();

        return true;
    }

    /// Determines whether the queue is empty.
    /// @return @c true if the queue is empty.
    bool empty()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return base::empty();
    }

    /// Gets the size of the queue.
    /// @return The queue size.
    std::size_t size()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return base::size();
    }

private:
    std::mutex mutex_;                ///< Synchronizes queue access.
    std::condition_variable cond_;    ///< Avoids busy-waiting.
};

} // namespace util
} // namespace vast

#endif
