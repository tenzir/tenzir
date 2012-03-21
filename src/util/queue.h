#ifndef VAST_UTIL_QUEUE_H
#define VAST_UTIL_QUEUE_H

#include <condition_variable>
#include <mutex>
#include <queue>

namespace vast {
namespace util {

/// A thread-safe std::queue.
template <typename T>
class queue
{
public:
    typedef typename std::queue<T>::container_type container_type;
    typedef typename std::queue<T>::value_type value_type;
    typedef typename std::queue<T>::reference reference;
    typedef typename std::queue<T>::const_reference const_reference;;
        
    /// Default Constructor.
    queue()
    {
    }

    queue(queue&) = delete;
    queue& operator=(queue&) = delete;

    /// Pushes a new element to the end of the queue.
    /// \param x The value to push in the queue.
    /// \note The notification occurs \e after the mutex is unlocked, thus the
    /// waiting thread will be able to acquire the mutex without blocking.
    void push(T const& x)
    { 
        std::unique_lock<std::mutex> lock(mutex_);
        const bool empty = queue_.empty();
        queue_.push(x); 

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
        bool const empty = queue_.empty();
        queue_.emplace(std::forward<Args>(args)...); 

        lock.unlock();

        if (empty)
            cond_.notify_one();
    }

    /// Get the top-most element or wait until an element is added. To avoid
    /// exception safety issues when returning data by-value, the queue uses a
    /// reference parameter to receive the result, transferring the ownership
    /// out of the queue. If the copy constructor of type \c T throws, the
    /// element is not removed from the queue.
    /// \param x The reference parameter to receive the result. 
    /// \note This function blocks when the queue is empty.
    T pop()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        while (queue_.empty())
            cond_.wait(lock);
    
        T x = queue_.front();
        queue_.pop();

        return std::move(x);
    }

    /// Try to get the top-most queue element. 
    /// \return \c true if the top-most element could be returned.
    ///         \c false if the queue is empty.
    /// \param x The reference parameter to receive the result. 
    /// \note This is the \e non-blocking version of pop().
    bool try_pop(T& x)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty())
            return false;

        x = queue_.front();
        queue_.pop();

        return true;
    }
    
    /// Determine whether the queue is empty.
    /// \return \c true if the queue is empty.
    bool empty()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }
    
    /// Get the size of the queue.
    /// \return The queue size.
    std::size_t size()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

private:
    std::queue<T> queue_;             ///< The underlying STL queue.
    std::mutex mutex_;                ///< Synchronizes queue access.
    std::condition_variable cond_;    ///< Avoids busy-waiting.
};

} // namespace util
} // namespace vast

#endif
