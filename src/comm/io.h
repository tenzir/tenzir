#ifndef VAST_COMM_IO_H
#define VAST_COMM_IO_H

#include <future>
#include <mutex>
#include <vector>
#include <ze/component.h>
#include "util/queue.h"

namespace vast {
namespace comm {

/// The I/O abstraction for communication and task scheduling.
class io
{
public:
    /// Constructs an I/O object.
    io(unsigned zmq_threads = 1u, unsigned asio_hint = 0u);

    /// Queues an arbitrary function for asynchronous execution by the
    /// I/O service object.
    /// @tparam F The function type.
    /// @param f An instance of @c F.
    /// @return A future for the execution of @a f.
    template <typename F>
    std::future<typename std::result_of<F()>::type>
    queue(F f)
    {
        typedef typename std::result_of<F()>::type result_type;
        std::packaged_task<result_type> task(f);
        std::future<result_type> future = task.get_future();

        tasks_.push(std::move(task));
        return std::move(future);
    }

    /// Starts the I/O loop and block.
    void start();

    /// Waits until all handler finish execution normally and then terminate.
    void stop();

    /// Immediately cancels all handler executions.
    void terminate();

    /// Retrieves the I/O service object.
    /// @return A reference to the I/O service object.
    boost::asio::io_service& service();

private:
    void run_worker();

    ze::component component_;
    std::unique_ptr<boost::asio::io_service::work> work_;
    std::vector<std::thread> threads_;
    util::queue<std::function<void()>> tasks_;
};

} // namespace comm
} // namespace vast

#endif
