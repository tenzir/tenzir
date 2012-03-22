#include "vast/comm/io.h"

#include "vast/util/logger.h"
#include "vast/util/make_unique.h"

namespace vast {
namespace comm {

io::io(unsigned zmq_threads, unsigned asio_hint)
  : component_(zmq_threads, asio_hint)
  , work_(
      std::make_unique<boost::asio::io_service::work>(component_.io_service()))
{
}

void io::start(util::queue<std::exception_ptr>& errors)
{
    for (auto i = 0u; i < std::thread::hardware_concurrency(); ++i)
        threads_.emplace_back(
            [&]
            {
                LOG(debug, comm)
                    << "spawned new i/o thread thread "
                    << std::this_thread::get_id();

                try
                {
                    component_.io_service().run();
                }
                catch (...)
                {
                    std::exception_ptr e = std::current_exception();
                    errors.push(e);

                    LOG(fatal, comm)
                        << "uncaught exception thrown in thread "
                        << std::this_thread::get_id();
                }
            });
}

void io::stop()
{
    LOG(verbose, comm) << "finishing outstanding I/O operations";

    /// FIXME: Even after removing the work sentinal, io_service::run does not
    /// return. Only calling io_service::stop seems to really make the thread
    /// exit. To fix this, we need to figure our *why* io_service::run does not
    /// return.
    work_.reset();

    for (auto& thread : threads_)
    {
        LOG(debug, comm) << "joining thread " << thread.get_id();
        thread.join();
    }
}

void io::terminate()
{
    LOG(verbose, core) << "terminating I/O";

    if (work_)
        work_.reset();

    component_.io_service().stop();
}

boost::asio::io_service& io::service()
{
    return component_.io_service();
}

void io::execute_task()
{
    auto task = tasks_.pop();
    task();
}

} // namespace comm
} // namespace vast
