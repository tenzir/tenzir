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

//void io::add_thread()
//{
//    threads_.emplace_back([&] { run(); });
//}
//
//void io::remove_thread()
//{
//}

void io::start()
{
    component_.io_service().run();
}

void io::stop()
{
    LOG(verbose, comm) << "finishing outstanding I/O operations";
    work_.reset();
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

void io::run_worker()
{
    try
    {
        while (true)
        {
            auto task = tasks_.pop();
            task();
        }
    }
    catch (...)
    {
        std::exception_ptr e = std::current_exception();
        // FIXME
        //errors.push(e);

        //LOG(fatal, comm)
        //    << "unexpected exception thrown in thread "
        //    << std::this_thread::get_id()
        //    << " (" << name_ << ')';
    }
}

} // namespace comm
} // namespace vast
