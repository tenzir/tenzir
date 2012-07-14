#include <vast/util/active_io_service.h>

#include <ze/util/make_unique.h>
#include <vast/util/logger.h>

namespace vast {
namespace util {

active_io_service::active_io_service(unsigned concurrency_hint)
  : boost::asio::io_service(concurrency_hint)
{
}

void active_io_service::start(unsigned threads)
{
  for (unsigned i = 0; i < threads; ++i)
    threads_.emplace_back([&] { run(); });
}

void active_io_service::stop(bool cancel)
{
  boost::asio::io_service::stop();
  for (auto& thread : threads_)
    thread.join();
}

} // namespace util
} // namespace vast
