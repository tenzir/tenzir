#include <vast/comm/detail/active_io_service.h>
#include <ze/util/make_unique.h>

namespace vast {
namespace comm {
namespace detail {

active_io_service::active_io_service(unsigned concurrency_hint)
  : boost::asio::io_service(concurrency_hint)
  , work_(std::make_unique<boost::asio::io_service::work>(*this))
{
}

void active_io_service::start(unsigned threads)
{
  for (unsigned i = 0; i < threads; ++i)
    threads_.emplace_back([&] { run(); });
}

void active_io_service::stop(bool cancel)
{
  work_.reset();

  if (cancel)
    boost::asio::io_service::stop();

  for (auto& thread : threads_)
    thread.join();
}

} // namespace detail
} // namespace comm
} // namespace vast
