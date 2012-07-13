#ifndef VAST_UTIL_ACTIVE_IO_SERVICE_H
#define VAST_UTIL_ACTIVE_IO_SERVICE_H

#include <thread>
#include <boost/asio/io_service.hpp>

namespace vast {
namespace util {

/// Wraps a Boost Asio I/O service object as an *active object*.
class active_io_service : public boost::asio::io_service
{
public:
  /// Creates an active I/O service object.
  /// @param concurrency_hint A hint to the io_service implementation of how
  /// many threads to use.
  active_io_service(unsigned concurrency_hint = 1);

  /// Invokes the `run` method of the I/O service object in one or more
  /// separate threads. The function returns immediately and does not block.
  ///
  /// @param threads The number of threads that should invoke run.
  void start(unsigned threads = 1);

  /// Stops the active object by joining the created threads.
  ///
  /// @param cancel If `true`, cancel all outstanding asynchronous operations.
  void stop(bool cancel = false);

private:
  std::unique_ptr<boost::asio::io_service::work> work_;
  std::vector<std::thread> threads_;
};

} // namespace util
} // namespace vast

#endif
