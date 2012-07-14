#ifndef VAST_COMM_SERVER_H
#define VAST_COMM_SERVER_H

#include <thread>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

namespace vast {
namespace comm {

// Forward declarations.
class connection;

/// An asynchronous TCP server.
class server : boost::noncopyable
{
public:
  typedef std::shared_ptr<connection> connection_ptr;
  typedef std::function<void(connection_ptr const&)> conn_handler;

  /// Binds to an endpoint identified by address/hostname and port and start
  /// accepting connections.
  /// @param callback The accept handler.
  void start(std::string const& addr, unsigned port, conn_handler handler);

  void stop();

private:
  /// Start the asynchronuous accept operation.
  void start_accept();

  /// Executes the registered accept handler for a freshly accepted connection.
  /// @param new_connection The accepted connection.
  /// @param error The error code of the accept operation.
  void handle_accept(connection_ptr new_connection,
                     boost::system::error_code const& error);

  boost::asio::io_service io_service_;
  boost::asio::ip::tcp::acceptor acceptor_{io_service_};
  conn_handler accept_handler_;
  std::thread thread_;
};

} // namespace comm
} // namespace vast

#endif

