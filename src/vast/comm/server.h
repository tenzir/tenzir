#ifndef VAST_COMM_SERVER_H
#define VAST_COMM_SERVER_H

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/noncopyable.hpp>

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

    /// Constructor.
    /// \param io_service The Asio I/O service instance.
    server(boost::asio::io_service& io_service);

    /// Bind to an endpoint identified by address/hostname and port and start
    /// accepting connections.
    /// \param callback The accept handler.
    void bind(std::string const& addr,
              unsigned port,
              conn_handler const& handler);

private:
    /// Start the asynchronuous accept operation.
    void start_accept();

    /// Execute the registered accept handler for a freshly accepted
    /// connection.
    /// \param new_connection The accepted connection.
    /// \param error The error code of the accept operation.
    void handle_accept(connection_ptr const& new_connection,
                       boost::system::error_code const& error);

    boost::asio::ip::tcp::acceptor acceptor_;
    conn_handler accept_handler_;
};

} // namespace comm
} // namespace vast

#endif

