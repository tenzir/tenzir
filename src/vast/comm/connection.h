#ifndef VAST_COMM_CONNECTION_H
#define VAST_COMM_CONNECTION_H

#include <iosfwd>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

namespace vast {
namespace comm {

/// A TCP connection.
class connection
{
    /// Sets the connection description upon accept.
    friend class server;

    connection(connection const&) = delete;
    connection& operator=(connection const&) = delete;

public:
    /// Constructor.
    /// @param io_service The io service object for that connection.
    explicit connection(boost::asio::io_service& io_service);

    /// Gets a reference to the underlying socket of the connection.
    /// @return A reference to the underlying socket.
    boost::asio::ip::tcp::socket& socket();

    /// Gets a const reference to the underlying socket of the connection.
    /// @return A const reference to the underlying socket.
    boost::asio::ip::tcp::socket const& socket() const;

    /// Gets a string representation of the local endpoint.
    /// @return A string of the form \c address:port
    std::string const& local() const;

    /// Gets a string representation of the remote endpoint.
    /// @return A string of the form \c address:port
    std::string const& remote() const;

private:
    boost::asio::ip::tcp::socket socket_;
    std::string local_;
    std::string remote_;
};

template <typename Elem, typename Traits>
std::basic_ostream<Elem, Traits>& operator<<(
        std::basic_ostream<Elem, Traits>& os, connection const& conn)
{
    os << conn.local() << " <-> " << conn.remote();
    return os;
}

} // namespace comm
} // namespace vast

#endif
