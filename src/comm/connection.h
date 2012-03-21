#ifndef VAST_COMM_CONNECTION_H
#define VAST_COMM_CONNECTION_H

#include <iosfwd>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/noncopyable.hpp>

namespace vast {
namespace comm {

/// A TCP connection.
class connection : boost::noncopyable
{
    /// Sets the connection description upon accept.
    friend class server;

public:
    /// Constructor.
    /// \param io_service The io service object for that connection.
    explicit connection(boost::asio::io_service& io_service);

    /// Get a reference to the underlying socket of the connection.
    /// \return A reference to the underlying socket.
    boost::asio::ip::tcp::socket& socket();

    /// Get a const reference to the underlying socket of the connection.
    /// \return A const reference to the underlying socket.
    boost::asio::ip::tcp::socket const& socket() const;

    /// Get a string representation of the local endpoint.
    /// \return A string of the form \c address:port
    std::string const& local() const;

    /// Get a string representation of the remote endpoint.
    /// \return A string of the form \c address:port
    std::string const& remote() const;

private:
    /// The underlying socket.
    boost::asio::ip::tcp::socket socket_;

    /// Description of the local endpoint.
    std::string local_;

    /// Description of the remote endpoint.
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
