#include "comm/connection.h"

namespace vast {
namespace comm {

connection::connection(boost::asio::io_service& io_service)
  : socket_(io_service)
  , local_("local endpoint")
  , remote_("remote endpoint")
{
}

boost::asio::ip::tcp::socket& connection::socket()
{
    return socket_;
}

boost::asio::ip::tcp::socket const& connection::socket() const
{
    return socket_;
}

std::string const& connection::local() const
{
    return local_;
}

std::string const& connection::remote() const
{
    return remote_;
}

} // namespace comm
} // namespace vast
