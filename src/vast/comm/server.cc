#include <vast/comm/server.h>

#include <vast/comm/connection.h>
#include <vast/util/logger.h>

namespace vast {
namespace comm {

server::server(boost::asio::io_service& io_service)
  : acceptor_(io_service)
{
}

void server::bind(std::string const& addr,
                  unsigned port,
                  conn_handler const& handler)
{
    accept_handler_ = handler;

    boost::asio::ip::tcp::resolver resolver(acceptor_.get_io_service());
    boost::asio::ip::tcp::resolver::query query(addr, std::to_string(port));
    boost::asio::ip::tcp::endpoint endpoint = *resolver.resolve(query);

    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen();

    LOG(info, comm) << "accepting connections on " << endpoint;
    start_accept();
}

void server::start_accept()
{
    auto new_connection =
        std::make_shared<connection>(acceptor_.get_io_service());

    acceptor_.async_accept(
        new_connection->socket(),
        [&, new_connection](boost::system::error_code const& ec)
        {
            handle_accept(new_connection, ec);
        });
}

void server::handle_accept(connection_ptr const& new_connection,
                           boost::system::error_code const& error)
{
    if (error)
    {
        LOG(error, comm) << error.message();
        return;
    }

    LOG(info, comm)
        << "accepted new connection from "
        << new_connection->socket().remote_endpoint();

    // Because it is impossible to obtain a string representation of the
    // socket once it is in a bad state, we save the necessary information
    // here for logging purposes.
    std::ostringstream tmp;
    try
    {
        tmp << new_connection->socket().local_endpoint();
        new_connection->local_ = tmp.str();

        tmp.str("");
        tmp << new_connection->socket().remote_endpoint();
        new_connection->remote_ = tmp.str();
    }
    catch (boost::system::system_error const& e)
    {
        LOG(warn, comm)
            << "could not get textual description for accepted connection";
    }

    accept_handler_(new_connection);
    start_accept();
}

} // namespace comm
} // namespace vast
