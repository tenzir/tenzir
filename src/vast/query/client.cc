#include "vast/query/client.h"

#include <ze/event.h>
#include "vast/util/console.h"
#include "vast/util/logger.h"
#include "vast/query/exception.h"

namespace vast {
namespace query {

client::client(ze::io& io)
  : ze::component(io)
  , control_(*this)
  , data_(*this)
{
    data_.receive(
        [](ze::event&& e)
        {
            std::cout << e << std::endl;
        });

    control_.receive(
        [&](ze::event&& e)
        {
            if (e.name() == "VAST::ack")
            {
                assert(e.size() == 2);
                assert(e[0].which() == ze::string_type);
                auto msg = e[0].get<ze::string>().to_string();
                if (msg == "query created")
                {
                    assert(e[1].which() == ze::string_type);
                    auto id = e[1].get<ze::string>().to_string();
                    auto i = std::find(queries_.begin(), queries_.end(), id);
                    assert(i == queries_.end());
                    queries_.push_back(id);
                }
            }
            else if (e.name() == "VAST::nack")
            {
                LOG(info, query) << e;
            }
            else
            {
                LOG(error, query) << "unknown VAST response: " << e;
            }
        });
}

void client::init(std::string const& host, unsigned port)
{
    auto endpoint = host + ":" + std::to_string(port);
    LOG(info, query) << "client connecting to " << endpoint;
    control_.connect(ze::zmq::tcp, endpoint);
}

void client::stop()
{
    for (auto& query : queries_)
    {
        LOG(verbose, query) << "telling VAST to stop query " << query;
        control_.send({"VAST::query", "remove", ze::table{"id", query}});
    }

    queries_.clear();
};

void client::submit(std::string const& expression, unsigned batch_size)
{
    // TODO: Make parameters configurable.

    std::string host("127.0.0.1");
    auto port = data_.bind(ze::zmq::tcp, host + ":*");
    auto endpoint = host + ":" + std::to_string(port);
    LOG(info, query) << "new query listening on " << endpoint;

    ze::event event("VAST::query",
                    "create",
                    ze::table{"expression", expression,
                              "destination", endpoint,
                              "batch size", std::to_string(batch_size)});
    control_.send(event);
}

void client::wait_for_input()
{
    util::unbuffer();

    char c;
    while (std::cin.get(c))
    {
        switch (c)
        {
            case ' ':
                {
                    // Currently only one query per client supported.
                    assert(queries_.size() == 1);

                    ze::event event("VAST::query",
                                    "control",
                                    ze::table{
                                        "id", queries_[0],
                                        "aspect", "next batch"});

                    LOG(debug, query) << "asking for next batch";
                    control_.send(event);
                }
                break;
            case 'q':
                stop();
                return;
            default:
                continue;
        }
    }

    util::buffer();
};

} // namespace query
} // namespace vast
