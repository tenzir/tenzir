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
  , printed_(0u)
  , asking_(true)
{
    data_.receive(
        [&](ze::event&& e)
        {
            std::lock_guard<std::mutex> lock(print_mutex_);
            if (asking_)
            {
                asking_ = false;
                std::cout << e << std::endl;
                ++printed_;
            }
            else if (printed_ % batch_size_ != 0u)
            {
                std::cout << e << std::endl;
                ++printed_;
            }
            else
            {
                print_mutex_.unlock();
                buffer_.push(std::move(e));
            }
        });

    control_.receive(
        [&](ze::event&& e)
        {
            if (e.name() == "VAST::ack")
            {
                assert(e[0].which() == ze::string_type);
                auto msg = e[0].get<ze::string>().to_string();
                if (msg == "query created")
                {
                    assert(e[1].which() == ze::string_type);
                    query_ = e[1].get<ze::string>().to_string();
                }
                else if (msg == "statistics")
                {
                    LOG(info, query) << e;
                }
            }
            else if (e.name() == "VAST::nack")
            {
                assert(e[0].which() == ze::string_type);
                auto msg = e[0].get<ze::string>().to_string();
                if (msg == "query finished")
                {
                    assert(e.size() == 2);
                    assert(e[1].which() == ze::string_type);
                    auto id = e[0].get<ze::string>().to_string();
                    assert(id == query_);

                    LOG(verbose, query) << "query finished";
                    terminating_ = true;
                    stop();
                }

            }
            else
                LOG(error, query) << "unknown VAST response: " << e;
        });
}

void client::init(std::string const& host,
                  unsigned port,
                  std::string const& expression,
                  unsigned batch_size)
{
    batch_size_ = batch_size;
    auto endpoint = host + ":" + std::to_string(port);
    LOG(info, query) << "client connecting to " << endpoint;
    control_.connect(ze::zmq::tcp, endpoint);

    std::string localhost("127.0.0.1");
    auto local_port = data_.bind(ze::zmq::tcp, localhost + ":*");
    auto local_endpoint = localhost + ":" + std::to_string(local_port);
    LOG(info, query) << "new query listening on " << local_endpoint;

    ze::event event("VAST::query",
                    "create",
                    ze::table{"expression", expression,
                              "destination", local_endpoint,
                              "batch size", std::to_string(batch_size_)});
    control_.send(event);
}

void client::stop()
{
    LOG(verbose, query) << "telling VAST to stop query " << query_;
    control_.send({"VAST::query", "remove", ze::table{"id", query_}});
};

void client::wait_for_input()
{
    util::unbuffer();

    char c;
    while (std::cin.get(c))
    {
        if (terminating_)
            break;

        switch (c)
        {
            case ' ':
                {
                    bool printed = try_print();
                    if (! printed)
                    {
                        ze::event event("VAST::query",
                                        "control",
                                        ze::table{
                                            "id", query_,
                                            "aspect", "next batch"});
                        LOG(debug, query)
                            << "asking for next batch in query " << query_;
                        control_.send(event);

                        std::lock_guard<std::mutex> lock(print_mutex_);
                        asking_ = true;
                    }
                }
                break;
            case 's':
                {
                    ze::event event("VAST::query",
                                    "statistics",
                                    ze::table{"id", query_});

                    LOG(debug, query)
                        << "asking statistics about query " << query_;

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

bool client::try_print()
{
    std::lock_guard<std::mutex> lock(print_mutex_);
    ze::event e;
    bool popped;
    do
    {
        popped = buffer_.try_pop(e);
        if (popped)
        {
            std::cout << e << std::endl;
            ++printed_;
        }
    }
    while (popped && printed_ % batch_size_ != 0u);
    return popped && printed_ % batch_size_ == 0u;
}

} // namespace query
} // namespace vast
