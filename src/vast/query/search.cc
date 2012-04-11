#include "vast/query/search.h"

#include <ze/event.h>
#include <ze/link.h>
#include "vast/util/logger.h"
#include "vast/query/exception.h"
#include "vast/store/archive.h"
#include "vast/store/emitter.h"

namespace vast {
namespace query {

search::search(ze::io& io, store::archive& archive)
  : ze::component(io)
  , archive_(archive)
  , source_(*this)
  , manager_(*this)
{
    ze::link(source_, manager_);

    manager_.receive(
        [&](ze::event_ptr&& e)
        {
            auto& event = *e;
            validate(event);

            auto action = event[0].get<ze::string>().to_string();
            auto& options = event[1].get<ze::table>();
            auto expr = options["expression"].get<ze::string>().to_string();

            if (action == "create")
            {
                query q(*this, expr);
                auto& emitter = archive_.create_emitter();
                ze::link(emitter, q.frontend());
                emitter.start();
                //q.backend().connect("tcp://" + {dst.begin(), dst.end()});
                //{
                //    std::lock_guard<std::mutex> lock(query_mutex_);
                //    queries_.push_back(std::move(q));
                //}
            }
            else if (action == "control")
            {
                assert(! "not yet implemented");
            }
        });
}

void search::init(std::string const& host, unsigned port)
{
    source_.init(host, port);
}

void search::stop()
{
    // TODO: stop all emitters and queries.
};

void search::validate(ze::event const& event)
{
    if (event.name() != "vast::query")
        throw exception("invalid query event name");

    if (event.size() != 2)
        throw exception("invalid number of query event arguments");

    if (! event[0].which() == ze::string_type)
        throw exception("invalid first argument type of query event");

    if (! event[1].which() == ze::table_type)
        throw exception("invalid second argument type of query event");

    auto action = event[0].get<ze::string>().to_string();
    auto& options = event[1].get<ze::table>();

    if (action == "create")
    {
        if (options.size() < 2)
            throw exception("action 'create' requires two parameters");

        auto expr = options.find("expression");
        if (expr == options.end())
            throw exception("option 'expression' required");
        else if (expr->second.which() != ze::string_type)
            throw exception("option 'expression' has invalid type");

        auto dst = options.find("destination");
        if (dst == options.end())
            throw exception("option 'destination' required");
        else if (dst->second.which() != ze::string_type)
            throw exception("option 'destination' has invalid type");
    }
    else if (action == "control")
    {
        // TODO: not yet implemented.
    }
}

} // namespace query
} // namespace vast
