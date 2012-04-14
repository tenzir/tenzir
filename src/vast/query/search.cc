#include "vast/query/search.h"

#include <ze/event.h>
#include <ze/util/make_unique.h>
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
    source_.to(manager_);
    manager_.receive([&](ze::event_ptr&& e) { submit(std::move(e)); });
}

void search::init(std::string const& host, unsigned port)
{
    source_.init(host, port);
    source_.subscribe("VAST::query");
}

void search::stop()
{
    source_.stop();
};

void search::submit(ze::event_ptr query_event)
{
    LOG(debug, query) << "new search event: " << *query_event;
    auto& event = *query_event;
    validate(event);

    auto action = event[0].get<ze::string>().to_string();
    auto& options = event[1].get<ze::table>();
    auto expr = options["expression"].get<ze::string>().to_string();
    auto dst = options["destination"].get<ze::string>().to_string();

    if (action == "create")
    {
        auto q = std::make_unique<query>(*this, expr);
        auto& emitter = archive_.create_emitter();
        emitter.to(q->frontend());
        LOG(verbose, query) << "query connecting to " << dst;
        q->backend().connect(ze::zmq::tcp, dst);
        {
            std::lock_guard<std::mutex> lock(query_mutex_);
            auto id = q->id();
            queries_.emplace(id, std::move(q));
            query_to_emitter_.emplace(id, emitter.id());
        }

        emitter.start();
    }
    else if (action == "control")
    {
        // TODO: not yet implemented
    }
}

void search::validate(ze::event const& event)
{
    if (event.name() != "VAST::query")
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
