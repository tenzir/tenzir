#include "vast/tracker.h"
#include "vast/identifier.h"

#include <caf/all.hpp>

using namespace caf;

namespace vast {

tracker::tracker(path dir)
  : dir_{std::move(dir)}
{
  attach_functor(
      [=](uint32_t)
      {
        identifier_ = invalid_actor;
        ingestion_receivers_.clear();
        ingestion_archives_.clear();
        ingestion_indexes_.clear();
        retrieval_receivers_.clear();
        retrieval_archives_.clear();
        retrieval_indexes_.clear();
      });
}

namespace {

template <typename Map>
bool add(Map& map, std::string const& domain, actor const& a)
{
  auto i = map.find(domain);
  if (i == map.end())
  {
    map[domain].push_back(a);
    return true;
  }

  auto j = std::find(i->second.begin(), i->second.end(), a);
  if (j == i->second.end())
    return false;

  i->second.push_back(a);
  return true;
}

template <typename Map>
std::vector<actor> lookup(Map& map, std::string const& domain)
{
  auto i = map.find(domain);
  return i == map.end() ? std::vector<actor>{} : i->second;
}

} // namespace <anonymous>

message_handler tracker::act()
{
  identifier_ = spawn<identifier,linked>(dir_);
  return
  {
    on(atom("identifier")) >> [=]
    {
      return identifier_;
    },
    on(atom("receiver"), atom("ingestion"), arg_match)
      >> [=](std::string const& domain, actor const& a)
    {
      if (! add(ingestion_receivers_, domain, a))
        VAST_LOG_ACTOR_WARN("got duplicate receiver " << a);
    },
    on(atom("receiver"), atom("ingestion"), arg_match)
      >> [=](std::string const& domain)
    {
      return lookup(ingestion_receivers_, domain);
    },
    on(atom("archive"), atom("ingestion"), arg_match)
      >> [=](std::string const& domain, actor const& a)
    {
      if (! add(ingestion_archives_, domain, a))
        VAST_LOG_ACTOR_WARN("got duplicate archive " << a);
    },
    on(atom("archive"), atom("ingestion"), arg_match)
      >> [=](std::string const& domain)
    {
      return lookup(ingestion_archives_, domain);
    },
    on(atom("index"), atom("ingestion"), arg_match)
      >> [=](std::string const& domain, actor const& a)
    {
      if (! add(ingestion_indexes_, domain, a))
        VAST_LOG_ACTOR_WARN("got duplicate index " << a);
    },
    on(atom("index"), atom("ingestion"), arg_match)
      >> [=](std::string const& domain)
    {
      return lookup(ingestion_indexes_, domain);
    },
    on(atom("receiver"), atom("retrieval"), arg_match)
      >> [=](std::string const& domain, actor const& a)
    {
      if (! add(retrieval_receivers_, domain, a))
        VAST_LOG_ACTOR_WARN("got duplicate receiver " << a);
    },
    on(atom("receiver"), atom("retrieval"), arg_match)
      >> [=](std::string const& domain)
    {
      return lookup(retrieval_receivers_, domain);
    },
    on(atom("archive"), atom("retrieval"), arg_match)
      >> [=](std::string const& domain, actor const& a)
    {
      if (! add(retrieval_archives_, domain, a))
        VAST_LOG_ACTOR_WARN("got duplicate archive " << a);
    },
    on(atom("archive"), atom("retrieval"), arg_match)
      >> [=](std::string const& domain)
    {
      return lookup(retrieval_archives_, domain);
    },
    on(atom("index"), atom("retrieval"), arg_match)
      >> [=](std::string const& domain, actor const& a)
    {
      if (! add(retrieval_indexes_, domain, a))
        VAST_LOG_ACTOR_WARN("got duplicate index " << a);
    },
    on(atom("index"), atom("retrieval"), arg_match)
      >> [=](std::string const& domain)
    {
      return lookup(retrieval_indexes_, domain);
    }
  };
}

std::string tracker::describe() const
{
  return "tracker";
}

} // namespace vast
