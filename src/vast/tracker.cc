#include "vast/tracker.h"
#include "vast/identifier.h"
#include "vast/replicator.h"

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
      });
}

message_handler tracker::make_handler()
{
  identifier_ = spawn<identifier, linked>(dir_);

  return
  {
    [=](down_msg const&)
    {
      // When an actor goes down, TRACKER sets the actor to invalid but
      // keeps the topology information, as the terminated actor may come up
      // again.
      for (auto& p : actors_)
        if (p.second.actor == last_sender())
          p.second.actor = invalid_actor;
    },
    on(atom("identifier")) >> [=]
    {
      return identifier_;
    },
    on(atom("put"), arg_match)
      >> [=](std::string const& type, actor const& a, std::string const& name)
    {
      if (actors_.find(name) != actors_.end())
        return make_message(error{"duplicate actor: ", name});

      auto& as = actors_[name];
      as.actor = a;
      if (type == "importer")
        as.type = component::importer;
      else if (type == "receiver")
        as.type = component::receiver;
      else if (type == "archive")
        as.type = component::archive;
      else if (type == "index")
        as.type = component::index;
      else if (type == "search")
        as.type = component::search;
      else
        return make_message(error{"duplicate actor: ", name});

      monitor(a);
      VAST_LOG_VERBOSE(*this, " registers " << type << ": " << name);
      return make_message(atom("success"));
    },
    on(atom("get"), arg_match) >> [=](std::string const& name)
    {
      auto i = actors_.find(name);
      if (i != actors_.end())
        return make_message(i->second.actor);
      else
        return make_message(error{"unknown actor: ", name});
    },
    on(atom("link"), arg_match)
      >> [=](std::string const& source, std::string const& sink)
    {
      auto i = actors_.find(source);
      actor_state* src = nullptr;
      if (i != actors_.end())
        src = &i->second;
      else
        return make_message(error{"unknown source: ", source});

      i = actors_.find(sink);
      actor_state* snk = nullptr;
      if (i != actors_.end())
        snk = &i->second;
      else
        return make_message(error{"unknown sink: ", sink});

      auto er = topology_.equal_range(source);
      for (auto i = er.first; i != er.second; ++i)
        if (i->first == source && i->second == sink)
          return make_message(error{"link exists: ", source, " -> ", sink});

      switch (src->type)
      {
        default:
          return make_message(error{"invalid source: ", source});
        case component::importer:
          if (snk->type != component::receiver)
            return make_message(error{"sink not a receiver: ", sink});
          else
            send(src->actor, atom("sink"), snk->actor);
          break;
        case component::receiver:
        case component::search:
          if (snk->type == component::archive)
            send(src->actor, atom("link"), atom("archive"), snk->actor);
          else if (snk->type == component::index)
            send(src->actor, atom("link"), atom("index"), snk->actor);
          else
            return make_message(error{"sink not archive or index: ", sink});
          break;
      }

      VAST_LOG_VERBOSE(*this, " links " << source << " -> " << sink);
      topology_.emplace(source, sink);
      return make_message(atom("success"));
    }
  };
}

std::string tracker::name() const
{
  return "tracker";
}

} // namespace vast
