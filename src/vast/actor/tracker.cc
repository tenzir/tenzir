#include "vast/actor/tracker.h"
#include "vast/actor/identifier.h"
#include "vast/actor/replicator.h"

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

void tracker::at_down(caf::down_msg const&)
{
  // When an actor goes down, TRACKER sets the actor to invalid but keeps the
  // topology information, as the terminated actor may come up again.
  for (auto i = actors_.begin(); i != actors_.end(); ++i)
    if (i->second.actor == last_sender())
    {
      VAST_VERBOSE(this, "disconnects", i->first);
      auto j = topology_.begin();
      while (j != topology_.end())
        if (j->first == i->first || j->second == i->first)
        {
          VAST_VERBOSE(this, "removes link", j->first, "->", j->second);
          j = topology_.erase(j);
        }
        else
        {
          ++j;
        }

      actors_.erase(i);
      break;
    }
}

message_handler tracker::make_handler()
{
  identifier_ = spawn<identifier, linked>(dir_);

  return
  {
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
      else if (type == "exporter")
        as.type = component::exporter;
      else if (type == "receiver")
        as.type = component::receiver;
      else if (type == "archive")
        as.type = component::archive;
      else if (type == "index")
        as.type = component::index;
      else if (type == "search")
        as.type = component::search;
      else
        return make_message(error{"invalid type: ", type});

      monitor(a);
      VAST_VERBOSE(this, "registers", type << ':', name);
      return make_message(atom("ok"));
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

      VAST_VERBOSE(this, "links", source, "->", sink);

      scoped_actor self;
      message_handler ok = on(atom("ok")) >> [] { /* do nothing */ };
      switch (src->type)
      {
        default:
          return make_message(error{"invalid source: ", source});
        case component::importer:
          if (snk->type != component::receiver)
            return make_message(error{"sink not a receiver: ", sink});
          else
            self->sync_send(src->actor, atom("add"), atom("sink"),
                            snk->actor).await(ok);
          break;
        case component::receiver:
        case component::search:
          if (snk->type == component::archive)
            self->sync_send(src->actor, atom("add"), atom("archive"),
                            snk->actor).await(ok);
          else if (snk->type == component::index)
            self->sync_send(src->actor, atom("add"), atom("index"),
                            snk->actor).await(ok);
          else
            return make_message(error{"sink not archive or index: ", sink});
          break;
      }

      topology_.emplace(source, sink);
      return make_message(atom("ok"));
    }
  };
}

std::string tracker::name() const
{
  return "tracker";
}

} // namespace vast
