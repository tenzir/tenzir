#include "vast/actor/tracker.h"

#include <caf/all.hpp>
#include "vast/actor/identifier.h"

using namespace caf;

namespace vast {

tracker::tracker(path dir)
  : default_actor{"tracker"},
    dir_{std::move(dir)}
{
  trap_exit(true);
}

void tracker::on_exit()
{
  identifier_ = invalid_actor;
  actors_.clear();
}

behavior tracker::make_behavior()
{
  identifier_ = spawn<identifier, linked>(dir_);
  return
  {
    [=](exit_msg const& msg)
    {
      for (auto& p : actors_)
        send_exit(p.second.actor, msg.reason);
      quit(msg.reason);
    },
    [=](down_msg const& msg)
    {
      for (auto i = actors_.begin(); i != actors_.end(); ++i)
        if (i->second.actor == msg.source)
        {
          i->second.actor = invalid_actor;
          break;
        }
    },
    [](ok_atom)
    {
      // Sent during relinking below, nothing to do here.
    },
    [=](put_atom, std::string const& type, actor const& a,
        std::string const& name)
    {
      if (name == "identifier")
        return make_message(error{"'identifier' is a reserved name"});
      auto c = component::invalid;
      if (type == "source")
        c = component::source;
      else if (type == "exporter")
        c = component::exporter;
      else if (type == "receiver")
        c = component::receiver;
      else if (type == "archive")
        c = component::archive;
      else if (type == "index")
        c = component::index;
      else if (type == "search")
        c = component::search;
      else
        return make_message(error{"invalid type: ", type});
      auto i = actors_.find(name);
      if (i == actors_.end())
      {
        VAST_INFO(this, "registers", type << ':', name);
        actors_.emplace(name, actor_state{a, c});
      }
      else
      {
        if (i->second.type != c)
        {
          VAST_WARN(this, "found existing actor with different type:", name);
          return make_message(error{"type mismatch for: ", name});
        }
        if (i->second.actor != invalid_actor)
        {
          VAST_WARN(this, "got duplicate actor:", name);
          return make_message(error{"duplicate actor: ", name});
        }
        VAST_INFO(this, "re-instantiates", name);
        i->second.actor = a;
        // Relink affected components.
        auto j = topology_.begin();
        while (j != topology_.end())
          if (j->first == name || j->second == name)
          {
            send(this, link_atom::value, j->first, j->second);
            j = topology_.erase(j);
          }
          else
          {
            ++j;
          }
      }
      monitor(a);
      return make_message(ok_atom::value);
    },
    [=](get_atom, std::string const& name)
    {
      if (name == "identifier")
        return make_message(identifier_);
      auto i = actors_.find(name);
      if (i != actors_.end())
        return make_message(i->second.actor);
      else
        return make_message(error{"unknown actor: ", name});
    },
    [=](link_atom, std::string const& source, std::string const& sink)
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
        if (i->second == sink)
        {
          VAST_INFO(this, "ignores existing link: ", source, " -> ", sink);
          return make_message(ok_atom::value);
        }
      VAST_INFO(this, "links", source, "->", sink);
      scoped_actor self;
      switch (src->type)
      {
        default:
          return make_message(error{"invalid source: ", source});
        case component::source:
          if (snk->type != component::receiver)
            return make_message(error{"sink not a receiver: ", sink});
          else
            self->sync_send(src->actor, add_atom::value, sink_atom::value,
                            snk->actor).await([](ok_atom) {});
          break;
        case component::receiver:
        case component::search:
          if (snk->type == component::archive)
            self->sync_send(src->actor, add_atom::value, archive_atom::value,
                            snk->actor).await([](ok_atom) {});
          else if (snk->type == component::index)
            self->sync_send(src->actor, add_atom::value, index_atom::value,
                            snk->actor).await([](ok_atom) {});
          else
            return make_message(error{"sink not archive or index: ", sink});
          break;
      }
      topology_.emplace(source, sink);
      return make_message(ok_atom::value);
    },
    catch_unexpected()
  };
}

} // namespace vast
