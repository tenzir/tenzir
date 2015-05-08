#include <caf/all.hpp>

#include "vast/error.h"
#include "vast/logger.h"
#include "vast/none.h"
#include "vast/actor/key_value_store.h"

using namespace caf;

namespace vast {

key_value_store::key_value_store(std::string const& seperator)
  : default_actor{"key-value-store"},
    seperator_{seperator}
{
}

void key_value_store::on_exit()
{
  peers_.clear();
}

behavior key_value_store::make_behavior()
{
  auto relay = [=]
  {
    for (auto& p : peers_)
      if (p->address() != current_sender())
      {
        VAST_DEBUG(this, "relays message to peer", p);
        send(p, current_message());
      }
  };
  return
  {
    [=](down_msg const& msg)
    {
      VAST_DEBUG(this, "got DOWN from", msg.source);
      peers_.erase(actor_cast<actor>(msg.source));
    },
    [=](peer_atom, caf::actor const& peer)
    {
      if (! peers_.insert(peer).second)
        return ok_atom::value;
      monitor(peer);
      send(peer, peer_atom::value, this); // Peerings are bidirectional.
      // TODO: relay state in a more efficient manner, e.g., by serializing the
      // underlying radix tree.
      for (auto& d : data_)
      {
        VAST_DEBUG(this, "relays data to peer:", d.first);
        send(peer, put_atom::value, d.first, d.second);
      }
      return ok_atom::value;
    },
    on(put_atom::value, val<std::string>, any_vals)
      >> [=](std::string const& key)
    {
      VAST_DEBUG(this, "got PUT:", key, '=',
                 to_string(current_message().drop(2)));
      if (key.empty())
        return make_message(error{"empty key"});
      else if (key == seperator_)
        return make_message(error{"invalid key: ", seperator_});
      relay();
      data_[key] = current_message().drop(2);
      return make_message(ok_atom::value);
    },
    [=](exists_atom, std::string const& key)
    {
      VAST_DEBUG(this, "got EXISTS:", key);
      return ! data_.prefixed_by(key).empty();
    },
    [=](get_atom, std::string const& key)
    {
      VAST_DEBUG(this, "got GET for key:", key);
      auto v = data_.find(key);
      if (v == data_.end())
        return make_message(nil);
      return v->second.empty() ? make_message(unit) : v->second;
    },
    [=](list_atom, std::string const& key)
    {
      VAST_DEBUG(this, "got LIST:", key);
      if (key.empty())
        return make_message(error{"empty key"});
      auto values = data_.prefixed_by(key);
      std::map<std::string, message> result;
      for (auto& v : values)
        result.emplace(v->first, v->second);
      return make_message(std::move(result));
    },
    [=](delete_atom, std::string const& key)
    {
      VAST_DEBUG(this, "got DELETE:", key);
      relay();
      auto values = data_.prefixed_by(key);
      std::vector<std::string> result(values.size());
      for (auto i = 0u; i < values.size(); ++i)
        result[i] = values[i]->first;
      auto total = uint64_t{0};
      for (auto v : result)
        total += data_.erase(v);
      return total;
    },
    on(delete_atom::value, val<std::string>, any_vals)
      >> [=](std::string const& key)
    {
      auto value = current_message().drop(2);
      VAST_DEBUG(this, "got DELETE:", key, '=', to_string(value));
      relay();
      auto values = data_.prefixed_by(key);
      std::vector<std::string> victims;
      victims.reserve(values.size());
      for (auto& v : values)
        if (v->second == value)
          victims.push_back(v->first);
      auto total = uint64_t{0};
      for (auto v : victims)
        total += data_.erase(v);
      return total;
    }
  };
}

} // namespace vast
