#ifndef VAST_ACTOR_KEY_VALUE_STORE_H
#define VAST_ACTOR_KEY_VALUE_STORE_H

#include <set>
#include <string>

#include <caf/scoped_actor.hpp>

#include "vast/error.h"
#include "vast/actor/actor.h"
#include "vast/util/radix_tree.h"

namespace vast {

/// A replicated hierarchical key-value store.
class key_value_store : public default_actor
{
public:
  /// Synchronous interface.
  class wrapper
  {
  public:
    // Constructs a key-value store.
    wrapper(caf::actor& store);

    /// Records a key-value pair.
    /// @param key The key to store *value* under.
    /// @param xs The values to associate with *key*.
    /// @returns `true` on success.
    template <typename... Ts>
    bool put(std::string const& key, Ts&&... xs) const
    {
      auto result = true;
      caf::scoped_actor self;
      auto msg = make_message(put_atom::value, key, std::forward<Ts>(xs)...);
      self->sync_send(store_, std::move(msg)).await(
        [&](error const&) { result = false; },
        [](ok_atom) { /* nop */ }
      );
      return result;
    }

    /// Deletes a key.
    /// @param key The key to delete.
    /// @returns The number of elements deleted. (0 or 1)
    size_t erase(std::string const& key) const;

    /// Deletes all key-value pairs having a specific value.
    /// @param value The value of the key-value pairs to delete.
    /// @returns The number of elements deleted.
    //size_t erase(caf::message const& value) const;

    /// Checks whether a key exists.
    /// @param key The key to check.
    /// @returns `true` if *key* exists in the store.
    bool exists(std::string const& key) const;

    /// Retrieves a value for a given key.
    /// @param *key* The key to lookup.
    /// @returns The value associated with *key* or an empty optional otherwise.
    caf::message get(std::string const& key) const;

    /// Enumerates all values for a given key prefix.
    /// @param prefix The prefix to enumerate.
    /// @returns All key-value pairs prefixed by *prefix*.
    std::map<std::string, caf::message> list(std::string const& prefix) const;

    /// Adds a peer to the store.
    /// @param peer The actor to add as peer.
    void add_peer(caf::actor const& peer) const;

  private:
    caf::actor& store_;
  };

  /// Spawns a key-value store.
  /// @param seperator The namespace separator.
  key_value_store(std::string const& seperator = "/");

private:
  void on_exit() override;
  caf::behavior make_behavior() override;

  std::string const seperator_;
  util::radix_tree<caf::message> data_;
  std::set<caf::actor> peers_;
};


} // namespace vast

#endif
