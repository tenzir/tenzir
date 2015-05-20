#ifndef VAST_ACTOR_ACTOR_H
#define VAST_ACTOR_ACTOR_H

#include <cassert>
#include <ostream>
#include <caf/event_based_actor.hpp>
#include <caf/send.hpp>
#include <caf/to_string.hpp>

#include "vast/util/assert.h"
#include "vast/actor/atoms.h"

namespace caf {

inline std::ostream& operator<<(std::ostream& out, actor_addr const& a)
{
  out << '#' << a.id();
  return out;
}

inline std::ostream& operator<<(std::ostream& out, actor const& a)
{
  out << a.address();
  return out;
}

inline std::ostream& operator<<(std::ostream& out, abstract_actor const& a)
{
  out << a.address();
  return out;
}

template <typename Stream>
inline Stream& operator<<(Stream& out, actor const* a)
{
  VAST_ASSERT(a != nullptr);
  out << *a;
  return out;
}

template <typename Stream>
inline Stream& operator<<(Stream& out, abstract_actor const* a)
{
  VAST_ASSERT(a != nullptr);
  out << *a;
  return out;
}

} // namespace caf

#include "vast/logger.h"
#include "vast/util/operators.h"
#include "vast/util/flat_set.h"

namespace vast {
namespace exit {

constexpr uint32_t done   = caf::exit_reason::user_defined;
constexpr uint32_t stop   = caf::exit_reason::user_defined + 1;
constexpr uint32_t error  = caf::exit_reason::user_defined + 2;
constexpr uint32_t kill   = caf::exit_reason::user_defined + 3;

} // namespace exit

inline char const* render_exit_reason(uint32_t reason)
{
  switch (reason)
  {
    default:
      return "unknown";
    case exit::done:
      return "done";
    case exit::stop:
      return "stop";
    case exit::error:
      return "error";
    case exit::kill:
      return "kill";
    case caf::exit_reason::normal:
      return "normal";
    case caf::exit_reason::unhandled_exception:
      return "unhandled exception";
    case caf::exit_reason::unhandled_sync_failure:
      return "unhandled sync failure";
    case caf::exit_reason::user_shutdown:
      return "user shutdown";
    case caf::exit_reason::remote_link_unreachable:
      return "remote link unreachable";
  }
}

/// The base class for VAST actors.
class default_actor : public caf::event_based_actor
{
public:
  default_actor(char const* name = "actor")
    : name_{name}
  {
    VAST_DEBUG(this, "spawned");
    attach_functor([=](uint32_t reason)
    {
      VAST_DEBUG(this, "terminated (" << render_exit_reason(reason) << ')');
    });
  }

  template <typename F, typename... Vs>
  default_actor(F fun, Vs&&... vs)
    : default_actor{}
  {
    make_behavior_ = std::bind(fun, this, std::forward<Vs>(vs)...);
  }

  caf::behavior make_behavior() override
  {
    auto result = make_behavior_(this);
    make_behavior_ = nullptr;
    return result;
  }

  char const* name() const
  {
    return name_;
  }

  void name(char const* name)
  {
    name_ = name;
  }

  std::string label() const
  {
    return std::string{name()} + '#' + std::to_string(id());
  }

protected:
  bool downgrade_exit()
  {
    if (! current_mailbox_element()->mid.is_high_priority())
      return false;
    VAST_DEBUG(this, "delays exit");
    send(caf::message_priority::normal, this, current_message());
    return true;
  };

  auto catch_unexpected()
  {
    return caf::others() >> [=]
    {
      VAST_WARN(this, "got unexpected message from",
                current_sender() << ':', caf::to_string(current_message()));
    };
  }

private:
  char const* name_ = "actor";
  std::function<caf::behavior(default_actor*)> make_behavior_;
};

inline std::ostream& operator<<(std::ostream& out, default_actor const& a)
{
  out << a.label();
  return out;
}

template <typename Stream>
inline Stream& operator<<(Stream& out, default_actor const* a)
{
  VAST_ASSERT(a != nullptr);
  out << *a;
  return out;
}

/// An actor which can participate in a flow-controlled setting.
/// A flow-controlled actor sits in a chain of actors which propagate overload
/// signals back to the original sender.
///
/// Consider the following scenario, where a sender *S* sends data to *A*,
/// which then forwards it to *B* and *C*.
///
///             C
///            /
///     S --- A --- B
///
/// If any of the actors downstream of *S* get overloaded, the need to
/// propagate the signal back to *S*. The decision what to do with an overload
/// signal is *local* to the actor on the path. if *A* is a load-balancer and
/// receives a signal from *C*, it mayb simply stop sending messages to *C*
/// until it receives an underload signal from *C*. But if *A* is a message
/// replicator, it would propagate the signal up to *S*.
///
/// To implement such flow control scenarios, users must provide the following
/// flow-control handlers:
///
///   1. <overload_atom>
///   2. <underload_atom>
///   3. <upstream_atom, caf::actor>
///
/// An actor who just sits in a flow-control aware chain of actors typically
/// just needs to forward overload signals from downstream nodes back upstream.
/// The default handlers do this, and they can be integrated into the actor's
/// handler with the following functions:
///
///     behavior make_behavior() override
///     {
///       return
///       {
///         register_upstream_node(),
///         forward_overload(),
///         forward_underload(),
///         [=](down_msg const& msg)
///         {
///           if (remove_upstream_node(msg.source))
///             return;
///           // Handle DOWN from other nodes.
///         },
///         // Other handlers here
///       };
///     }
///
/// An actor the becomes overloaded calls `overloaded(true)` and underloaded
/// with `overloaded(false)`. Calls to these functions propagate the signal
/// upstream to the sender. At the source producing data, the handlers for
/// overload/underload should regulate the sender rate.
class flow_controlled_actor : public default_actor
{
public:
  flow_controlled_actor(char const* name = "flow-controlled-actor")
    : default_actor{name}
  {
    attach_functor([=](uint32_t) { upstream_.clear(); });
  }

protected:
  void add_upstream_node(caf::actor const& upstream)
  {
    VAST_DEBUG(this, "registers", upstream, "as upstream flow-control node");
    monitor(upstream);
    upstream_.insert(upstream);
  }

  bool remove_upstream_node(caf::actor_addr const& upstream)
  {
    auto i = std::find_if(
        upstream_.begin(),
        upstream_.end(),
        [&](auto& u) { return u == upstream; });
    if (i == upstream_.end())
      return false;
    VAST_DEBUG(this, "deregisters upstream flow-control node", upstream);
    upstream_.erase(i);
    return true;
  }

  bool overloaded() const
  {
    return overloaded_;
  }

  bool overloaded(bool flag)
  {
    if (flag)
    {
      if (overloaded())
        return false;
      VAST_DEBUG(this, "becomes overloaded");
      overloaded_ = true;
      propagate_overload();
    }
    else
    {
      if (! overloaded())
        return false;
      VAST_DEBUG(this, "becomes underloaded");
      overloaded_ = false;
      propagate_underload();
    }
    return true;
  }

  void propagate_overload()
  {
    for (auto& u : upstream_)
    {
      VAST_DEBUG(this, "propagates overload signal to", u);
      send(caf::message_priority::high, u, overload_atom::value);
    }
  }

  void propagate_underload()
  {
    for (auto& u : upstream_)
    {
      VAST_DEBUG(this, "propagates underload signal to", u);
      send(caf::message_priority::high, u, underload_atom::value);
    }
  }

  auto forward_overload()
  {
    return [=](overload_atom) { propagate_overload(); };
  }

  auto forward_underload()
  {
    return [=](underload_atom) { propagate_underload(); };
  }

  auto register_upstream_node()
  {
    return [=](upstream_atom, caf::actor const& upstream)
    {
      add_upstream_node(upstream);
    };
  }

  auto upstream() const
  {
    return upstream_;
  }

private:
  bool overloaded_ = false;
  util::flat_set<caf::actor> upstream_;
};

} // namespace vast

#endif
