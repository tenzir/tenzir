#ifndef VAST_ACTOR_H
#define VAST_ACTOR_H

#include <caf/event_based_actor.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/send.hpp>
#include <caf/to_string.hpp>
#include "vast/logger.h"
#include "vast/util/operators.h"
#include "vast/util/flat_set.h"

namespace vast {

namespace flow_control {

struct announce : util::equality_comparable<announce>
{
  announce(caf::actor a = {})
    : source{std::move(a)}
  {
  }

  caf::actor source;

  friend bool operator==(announce const& lhs, announce const& rhs)
  {
    return lhs.source == rhs.source;
  }
};

struct overload : util::equality_comparable<overload>
{
  friend bool operator==(overload const&, overload const&)
  {
    return true;
  }
};

struct underload : util::equality_comparable<underload>
{
  friend bool operator==(underload const&, underload const&)
  {
    return true;
  }
};

} // namespace flow_control

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
    case caf::exit_reason::unhandled_sync_timeout:
      return "unhandled sync timeout";
    case caf::exit_reason::user_shutdown:
      return "user shutdown";
    case caf::exit_reason::remote_link_unreachable:
      return "remote link unreachable";
  }
}

/// The base class for VAST actors.
struct base_actor : caf::event_based_actor
{
  virtual caf::behavior act() = 0;

  caf::behavior make_behavior() override
  {
    VAST_LOG_DEBUG(*this, " spawned");
    return act();
  }

  virtual std::string name() const
  {
    return "actor";
  }

  std::string description() const
  {
    return name() + '#' + std::to_string(id());
  }

  void on_exit()
  {
    VAST_LOG_DEBUG(*this, " terminated (" <<
                   render_exit_reason(planned_exit_reason()) << ')');
  }
};

inline std::ostream& operator<<(std::ostream& out, base_actor const& a)
{
  out << a.description();
  return out;
}

// A composable, stateful CAF message handler.
struct component
{
  void at_down(base_actor*, caf::down_msg const&) { }
  void at_exit(base_actor*, caf::exit_msg const&) { }
};

/// An actor enhanced with a sequence of components.
template <typename Derived, typename... Components>
struct actor_mixin : base_actor, Components...
{
  caf::behavior act() override
  {
    using namespace caf;

    message_handler system =
    {
      [=](down_msg const& d)
      {
        exec_derived_down(d);
        exec_down<Components...>(d);
      },
      [=](exit_msg const& e)
      {
        exec_derived_exit(e);
        exec_exit<Components...>(e);
      }
    };

    auto first = static_cast<Derived*>(this)->make_handler();
    auto rest = build_handler<Components...>();

    return first.or_else(system).or_else(rest);
  }

  void at_down(caf::down_msg const&) { }
  void at_exit(caf::exit_msg const&) { }

private:
  void exec_derived_down(caf::down_msg const& down)
  {
    static_cast<Derived*>(this)->at_down(down);
  }

  void exec_derived_exit(caf::exit_msg const& msg)
  {
    static_cast<Derived*>(this)->at_exit(msg);
  }

  template <typename C>
  caf::message_handler build_handler()
  {
    return static_cast<C*>(this)->make_handler(static_cast<base_actor*>(this));
  }

  template <typename C, typename... Cs>
  std::enable_if_t<sizeof...(Cs) != 0, caf::message_handler>
  build_handler()
  {
    return build_handler<C>().or_else(build_handler<Cs...>());
  }

  template <typename C>
  void exec_down(caf::down_msg const& msg)
  {
    C::at_down(this, msg);
  }

  template <typename C, typename... Cs>
  std::enable_if_t<sizeof...(Cs) != 0>
  exec_down(caf::down_msg const& msg)
  {
    exec_down<C>(msg);
    exec_down<Cs...>(msg);
  }

  template <typename C>
  void exec_exit(caf::exit_msg const& msg)
  {
    C::at_exit(this, msg);
  }

  template <typename C, typename... Cs>
  std::enable_if_t<sizeof...(Cs) != 0>
  exec_exit(caf::exit_msg const& msg)
  {
    exec_exit<C>(msg);
    exec_exit<Cs...>(msg);
  }
};

/// Handles all unexpected messages.
struct sentinel : component
{
  void on_unexpected(base_actor* self, caf::message const& msg)
  {
    VAST_LOG_WARN(*self, " got unexpected message from " <<
                  self->last_sender() << ": " << to_string(msg));
  }

  caf::message_handler make_handler(base_actor* self)
  {
    using namespace caf;
    return { others() >> [=] { on_unexpected(self, self->last_dequeued()); } };
  }
};

/// Handles flow-control signals.
struct flow_controlled : component
{
  void at_down(base_actor* self, caf::down_msg const&)
  {
    auto i = std::find_if(
        upstream_.begin(),
        upstream_.end(),
        [&](caf::actor const& a) { return a == self->last_sender(); });

    if (i != upstream_.end())
      upstream_.erase(i);
  }

  void on_announce(base_actor* self, caf::actor const& upstream)
  {
    VAST_LOG_DEBUG(*self, " registers " << upstream <<
                   " as upstream node for flow-control");

    self->monitor(upstream);
    upstream_.insert(upstream);
  }

  void on_overload(base_actor* self)
  {
    VAST_LOG_DEBUG(*self, " got overload signal");
    for (auto& a : upstream_)
      caf::send_tuple_as(self, a, caf::message_priority::high,
                         self->last_dequeued());
  }

  void on_underload(base_actor* self)
  {
    VAST_LOG_DEBUG(*self, " got underload signal");
    for (auto& a : upstream_)
      caf::send_tuple_as(self, a, caf::message_priority::high,
                         self->last_dequeued());
  }

  auto upstream() const
  {
    return upstream_;
  }

  caf::message_handler make_handler(base_actor* self)
  {
    return
    {
      [=](flow_control::announce const& a)
      {
        on_announce(self, a.source);
      },
      [=](flow_control::overload const&)
      {
        on_overload(self);
      },
      [=](flow_control::underload const&)
      {
        on_underload(self);
      }
    };
  }

  util::flat_set<caf::actor> upstream_;
};

} // namespace vast

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

} // namespace caf

#endif
