#ifndef VAST_ACTOR_H
#define VAST_ACTOR_H

#include <cppa/event_based_actor.hpp>
#include <cppa/typed_event_based_actor.hpp>
#include "vast/logger.h"

namespace vast {

namespace exit {

constexpr uint32_t done   = cppa::exit_reason::user_defined;
constexpr uint32_t stop   = cppa::exit_reason::user_defined + 1;
constexpr uint32_t error  = cppa::exit_reason::user_defined + 2;
constexpr uint32_t kill   = cppa::exit_reason::user_defined + 3;

} // namespace exit

/// The base class for event-based actors in VAST.
struct actor_base : cppa::event_based_actor
{
protected:
  cppa::behavior make_behavior() final
  {
    VAST_LOG_ACTOR_DEBUG(describe(), "spawned");
    return act();
  }

  void on_exit() final
  {
    VAST_LOG_ACTOR_DEBUG(describe(), "terminated");
  }

  virtual cppa::behavior act() = 0;
  virtual std::string describe() const = 0;
};

/// The base class for typed actors in VAST.
template <typename Actor>
struct typed_actor_base : Actor::base
{
protected:
  typename Actor::behavior_type make_behavior() final
  {
    VAST_LOG_ACTOR_DEBUG(describe(), "spawned");
    return act();
  }

  void on_exit() final
  {
    VAST_LOG_ACTOR_DEBUG(describe(), "terminated");
  }

  virtual typename Actor::behavior_type act() = 0;
  virtual std::string describe() const = 0;
};

using cppa::replies_to;

} // namespace vast

namespace cppa {

inline auto operator<<(std::ostream& out, actor_addr const& a) -> decltype(out)
{
  out << '@' << a.id();
  return out;
}

inline auto operator<<(std::ostream& out, actor const& a) -> decltype(out)
{
  out << a.address();
  return out;
}

inline auto operator<<(std::ostream& out, abstract_actor const& a)
  -> decltype(out)
{
  out << a.address();
  return out;
}

} // namespace cppa

#endif
