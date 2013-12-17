#ifndef VAST_ACTOR_H
#define VAST_ACTOR_H

#include <cppa/event_based_actor.hpp>
#include "vast/logger.h"

namespace vast {

namespace exit {

constexpr uint32_t done   = cppa::exit_reason::user_defined;
constexpr uint32_t stop   = cppa::exit_reason::user_defined + 1;
constexpr uint32_t error  = cppa::exit_reason::user_defined + 2;
constexpr uint32_t kill   = cppa::exit_reason::user_defined + 3;

} // namespace exit

/// An actor enhanced in 
template <typename Derived>
class actor : public cppa::event_based_actor
{
public:
  /// Implements `cppa::event_based_actor::init`.
  virtual void init() override
  {
    VAST_LOG_ACTOR_VERBOSE(derived()->description(), "spawned");
    derived()->act();
    if (! has_behavior())
    {
      VAST_LOG_ACTOR_ERROR(derived()->description(),
                           "act() did not set a behavior, terminating");
      quit(exit::error);
    }
  }

  /// Overrides `event_based_actor::on_exit`.
  virtual void on_exit() override
  {
    VAST_LOG_ACTOR_VERBOSE(derived()->description(), "terminated");
  }

private:
  Derived const* derived() const
  {
    return static_cast<Derived const*>(this);
  }

  Derived* derived()
  {
    return static_cast<Derived*>(this);
  }
};

} // namespace vast

#endif
