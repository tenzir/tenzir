#ifndef VAST_ACTOR_H
#define VAST_ACTOR_H

#include <caf/event_based_actor.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/to_string.hpp>
#include "vast/logger.h"

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
    case caf::exit_reason::unhandled_sync_timeout:
      return "unhandled sync timeout";
    case caf::exit_reason::user_shutdown:
      return "user shutdown";
    case caf::exit_reason::remote_link_unreachable:
      return "remote link unreachable";
  }
}

/// The base class for event-based actors in VAST.
struct actor_base : caf::event_based_actor
{
protected:
  void catch_all(bool flag)
  {
    catch_all_ = flag;
  }

  caf::behavior make_behavior() final
  {
    using namespace caf;

    VAST_LOG_ACTOR_DEBUG(describe(), "spawned");

    auto catch_all =
      others() >> [=]
      {
        VAST_LOG_ACTOR_WARN("got unexpected message from " <<
                            last_sender() << ": " <<
                            to_string(last_dequeued()));
      };

    auto partial = act();

    return catch_all_ ? partial.or_else(catch_all) : partial;
  }

  void on_exit()
  {
    VAST_LOG_ACTOR_DEBUG(describe(), "terminated (" <<
                         render_exit_reason(planned_exit_reason()) << ')');
  }

  virtual caf::message_handler act() = 0;
  virtual std::string describe() const = 0;

private:
  bool catch_all_ = true;
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

using caf::replies_to;

} // namespace vast

namespace caf {

inline std::ostream& operator<<(std::ostream& out, actor_addr const& a)
{
  out << '@' << a.id();
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
