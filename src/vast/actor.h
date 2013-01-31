#ifndef VAST_ACTOR_H
#define VAST_ACTOR_H

#include <cppa/cppa.hpp>

namespace vast {

using cppa::actor_ptr;
using cppa::behavior;

/// The mixin for actors, either event-based or thread-mapped.
template <typename Derived>
struct minion
{
  behavior const& operating() const
  {
    return static_cast<Derived*>(this)->operating_;
  }
};

/// The base class for event-based actors.
template <typename Derived>
class actor : minion<Derived>, public cppa::event_based_actor
{
public:
  void init() override
  {
    become(this->operating());
  }
};

/// Spawns a monitored thread-mapped actor.
///
/// @tparam Minion A class that derives from vast::minion.
///
/// @param guardian The actor monitoring the thread-mapped actor.
///
/// @return An actor pointer to the new minion.
template <typename Minion, typename... Args>
actor_ptr unleash(actor_ptr guarding, Args&&... args)
{
  using namespace cppa;
  auto a = spawn<detached>(
      [=]()
      {
        Minion m(std::forward<Args>(args)...);
        self->become(m.operating());
      });

  guardian->monitor(a);
  return a;
}

} // namespace vast

#endif
