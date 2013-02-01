#ifndef VAST_ACTOR_H
#define VAST_ACTOR_H

#include <ze/config.h>
#include <cppa/cppa.hpp>

namespace vast {

/// The mixin for actors, either event-based or thread-mapped.
template <typename Derived>
struct minion
{
  cppa::behavior const& operating() const
  {
    return static_cast<Derived const*>(this)->operating_;
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

/// Spawns a minion as thread-mapped actor.
///
/// @tparam Minion A class that derives from vast::minion.
///
/// @param args The arguments passed to the minion.
///
/// @return The new minion as actor.
template <typename Minion, typename... Args>
cppa::actor_ptr unleash(Args&&... args)
{
  using namespace cppa;
#ifdef ZE_GCC
  auto m = std::make_shared<Minion>(std::forward<Args>(args)...);
  return spawn<detached>([=] { self->become(m->operating()); });
#else
  return spawn<detached>(
      [=]()
      {
        Minion m(std::forward<Args>(args)...);
        self->become(m.operating());
      });
#endif
}

} // namespace vast

#endif
