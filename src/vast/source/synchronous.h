#ifndef VAST_SOURCE_SYNCHRONOUS_H
#define VAST_SOURCE_SYNCHRONOUS_H

#include <cppa/cppa.hpp>
#include "vast/actor.h"
#include "vast/event.h"
#include "vast/optional.h"

namespace vast {
namespace source {

/// A synchronous source that extracts events one by one.
struct synchronous : public actor<synchronous>
{
public:
  /// Spawns a synchronous source.
  /// @param The actor receiving the generated events.
  /// @param batch_size The number of events
  synchronous(cppa::actor_ptr sink, size_t batch_size = 0);

  void act();
  virtual char const* description() const = 0;

protected:
  /// Extracts a single event.
  /// @returns The parsed event.
  virtual optional<event> extract() = 0;
  
  /// Checks whether the source has finished generating events.
  /// @returns `true` if the source cannot provide more events.
  virtual bool finished() = 0;

private:
  void send_events();

  cppa::actor_ptr sink_;
  size_t batch_size_ = 0;
  size_t errors_ = 0;
  std::vector<event> events_;
};

} // namespace source
} // namespace vast

#endif
