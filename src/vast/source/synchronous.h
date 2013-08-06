#ifndef VAST_SOURCE_SYNCHRONOUS_H
#define VAST_SOURCE_SYNCHRONOUS_H

#include <cppa/cppa.hpp>
#include "vast/event.h"
#include "vast/option.h"

namespace vast {
namespace source {

/// A synchronous source that extracts events one by one.
struct synchronous : public cppa::event_based_actor
{
public:
  /// Spawns a synchronous source.
  /// @param The actor receiving the generated events.
  /// @param batch_size The number of events
  synchronous(cppa::actor_ptr sink, size_t batch_size = 0);

  /// Implements `event_based_actor::run`.
  virtual void init() final;

  /// Implements `event_based_actor::on_exit`.
  virtual void on_exit() override;

protected:
  /// Extracts a single event.
  /// @return The parsed event.
  virtual option<event> extract() = 0;
  
  /// Checks whether the source has finished generating events.
  /// @return `true` if the source cannot provide more events.
  virtual bool finished() = 0;

private:
  void run();

  cppa::actor_ptr sink_;
  size_t batch_size_ = 0;
  size_t errors_ = 0;
  std::vector<event> events_;
};

} // namespace source
} // namespace vast

#endif
