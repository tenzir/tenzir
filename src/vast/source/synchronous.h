#ifndef VAST_SOURCE_SYNCHRONOUS_H
#define VAST_SOURCE_SYNCHRONOUS_H

#include <cppa/cppa.hpp>
#include "vast/option.h"

namespace vast {
namespace source {

/// A synchronous source that extracts events one by one.
struct synchronous : public cppa::event_based_actor
{
public:
  /// Constructs a synchronous source.
  synchronous() = default;

protected:
  /// Extracts a single event.
  /// @return The parsed event.
  virtual option<ze::event> extract() = 0;
  
  /// Checks whether the source has finished generating events.
  /// @return `true` if the source cannot provide more events.
  virtual bool finished() = 0;

private:
  /// Implements `event_based_actor::run`.
  virtual void init() override;

  cppa::actor_ptr upstream_;
  size_t batch_size_ = 0;
  size_t errors_ = 0;
  std::vector<ze::event> events_;
};

} // namespace source
} // namespace vast

#endif
