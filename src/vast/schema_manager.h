#ifndef VAST_SCHEMA_MANAGER_H
#define VAST_SCHEMA_MANAGER_H

#include <cppa/cppa.hpp>
#include "vast/schema.h"

namespace vast {

/// Manages the existing taxonomies.
class schema_manager : public cppa::event_based_actor
{
public:
  /// Spawns the schema manager.
  schema_manager() = default;

  /// Implements `event_based_actor::init`.
  virtual void init() final;

  /// Overrides `event_based_actor::on_exit`.
  virtual void on_exit() final;

private:
  schema schema_;
};

} // namespace vast

#endif
