#ifndef VAST_SCHEMA_MANAGER_H
#define VAST_SCHEMA_MANAGER_H

#include "vast/actor.h"
#include "vast/schema.h"

namespace vast {

/// Manages the existing taxonomies.
class schema_manager_actor : public actor<schema_manager_actor>
{
public:
  /// Spawns the schema manager.
  schema_manager_actor() = default;

  void act();
  char const* description() const;

private:
  schema schema_;
};

} // namespace vast

#endif
