#ifndef VAST_SCHEMA_MANAGER_H
#define VAST_SCHEMA_MANAGER_H

#include "vast/actor.h"
#include "vast/schema.h"

namespace vast {

/// Manages the existing taxonomies.
class schema_manager : public actor<schema_manager>
{
public:
  /// Spawns the schema manager.
  schema_manager() = default;

  void act();
  char const* description() const;

private:
  schema schema_;
};

} // namespace vast

#endif
