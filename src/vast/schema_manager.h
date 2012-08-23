#ifndef VAST_SCHEMA_MANAGER_H
#define VAST_SCHEMA_MANAGER_H

#include <cppa/cppa.hpp>
#include "vast/schema.h"

namespace vast {

/// Manages the existing taxonomies.
class schema_manager : public cppa::sb_actor<schema_manager>
{
  friend class cppa::sb_actor<schema_manager>;

public:
  /// Spawns the schema manager.
  schema_manager();

private:
  schema schema_;
  cppa::behavior init_state;
};

} // namespace vast

#endif
