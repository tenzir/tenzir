#ifndef VAST_META_SCHEMA_MANAGER_H
#define VAST_META_SCHEMA_MANAGER_H

#include <cppa/cppa.hpp>
#include "vast/fs/path.h"
#include "vast/meta/taxonomy.h"

namespace vast {
namespace meta {

/// Manages the existing taxonomies.
class schema_manager : public cppa::sb_actor<schema_manager>
{
  friend class cppa::sb_actor<schema_manager>;

public:
  /// Spawns the schema manager.
  schema_manager();

private:
  // For now, we have a single schema.
  std::unique_ptr<taxonomy> schema_;

  cppa::behavior init_state;
};

} // namespace meta
} // namespace vast

#endif
