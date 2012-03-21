#ifndef VAST_META_TAXONOMY_MANAGER_H
#define VAST_META_TAXONOMY_MANAGER_H

#include "vast/fs/path.h"
#include "vast/meta/forward.h"

namespace vast {
namespace meta {

/// Manages the existing taxonomies.
class taxonomy_manager
{
public:
    /// Initializes the taxonomy manager.
    /// @param tax_file The name of the taxonomy file.
    void init(fs::path const& tax_file);

    /// Retrieves the current taxonomy.
    /// @return A pointer to the current taxonomy.
    taxonomy_ptr get() const;

private:
    // For now, we have a single taxonomy.
    taxonomy_ptr tax_;
};

} // namespace meta
} // namespace vast

#endif
