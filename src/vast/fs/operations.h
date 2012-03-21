#ifndef VAST_FS_OPERATIONS_H
#define VAST_FS_OPERATIONS_H

#include "vast/fs/path.h"

namespace vast {
namespace fs {

/// Check whether a given path exists.
/// \param p The path to check.
/// \return \c true if \a p exists.
bool exists(const path& p);

/// Create a diretory (including missing parents).
/// \param p The path containing the directories to create.
void mkdir(const path& p);

} // namespace fs
} // namespace vast

#endif
