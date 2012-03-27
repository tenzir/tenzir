#ifndef VAST_FS_OPERATIONS_H
#define VAST_FS_OPERATIONS_H

#include "vast/fs/path.h"

namespace vast {
namespace fs {

/// Checks whether a given path exists.
/// @param p The path to check.
/// @return @c true if @a p exists.
bool exists(path const& p);

/// Creates a diretory (including missing parents).
/// @param p The path containing the directories to create.
void mkdir(path const& p);

/// Checks whether a path is a regular file.
/// @param p The path to check.
bool is_file(path const& p);

/// Checks whether a path is a directory.
/// @param p The path to check.
bool is_directory(path const& p);

/// Checks whether a path is a symlink.
/// @param p The path to check.
bool is_symlink(path const& p);

/// Iterates over a directory and and invokes the provided callback for each
/// directory entry.
///
/// @param dir The path to the directory to iterate over.
///
/// @param f The callback function to invoke for each directory entry. The
/// callback parameters have the following meaning:
void each_dir_entry(fs::path const& dir,
                    std::function<void(fs::path const&)> f);

} // namespace fs
} // namespace vast

#endif
