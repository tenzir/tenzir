/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/config.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/iterator.hpp"
#include "vast/path.hpp"

#if VAST_POSIX
#  include <dirent.h>
#else

#include <functional>

namespace vast {
struct DIR;
} // namespace vast

#endif

namespace vast {

/// An ordered sequence of all the directory entries in a particular directory.
class directory {
public:
  using const_iterator = class iterator
    : public detail::iterator_facade<iterator, std::input_iterator_tag,
                                     const vast::path&, const vast::path&> {
  public:
    iterator(const directory* dir = nullptr);

    void increment();
    const path& dereference() const;
    bool equals(const iterator& other) const;

  private:
    path current_;
    const directory* dir_ = nullptr;
  };

  /// Constructs a directory stream.
  /// @param p The path to the directory.
  directory(vast::path p);

  directory(directory&&);
  directory(const directory&);

  directory& operator=(const directory&);
  directory& operator=(directory&&);

  ~directory();

  iterator begin() const;
  iterator end() const;

  /// Retrieves the path for this file.
  const vast::path& path() const;

private:
  vast::path path_;
  DIR* dir_ = nullptr;
};

/// Calculates the sum of the sizes of all regular files in the directory.
/// @param dir The directory to traverse.
/// @returns The size of all regular files in *dir*.
size_t recursive_size(const vast::directory& dir);

/// Recursively traverses a directory and lists all file names that match a
/// given filter expresssion.
/// @param dir The directory to enumerate.
/// @param filter An optional filter function to apply on the filename of every
/// file in *dir*, which allows for filtering specific files.
/// @param max_recursion The maximum number of nested directories to traverse.
/// @returns A list of file that match *filter*.
std::vector<path>
filter_dir(const path& dir, std::function<bool(const path&)> filter = {},
           size_t max_recursion = defaults::max_recursion);

} // namespace vast
