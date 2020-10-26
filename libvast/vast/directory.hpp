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
#include "vast/detail/iterator.hpp"
#include "vast/path.hpp"

#ifdef VAST_POSIX
#  include <dirent.h>
#endif

namespace vast {

/// An ordered sequence of all the directory entries in a particular directory.
class directory {
public:
  using const_iterator = class iterator
    : public detail::iterator_facade<iterator, std::input_iterator_tag,
                                     const path&, const path&> {
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

  ~directory();

  iterator begin() const;
  iterator end() const;

  /// Retrieves the ::path for this file.
  /// @returns The ::path for this file.
  const vast::path& path() const;

private:
  vast::path path_;
#ifdef VAST_POSIX
  DIR* dir_ = nullptr;
#endif
};

/// Calculates the sum of the sizes of all regular files in the directory.
/// @param dir The directory to traverse.
/// @returns The size of all regular files in *dir*.
size_t recursive_size(const vast::directory& dir);

} // namespace vast
