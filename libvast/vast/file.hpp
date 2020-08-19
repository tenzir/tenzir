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
#include "vast/path.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>

#include <cstdint>

namespace vast {

/// A simple file abstraction.
class file {
  file(const file&) = delete;
  file& operator=(const file&) = delete;

public:
/// The native type of a file.
#ifdef VAST_POSIX
  typedef int native_type;
#else
  typedef void native_type;
#endif

  /// The mode in which to open a file.
  enum open_mode {
    invalid,
    read_only,
    write_only,
    read_write,
  };

  /// Default-constructs a file.
  file() = default;

  /// Constructs a file from a path.
  /// @param p The file path.
  explicit file(vast::path p);

  file(file&&) = default;

  /// Destroys and closes a file.
  ~file();

  file& operator=(file&&) = default;

  /// Opens the file.
  /// @param mode How to open the file. If not equal to `read_only`, the
  ///             function attempts to create non-existing parent directories.
  /// @param append If `false`, the first write truncates the file.
  /// @returns `true` on success.
  caf::expected<void> open(open_mode mode = read_write, bool append = false);

  /// Attempts to close the file.
  /// @returns `true` on success.
  bool close();

  /// Checks whether the file is open.
  /// @returns `true` iff the file is open
  bool is_open() const;

  /// Reads a given number of bytes in to a buffer.
  /// @param sink The destination of the read.
  /// @param size The number of bytes to read.
  /// @param got The number of bytes read.
  /// @returns `true` on success.
  bool read(void* sink, size_t size, size_t* got = nullptr);

  /// Writes a given number of bytes into a buffer.
  /// @param source The source of the write.
  /// @param size The number of bytes to write.
  /// @param put The number of bytes written.
  /// @returns `caf::none` on success.
  [[nodiscard]] caf::error
  write(const void* source, size_t size, size_t* put = nullptr);

  /// Seeks the file forward.
  /// @param bytes The number of bytes to seek forward relative to the current
  ///              position.
  /// @returns `true` on success.
  bool seek(size_t bytes);

  /// Retrieves the ::path for this file.
  /// @returns The ::path for this file.
  const vast::path& path() const;

  /// Retrieves the native handle for this file.
  /// @returns The native handle.
  native_type handle() const;

private:
  native_type handle_;
  bool is_open_ = false;
  bool seek_failed_ = false;
  vast::path path_;
};

} // namespace vast
