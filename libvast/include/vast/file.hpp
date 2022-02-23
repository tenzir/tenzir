//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/config.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>

#include <cstdint>
#include <filesystem>

namespace vast {

/// A simple file abstraction.
class file {
public:
/// The native type of a file.
#if VAST_POSIX
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
  explicit file(std::filesystem::path p);

  file(const file&) = delete;
  file& operator=(const file&) = delete;

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
  [[nodiscard]] bool is_open() const;

  /// Reads a given number of bytes in to a buffer.
  /// @param sink The destination of the read.
  /// @param size The number of bytes to read.
  /// @returns The number of read bytes on success.
  [[nodiscard]] caf::expected<size_t> read(void* sink, size_t size);

  /// Writes a given number of bytes into a buffer.
  /// @param source The source of the write.
  /// @param size The number of bytes to write.
  /// @returns `caf::none` on success.
  [[nodiscard]] caf::error write(const void* source, size_t size);

  /// Seeks the file forward.
  /// @param bytes The number of bytes to seek forward relative to the current
  ///              position.
  /// @returns `true` on success.
  bool seek(size_t bytes);

  /// Retrieves the path for this file.
  /// @returns The path for this file.
  [[nodiscard]] const std::filesystem::path& path() const;

  /// Retrieves the native handle for this file.
  /// @returns The native handle.
  [[nodiscard]] native_type handle() const;

private:
  native_type handle_;
  bool is_open_ = false;
  bool seek_failed_ = false;
  std::filesystem::path path_;
};

} // namespace vast
