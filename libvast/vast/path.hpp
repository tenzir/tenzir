//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/config.hpp"
#include "vast/detail/iterator.hpp"
#include "vast/detail/operators.hpp"

#include <caf/expected.hpp>

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace vast {

struct access;

/// A filesystem path abstraction.
class path : detail::totally_ordered<path>,
             detail::addable<path>,
             detail::dividable<path> {
  friend access;

public:
#if VAST_WINDOWS
  static constexpr const char* separator = "\\";
#else
  static constexpr const char* separator = "/";
#endif

  /// The maximum length of a path.
  static constexpr size_t max_len = 1024;

  /// The type of a file.
  enum type {
    unknown,
    regular_file,
    directory,
    symlink,
    block,
    character,
    fifo,
    socket
  };

  /// Retrieves the path of the current directory.
  /// @param The absolute path.
  static path current();

  /// Default-constructs an empty path.
  path() = default;

  /// Constructs a path from a C string.
  /// @param str The string representing of a path.
  path(const char* str);

  /// Constructs a path from a string.
  /// @param str The string representing of a path.
  path(std::string str);

  /// Constructs a path from a string_view.
  /// @param str The `string_view` representing of a path.
  path(std::string_view str);

  path& operator/=(const path& p);
  path& operator+=(const path& p);

  /// Checks whether the path is empty.
  /// @param `true` if the path is empty.
  bool empty() const;

  /// Retrieves the root of the path.
  /// @returns The root of the path or the empty if path if not absolute.
  path root() const;

  /// Retrieves the parent directory.
  /// @returns The parent of this path.
  path parent() const;

  /// Retrieves the basename of this path.
  /// @returns The basename of this path.
  path basename(bool strip_extension = false) const;

  /// Completes the path to an absolute path.
  /// @returns An absolute path.
  path complete() const;

  /// Retrieves the underlying string representation.
  /// @returns The string representation of the path.
  const std::string& str() const;

  //
  // Filesystem operations
  //

  /// Retrieves the type of the path.
  type kind() const;

  /// Checks whether the file type is a regular file.
  /// @returns `true` if the path points to a regular file.
  bool is_regular_file() const;

  /// Checks whether the file type is a directory.
  /// @returns `true` if the path points to a directory.
  bool is_directory() const;

  /// Checks whether the file type is a symlink.
  /// @returns `true` if the path is a symlink.
  bool is_symlink() const;

  /// Checks whether the path is writable.
  /// @returns `true` if the path is writable.
  bool is_writable() const;

  /// Checks whether the path is absolute.
  /// @returns `true` if the path is absolute.
  bool is_absolute() const;

  friend bool operator==(const path& x, const path& y);
  friend bool operator<(const path& x, const path& y);

  template <class Inspector>
  friend auto inspect(Inspector&f, path& p) {
    return f(p.str_);
  }

private:
  std::string str_;
};

/// Splits the string at the path separator.
/// @param p The path to split.
/// @returns A vector of the path components.
std::vector<path> split(const path& p);

/// Checks whether the path exists on the filesystem.
/// @param p The path to check for existance.
/// @returns `true` if *p* exists.
bool exists(const path& p);

/// If the path does not exist, create it as directory.
/// @param p The path to a directory to create.
/// @returns `caf::none` on success or if *p* exists already.
[[nodiscard]] caf::error mkdir(const path& p);
} // namespace vast

namespace std {

template <>
struct hash<vast::path> {
  size_t operator()(const vast::path& p) const {
    return hash<std::string>{}(p.str());
  }
};

} // namespace std
