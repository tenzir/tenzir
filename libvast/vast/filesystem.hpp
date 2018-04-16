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

#ifdef VAST_POSIX
#  include <dirent.h>
#endif

#include <functional>
#include <string>
#include <vector>

#include "vast/expected.hpp"

#include "vast/detail/iterator.hpp"
#include "vast/detail/operators.hpp"

namespace vast {

struct access;

/// A filesystem path abstraction.
class path : detail::totally_ordered<path>,
             detail::addable<path>,
             detail::dividable<path> {
  friend access;

public:
#ifdef VAST_WINDOWS
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

  /// Retrieves the extension of this path.
  /// @param The extension including ".".
  path extension() const;

  /// Completes the path to an absolute path.
  /// @returns An absolute path.
  path complete() const;

  /// Retrieves a sub-path from beginning or end.
  ///
  /// @param n If positive, the function returns the first *n*
  /// components of the path. If negative, it returns last *n* components.
  ///
  /// @returns The path trimmed according to *n*.
  path trim(int n) const;

  /// Chops away path components from beginning or end.
  ///
  /// @param n If positive, the function chops away the first *n*
  /// components of the path. If negative, it removes the last *n* components.
  ///
  /// @returns The path trimmed according to *n*.
  path chop(int n) const;

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

  friend bool operator==(const path& x, const path& y);
  friend bool operator<(const path& x, const path& y);

  template <class Inspector>
  friend auto inspect(Inspector&f, path& p) {
    return f(p.str_);
  }

private:
  std::string str_;
};

constexpr bool close_on_destruction = true;

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
  file(vast::path p);

  /// Constructs a file from the OS' native file handle type.
  /// @param handle The file handle.
  /// @param p The file path.
  /// @param close_behavior Whether to close or leave open the file handle upon
  ///                       destruction.
  /// @pre The file identified via *handle* is open.
  file(native_type handle, bool close_behavior = close_on_destruction,
       vast::path p = {});

  file(file&&) = default;

  /// Destroys and closes a file.
  ~file();

  file& operator=(file&&) = default;

  /// Opens the file.
  /// @param mode How to open the file. If not equal to `read_only`, the
  ///             function attempts to create non-existing parent directories.
  /// @param append If `false`, the first write truncates the file.
  /// @returns `true` on success.
  expected<void> open(open_mode mode = read_write, bool append = false);

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
  /// @returns `true` on success.
  bool write(const void* source, size_t size, size_t* put = nullptr);

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
  bool close_on_destruction_ = !close_on_destruction;
  bool is_open_ = false;
  bool seek_failed_ = false;
  vast::path path_;
};

/// An ordered sequence of all the directory entries in a particular directory.
class directory {
public:
  using const_iterator =
    class iterator
      : public detail::iterator_facade<
          iterator,
          std::input_iterator_tag,
          const path&,
          const path&
        > {
  public:
    iterator(directory* dir = nullptr);

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

  iterator begin();
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

/// Splits the string at the path separator.
/// @param p The path to split.
/// @returns A vector of the path components.
std::vector<path> split(const path& p);

/// Checks whether the path exists on the filesystem.
/// @param p The path to check for existance.
/// @returns `true` if *p* exists.
bool exists(const path& p);

/// Creates a symlink (aka. "soft link").
/// @param target The existing file that should be linked.
/// @param link The symlink that points to *target*.
void create_symlink(const path& target, const path& link);

/// Deletes the path on the filesystem.
/// @param p The path to a directory to delete.
/// @returns `true` if *p* has been successfully deleted.
bool rm(const path& p);

/// If the path does not exist, create it as directory.
/// @param p The path to a directory to create.
/// @returns `true` on success or if *p* exists already.
expected<void> mkdir(const path& p);

// Loads file contents into a string.
// @param p The path of the file to load.
// @returns The contents of the file *p*.
expected<std::string> load_contents(const path& p);

} // namespace vast

namespace std {

template <>
struct hash<vast::path> {
  size_t operator()(const vast::path& p) const {
    return hash<std::string>{}(p.str());
  }
};

} // namespace std

