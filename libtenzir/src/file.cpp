//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/file.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/posix.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"

#if TENZIR_POSIX
#  include <sys/stat.h>
#  include <sys/types.h>

#  include <fcntl.h>
#  include <stdio.h>
#endif // TENZIR_POSIX

namespace tenzir {

file::file(std::filesystem::path p) : path_{std::move(p)} {
}

file::~file() {
  // Don't close stdin/stdout implicitly.
  if (path_.string() != "-")
    close();
}

caf::expected<void> file::open(open_mode mode, bool append) {
  if (is_open_)
    return caf::make_error(ec::filesystem_error, "file already open");
  if (mode == read_only && append)
    return caf::make_error(ec::filesystem_error, "cannot open file in read and "
                                                 "append mode simultaneously");
#if TENZIR_POSIX
  // Support reading from STDIN and writing to STDOUT.
  if (path_.string() == "-") {
    if (mode == read_write)
      return caf::make_error(ec::filesystem_error,
                             "cannot open - in read/write "
                             "mode");
    handle_ = ::fileno(mode == read_only ? stdin : stdout);
    is_open_ = true;
    return {};
  }
  int flags = 0;
  switch (mode) {
    case invalid:
      return caf::make_error(ec::filesystem_error, "invalid open mode");
    case read_write:
      flags = O_CREAT | O_RDWR;
      break;
    case read_only:
      flags = O_RDONLY;
      break;
    case write_only:
      flags = O_CREAT | O_WRONLY;
      break;
  }
  if (append)
    flags |= O_APPEND;
  errno = 0;
  std::error_code err{};
  if (path_.has_parent_path()) {
    const auto parent = path_.parent_path();
    const auto file_exists = std::filesystem::exists(parent, err);
    if (mode != read_only && !file_exists) {
      const auto created_directory
        = std::filesystem::create_directories(parent, err);
      if (!created_directory) {
        return caf::make_error(ec::filesystem_error,
                               fmt::format("failed to create parent directory "
                                           "{}: {}",
                                           parent, err.message()));
      }
    }
  }
  handle_ = ::open(path_.string().data(), flags, 0644);
  if (handle_ != -1) {
    is_open_ = true;
    return {};
  }
  return caf::make_error(ec::filesystem_error,
                         "failed in open(2):", detail::describe_errno());
#else
  return caf::make_error(ec::filesystem_error, "not yet implemented");
#endif // TENZIR_POSIX
}

bool file::close() {
  if (!is_open_)
    return false;
  if (detail::close(handle_))
    return false;
  is_open_ = false;
  return true;
}

bool file::is_open() const {
  return is_open_;
}

caf::expected<size_t> file::read(void* sink, size_t bytes) {
  if (!is_open_)
    return caf::make_error(ec::filesystem_error, "file is not open",
                           path_.string());
  return detail::read(handle_, sink, bytes);
}

caf::error file::write(const void* source, size_t bytes) {
  if (!is_open_)
    return caf::make_error(ec::filesystem_error, "file is not open",
                           path_.string());
  auto count = detail::write(handle_, source, bytes);
  if (!count)
    return count.error();
  if (*count != bytes)
    return caf::make_error(ec::filesystem_error, "incomplete read",
                           path_.string());
  return caf::none;
}

bool file::seek(size_t bytes) {
  if (!is_open_ || seek_failed_)
    return false;
  if (!detail::seek(handle_, bytes)) {
    seek_failed_ = true;
    return false;
  }
  return true;
}

const std::filesystem::path& file::path() const {
  return path_;
}

file::native_type file::handle() const {
  return handle_;
}

} // namespace tenzir
