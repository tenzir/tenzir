// SPDX-FileCopyrightText: (c) 2020 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/file.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/posix.hpp"
#include "vast/error.hpp"
#include "vast/path.hpp"

#if VAST_POSIX
#  include <fcntl.h>
#  include <stdio.h>

#  include <sys/stat.h>
#  include <sys/types.h>
#endif // VAST_POSIX

namespace vast {

file::file(vast::path p) : path_{std::move(p)} {
}

file::~file() {
  // Don't close stdin/stdout implicitly.
  if (path_ != "-")
    close();
}

caf::expected<void> file::open(open_mode mode, bool append) {
  if (is_open_)
    return caf::make_error(ec::filesystem_error, "file already open");
  if (mode == read_only && append)
    return caf::make_error(ec::filesystem_error, "cannot open file in read and "
                                                 "append mode simultaneously");
#if VAST_POSIX
  // Support reading from STDIN and writing to STDOUT.
  if (path_ == "-") {
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
  if (mode != read_only && !exists(path_.parent())) {
    if (auto err = mkdir(path_.parent()))
      return caf::make_error(
        ec::filesystem_error,
        "failed to create parent directory: ", err.context());
  }
  handle_ = ::open(path_.str().data(), flags, 0644);
  if (handle_ != -1) {
    is_open_ = true;
    return {};
  }
  return caf::make_error(ec::filesystem_error,
                         "failed in open(2):", std::strerror(errno));
#else
  return caf::make_error(ec::filesystem_error, "not yet implemented");
#endif // VAST_POSIX
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
    return caf::make_error(ec::filesystem_error, "file is not open", path_);
  return detail::read(handle_, sink, bytes);
}

caf::error file::write(const void* source, size_t bytes) {
  if (!is_open_)
    return caf::make_error(ec::filesystem_error, "file is not open", path_);
  auto count = detail::write(handle_, source, bytes);
  if (!count)
    return count.error();
  if (*count != bytes)
    return caf::make_error(ec::filesystem_error, "incomplete read", path_);
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

const path& file::path() const {
  return path_;
}

file::native_type file::handle() const {
  return handle_;
}

} // namespace vast
