// SPDX-FileCopyrightText: (c) 2020 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/path.hpp"

#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/posix.hpp"
#include "vast/detail/string.hpp"
#include "vast/logger.hpp"

#include <caf/streambuf.hpp>

#include <fstream>
#include <iterator>

#if VAST_POSIX
#  include <cerrno>
#  include <cstdio>
#  include <cstring>
#  include <fcntl.h>
#  include <unistd.h>

#  include <sys/stat.h>
#  include <sys/types.h>
#  define VAST_CHDIR(P) (::chdir(P) == 0)
#  define VAST_CREATE_DIRECTORY(P)                                             \
    (::mkdir(P, S_IRWXU | S_IRWXG | S_IRWXO) == 0)
#  define VAST_CREATE_HARD_LINK(F, T) (::link(T, F) == 0)
#  define VAST_CREATE_SYMBOLIC_LINK(F, T, Flag) (::symlink(T, F) == 0)
#  define VAST_DELETE_FILE(P) (::unlink(P) == 0)
#  define VAST_DELETE_DIRECTORY(P) (::rmdir(P) == 0)
#  define VAST_MOVE_FILE(F, T) (::rename(F, T) == 0)
#else
#  define VAST_CHDIR(P) (::SetCurrentDirectoryW(P) != 0)
#  define VAST_CREATE_DIRECTORY(P) (::CreateDirectoryW(P, 0) != 0)
#  define VAST_CREATE_HARD_LINK(F, T) (create_hard_link_api(F, T, 0) != 0)
#  define VAST_CREATE_SYMBOLIC_LINK(F, T, Flag)                                \
    (create_symbolic_link_api(F, T, Flag) != 0)
#  define VAST_DELETE_FILE(P) (::DeleteFileW(P) != 0)
#  define VAST_DELETE_DIRECTORY(P)                                             \
    (::RemoveDirectoryW(P) != 0)(::CopyFileW(F, T, FailIfExistsBool) != 0)
#  define VAST_MOVE_FILE(F, T)                                                 \
    (::MoveFileExW(\ F, T, MOVEFILE_REPLACE_EXISTING | MOVEFILE_COPY_ALLOWED)  \
     != 0)
#endif // VAST_POSIX

namespace vast {

constexpr const char* path::separator;

path path::current() {
#if VAST_POSIX
  char buf[max_len];
  return ::getcwd(buf, max_len);
#else
  return {};
#endif
}

path::path(const char* str) : str_{str} {
}

path::path(std::string str) : str_{std::move(str)} {
}

path::path(std::string_view str) : str_{str} {
}

path& path::operator/=(const path& p) {
  if (p.empty() || (detail::ends_with(str_, separator) && p == separator))
    return *this;
  if (str_.empty())
    str_ = p.str_;
  else if (detail::ends_with(str_, separator) || p == separator)
    str_ = str_ + p.str_;
  else
    str_ = str_ + separator + p.str_;
  return *this;
}

path& path::operator+=(const path& p) {
  str_ = str_ + p.str_;
  return *this;
}

bool path::empty() const {
  return str_.empty();
}

path path::root() const {
#if VAST_POSIX
  if (!empty() && str_[0] == *separator)
    return str_.size() > 1 && str_[1] == *separator ? "//" : separator;
#endif
  return {};
}

path path::parent() const {
  if (str_ == separator || str_ == "." || str_ == "..")
    return {};
  auto pos = str_.rfind(separator);
  if (pos == std::string::npos)
    return {};
  if (pos == 0) // The parent is root.
    return separator;
  return str_.substr(0, pos);
}

path path::basename(bool strip_extension) const {
  if (str_ == separator)
    return separator;
  auto pos = str_.rfind(separator);
  if (pos == std::string::npos && !strip_extension) // Already a basename.
    return *this;
  if (pos == str_.size() - 1)
    return ".";
  if (pos == std::string::npos)
    pos = 0;
  auto base = str_.substr(pos + 1);
  if (!strip_extension)
    return base;
  auto ext = base.rfind(".");
  if (ext == 0)
    return {};
  if (ext == std::string::npos)
    return base;
  return base.substr(0, ext);
}

path path::complete() const {
  return root().empty() ? current() / *this : *this;
}

const std::string& path::str() const {
  return str_;
}

path::type path::kind() const {
#if VAST_POSIX
  struct stat st;
  if (::lstat(str_.data(), &st))
    return unknown;
  if (S_ISREG(st.st_mode))
    return regular_file;
  if (S_ISDIR(st.st_mode))
    return directory;
  if (S_ISLNK(st.st_mode))
    return symlink;
  if (S_ISBLK(st.st_mode))
    return block;
  if (S_ISCHR(st.st_mode))
    return character;
  if (S_ISFIFO(st.st_mode))
    return fifo;
  if (S_ISSOCK(st.st_mode))
    return socket;
#endif // VAST_POSIX
  return unknown;
}

bool path::is_absolute() const {
  return !str_.empty() && str_[0] == '/';
}

bool path::is_regular_file() const {
  return kind() == regular_file;
}

bool path::is_directory() const {
  return kind() == directory;
}

bool path::is_symlink() const {
  return kind() == symlink;
}

bool path::is_writable() const {
  return ::access(str().c_str(), W_OK) == 0;
}

bool operator==(const path& x, const path& y) {
  return x.str_ == y.str_;
}

bool operator<(const path& x, const path& y) {
  return x.str_ < y.str_;
}

std::vector<path> split(const path& p) {
  if (p.empty())
    return {};
  auto components = detail::split(p.str(), path::separator, "\\", -1, true);
  VAST_ASSERT(!components.empty());
  std::vector<path> result;
  size_t begin = 0;
  if (components[0].empty()) {
    // Path starts with "/".
    result.emplace_back(path::separator);
    begin = 2;
  }
  for (size_t i = begin; i < components.size(); i += 2)
    result.emplace_back(std::string{components[i]});
  return result;
}

bool exists(const path& p) {
#if VAST_POSIX
  struct stat st;
  return ::lstat(p.str().data(), &st) == 0;
#else
  return false;
#endif // VAST_POSIX
}

caf::error mkdir(const path& p) {
  auto components = split(p);
  if (components.empty())
    return caf::make_error(ec::filesystem_error, "cannot mkdir empty path");
  path c;
  for (auto& comp : components) {
    c /= comp;
    if (exists(c)) {
      auto kind = c.kind();
      if (!(kind == path::directory || kind == path::symlink))
        return caf::make_error(ec::filesystem_error,
                               "not a directory or symlink:", c);
    } else {
      if (!VAST_CREATE_DIRECTORY(c.str().data())) {
        // Because there exists a TOCTTOU issue here, we have to check again.
        if (errno == EEXIST) {
          auto kind = c.kind();
          if (!(kind == path::directory || kind == path::symlink))
            return caf::make_error(ec::filesystem_error,
                                   "not a directory or symlink:", c);
        } else {
          return caf::make_error(ec::filesystem_error,
                                 "failed in mkdir(2):", std::strerror(errno),
                                 c);
        }
      }
    }
  }
  return caf::none;
}

caf::expected<std::uintmax_t> file_size(const path& p) noexcept {
  struct stat st;
  if (::lstat(p.str().data(), &st) < 0)
    return caf::make_error(ec::filesystem_error, "file does not exist");
  // TODO: before returning, we may want to check whether we're dealing with a
  // regular file.
  return st.st_size;
}

caf::expected<std::string> load_contents(const path& p) {
  std::string contents;
  caf::containerbuf<std::string> obuf{contents};
  std::ostream out{&obuf};
  std::ifstream in{p.str()};
  if (!in)
    return caf::make_error(ec::filesystem_error,
                           "failed to read from file " + p.str());
  out << in.rdbuf();
  return contents;
}

} // namespace vast
