#include "vast/file_system.h"

#include <cassert>
#include "vast/exception.h"
#include "vast/serialization.h"
#include "vast/convert.h"

#ifdef VAST_POSIX
#  include <cerrno>
#  include <cstring>
#  include <cstdio>
#  include <dirent.h>
#  include <fcntl.h>
#  include <unistd.h>
#  include <sys/stat.h>
#  include <sys/types.h>
#  define VAST_ERRNO errno 
#  define VAST_CHDIR(P)(::chdir(P) == 0)
#  define VAST_CREATE_DIRECTORY(P)(::mkdir(P, S_IRWXU|S_IRWXG|S_IRWXO) == 0)
#  define VAST_CREATE_HARD_LINK(F, T)(::link(T, F) == 0)
#  define VAST_CREATE_SYMBOLIC_LINK(F, T, Flag)(::symlink(T, F) == 0)
#  define VAST_DELETE_FILE(P)(::unlink(P) == 0)
#  define VAST_DELETE_DIRECTORY(P)(::rmdir(P) == 0)
#  define VAST_MOVE_FILE(F,T)(::rename(F, T) == 0)
#else
#  define VAST_ERRNO ::GetLastError()
#  define VAST_CHDIR(P)(::SetCurrentDirectoryW(P) != 0)
#  define VAST_CREATE_DIRECTORY(P)(::CreateDirectoryW(P, 0) != 0)
#  define VAST_CREATE_HARD_LINK(F,T)(create_hard_link_api(F, T, 0) != 0)
#  define VAST_CREATE_SYMBOLIC_LINK(F,T,Flag)(create_symbolic_link_api(F, T, Flag) != 0)
#  define VAST_DELETE_FILE(P)(::DeleteFileW(P) != 0)
#  define VAST_DELETE_DIRECTORY(P)(::RemoveDirectoryW(P) != 0) \
   (::CopyFileW(F, T, FailIfExistsBool) != 0)
#  define VAST_MOVE_FILE(F,T) \
   (::MoveFileExW(\ F, T, MOVEFILE_REPLACE_EXISTING|MOVEFILE_COPY_ALLOWED) != 0)
#endif // VAST_POSIX

namespace vast {

constexpr char const* path::separator;

path path::current()
{
#ifdef VAST_POSIX
  char buf[max_len];
  return string{::getcwd(buf, max_len)};
#else
  return {};
#endif
}

path::path(char const* str)
  : path{string(str)}
{
}

path::path(string str)
  : str_{std::move(str)}
{
}

path& path::operator/=(path const& p)
{
  if (p.empty() || (str_.ends_with(separator) && p == separator))
    return *this;
  if (str_.empty())
    str_ = p.str_;
  else if (str_.ends_with(separator) || p == separator)
    str_ = str_ + p.str_;
  else
    str_ = str_ + separator + p.str_;
  return *this;
}

path& path::operator+=(path const& p)
{
  str_ = str_ + p.str_;
  return *this;
}

bool path::empty() const
{
  return str_.empty();
}

path path::parent() const
{
  if (str_ == separator || str_ == "." || str_ == "..")
    return {};
  auto pos = str_.rfind(separator);
  if (pos == string::npos)
    return {};
  if (pos == 0) // The parent is root.
    return {separator};
  return str_.substr(0, pos);
}

path path::basename(bool strip_extension) const
{
  if (str_ == separator)
    return {separator};
  auto pos = str_.rfind(separator);
  if (pos == string::npos && ! strip_extension)  // Already a basename.
    return *this;
  if (pos == str_.size() - 1)
    return {"."};
  if (pos == string::npos)
    pos = 0;
  auto base = str_.substr(pos + 1);
  if (! strip_extension)
    return base;
  auto ext = base.rfind(".");
  if (ext == 0)
    return {};
  if (ext == string::npos)
    return base;
  return base.substr(0, ext);
}

path path::extension() const
{
  if (str_.back() == '.')
    return {"."};
  auto base = basename();
  auto ext = base.str_.rfind(".");
  if (base == path(string(".")) || ext == string::npos)
    return {};
  return base.str_.substr(ext);
}

std::vector<path> path::split() const
{
  if (empty())
    return {};
  auto components = str_.split(separator, "\\", -1, true);
  assert(! components.empty());
  std::vector<path> result;
  size_t begin = 0;
  if (string{components[0].first, components[0].second}.empty())
  {
    // Path starts with "/".
    result.emplace_back(separator);
    begin = 2;
  }
  for (size_t i = begin; i < components.size(); i += 2)
    result.emplace_back(string{components[i].first, components[i].second});
  return result;
}

path path::trim(int offset) const
{
  path r;
  if (empty() || offset == 0)
    return *this;
  auto pieces = split();
  std::pair<size_t, size_t> range{0, 0};
  if (offset > 0 && size_t(offset) < pieces.size())
    range.second = pieces.size() - size_t(offset);
  else if (offset < 0 && size_t(-offset) <= pieces.size())
    range = {pieces.size() - size_t(-offset), pieces.size()};
  for (size_t i = range.first; i < range.second; ++i)
    r /= pieces[i];
  return r;
}

string const& path::str() const
{
  return str_;
}

path::type path::kind() const
{
#ifdef VAST_POSIX
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

bool path::is_regular_file() const
{
  return kind() == regular_file;
}

bool path::is_directory() const
{
  return kind() == directory;
}

bool path::is_symlink() const
{
  return kind() == symlink;
}

void path::serialize(serializer& sink) const
{
  sink << str_;
}

void path::deserialize(deserializer& source)
{
  source >> str_;
}

bool operator==(path const& x, path const& y)
{
  return x.str_ == y.str_;
}

bool operator<(path const& x, path const& y)
{
  return x.str_ < y.str_;
}


file::file(path p)
  : path_(std::move(p))
{
}

file::file(path p, native_type handle)
  : handle_(handle),
    is_open_(true),
    path_(std::move(p))
{
}

file::file(file&& other)
  : handle_(other.handle_),
    is_open_(other.is_open_),
    seek_failed_(other.seek_failed_),
    path_(std::move(other.path_))
{
  other.handle_ = 0;
  other.is_open_ = false;
  other.seek_failed_ = false;
}

file::~file()
{
  close();
}

file& file::operator=(file&& other)
{
  using std::swap;
  swap(path_, other.path_);
  swap(handle_, other.handle_);
  swap(is_open_, other.is_open_);
  swap(seek_failed_, other.seek_failed_);
  return *this;
}

bool file::open(open_mode mode, bool append)
{
  if (is_open_)
    throw error::io("cannot open file twice");
  if (mode == read_only && append)
    throw error::io("cannot open file in read mode and append");
#ifdef VAST_POSIX
  int flags = O_CREAT;
  switch (mode)
  {
    case read_write:
      flags |= O_RDWR;
      break;
    case read_only:
      flags |= O_RDONLY;
      break;
    case write_only:
      flags |= O_WRONLY;
      break;
  }
  if (append)
    flags |= O_APPEND;
  handle_ = ::open(path_.str().data(), flags, 0644);
  if (handle_ > 0)
    is_open_ = true;
  return is_open_;
#else
  return false;
#endif // VAST_POSIX
}

bool file::close()
{
#ifdef VAST_POSIX
  if (! is_open_)
    return false;
  int result;
  do
  {
    result = ::close(handle_);
  }
  while (result < 0 && errno == EINTR);
  return ! result;
#else
  return false;
#endif // VAST_POSIX
}

bool file::is_open() const
{
  return is_open_;
}

bool file::read(void* sink, size_t bytes, size_t* got)
{
  if (got)
    *got = 0;
  if (! is_open_)
    return false;
#ifdef VAST_POSIX
  int result;
  do
  {
    result = ::read(handle_, sink, bytes);
  }
  while (result < 0 && errno == EINTR);
  if (result < 0)
    return false;       // Error, inspect errno for details.
  else if (result == 0) // EOF
    return false;
  else if (got)
    *got = result;
  return true;
#else
  return false;
#endif // VAST_POSIX
}

bool file::write(void const* source, size_t bytes, size_t* put)
{
  if (put)
    *put = 0;
  if (! is_open_)
    return false;
  size_t total = 0;
  auto buffer = reinterpret_cast<uint8_t const*>(source);
#ifdef VAST_POSIX
  while (total < bytes)
  {
    int written;
    do
    {
      written = ::write(handle_, buffer + total, bytes - total);
    }
    while (written < 0 && errno == EINTR);
    if (written <= 0)
      return false;
    total += written;
    if (put)
      *put += written;
  }
  return true;
#else
  return false;
#endif // VAST_POSIX
}

bool file::seek(size_t bytes, size_t *skipped)
{
  if (skipped)
    *skipped = 0;
  if (! is_open_)
    return false;
  if (seek_failed_)
    return false;
#ifdef VAST_POSIX
  if (::lseek(handle_, bytes, SEEK_CUR) == off_t(-1))
  {
    seek_failed_ = true;
    return false;
  }
#else
  return false;
#endif // VAST_POSIX
  if (skipped)
    *skipped = bytes;
  return true;
}


bool exists(path const& p)
{
#ifdef VAST_POSIX
  struct stat st;
  return ::lstat(p.str().data(), &st) == 0;
#else
  return false;
#endif // VAST_POSIX
}

bool rm(const path& p)
{
  // Because a file system only offers primitives to delete empty directories,
  // we have to recursively delete all files in a directory before deleting it.
  auto t = p.kind();
  if (t == path::type::directory)
  {
    traverse(p, [](path const& inner) { return rm(inner); });
    return VAST_DELETE_DIRECTORY(p.str().data());
  }
  if (t == path::type::regular_file || t == path::type::symlink)
    return VAST_DELETE_FILE(p.str().data());
  return false;
}

bool mkdir(path const& p)
{
  // Ideally we iterate over the path with a custom iterator.
  // This is the lazy version.
  string::size_type pos = 0;
  while (pos != string::npos)
  {
    string str = p.str();
    pos = str.find(path::separator, pos);
    if (pos != string::npos)
      ++pos;
    str = str.substr(0, pos);
    path component{str};
    if (! exists(component) && ! VAST_CREATE_DIRECTORY(str.data()))
      return false;
    if (! (component.is_directory() || component.is_symlink()))
      return false;
  }
  return true;
}

void traverse(path const& p, std::function<bool(path const&)> f)
{
#ifdef VAST_POSIX
  DIR* d = ::opendir(p.str().data());
  if (! d)
    return;
  struct dirent* ent;
  while ((ent = ::readdir(d)))
  {
    string str(ent->d_name);
    if (str != "." && str != ".." && ! f(p / std::move(str)))
      break;
  }
  ::closedir(d);
#else
  assert(false);
#endif // VAST_POSIX
}

} // namespace vast
