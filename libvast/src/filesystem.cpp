#include <fstream>
#include <iterator>

#include <caf/streambuf.hpp>

#include "vast/filesystem.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/posix.hpp"
#include "vast/detail/string.hpp"

#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"

#ifdef VAST_POSIX
#  include <cerrno>
#  include <cstring>
#  include <cstdio>
#  include <fcntl.h>
#  include <unistd.h>
#  include <sys/stat.h>
#  include <sys/types.h>
#  define VAST_CHDIR(P)(::chdir(P) == 0)
#  define VAST_CREATE_DIRECTORY(P)(::mkdir(P, S_IRWXU|S_IRWXG|S_IRWXO) == 0)
#  define VAST_CREATE_HARD_LINK(F, T)(::link(T, F) == 0)
#  define VAST_CREATE_SYMBOLIC_LINK(F, T, Flag)(::symlink(T, F) == 0)
#  define VAST_DELETE_FILE(P)(::unlink(P) == 0)
#  define VAST_DELETE_DIRECTORY(P)(::rmdir(P) == 0)
#  define VAST_MOVE_FILE(F,T)(::rename(F, T) == 0)
#else
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

path path::current() {
#ifdef VAST_POSIX
  char buf[max_len];
  return ::getcwd(buf, max_len);
#else
  return {};
#endif
}

path::path(char const* str) : str_{str} {
}

path::path(std::string str) : str_{std::move(str)} {
}

path& path::operator/=(path const& p) {
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

path& path::operator+=(path const& p) {
  str_ = str_ + p.str_;
  return *this;
}

bool path::empty() const {
  return str_.empty();
}

path path::root() const {
#ifdef VAST_POSIX
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

path path::extension() const {
  if (str_.back() == '.')
    return ".";
  auto base = basename();
  auto ext = base.str_.rfind(".");
  if (base == "." || ext == std::string::npos)
    return {};
  return base.str_.substr(ext);
}

path path::complete() const {
  return root().empty() ? current() / *this : *this;
}

path path::trim(int n) const {
  if (empty())
    return *this;
  else if (n == 0)
    return {};
  auto pieces = split(*this);
  size_t first = 0;
  size_t last = pieces.size();
  if (n < 0)
    first = last - std::min(size_t(-n), pieces.size());
  else
    last = std::min(size_t(n), pieces.size());
  path r;
  for (size_t i = first; i < last; ++i)
    r /= pieces[i];

  return r;
}

path path::chop(int n) const {
  if (empty() || n == 0)
    return *this;
  auto pieces = split(*this);
  size_t first = 0;
  size_t last = pieces.size();
  if (n < 0)
    last -= std::min(size_t(-n), pieces.size());
  else
    first += std::min(size_t(n), pieces.size());
  path r;
  for (size_t i = first; i < last; ++i)
    r /= pieces[i];
  return r;
}

std::string const& path::str() const {
  return str_;
}

path::type path::kind() const {
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

bool path::is_regular_file() const {
  return kind() == regular_file;
}

bool path::is_directory() const {
  return kind() == directory;
}

bool path::is_symlink() const {
  return kind() == symlink;
}

bool operator==(path const& x, path const& y) {
  return x.str_ == y.str_;
}

bool operator<(path const& x, path const& y) {
  return x.str_ < y.str_;
}

file::file(vast::path p) : path_{std::move(p)} {
}

file::file(native_type handle, bool close_behavior, vast::path p)
  : handle_{handle},
    close_on_destruction_{close_behavior},
    is_open_{true},
    path_{std::move(p)} {
}

file::~file() {
  // Don't close stdin/stdout implicitly.
  if (path_ != "-" && close_on_destruction_)
    close();
}

maybe<void> file::open(open_mode mode, bool append) {
  if (is_open_)
    return fail<ec::filesystem_error>("file already open");
  if (mode == read_only && append)
    return fail<ec::filesystem_error>(
      "cannot open file in read and append mode simultaneously");
#ifdef VAST_POSIX
  // Support reading from STDIN and writing to STDOUT.
  if (path_ == "-") {
    if (mode == read_write)
      return fail<ec::filesystem_error>("cannot open - in read/write mode");
    handle_ = ::fileno(mode == read_only ? stdin : stdout);
    is_open_ = true;
    return {};
  }
  int flags = 0;
  switch (mode) {
    case invalid:
      return fail<ec::filesystem_error>("invalid open mode");
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
    auto m = mkdir(path_.parent());
    if (!m)
      return fail<ec::filesystem_error>("failed to create parent directory: ",
                                        m.error());
  }
  handle_ = ::open(path_.str().data(), flags, 0644);
  if (handle_ != -1) {
    is_open_ = true;
    return {};
  }
  return fail<ec::filesystem_error>(std::strerror(errno));
#else
  return fail<ec::filesystem_error>("not yet implemented");
#endif // VAST_POSIX
}

bool file::close() {
  if (!(is_open_ && detail::close(handle_)))
    return false;
  is_open_ = false;
  return true;
}

bool file::is_open() const {
  return is_open_;
}

bool file::read(void* sink, size_t bytes, size_t* got) {
  return is_open_ && detail::read(handle_, sink, bytes, got);
}

bool file::write(void const* source, size_t bytes, size_t* put) {
  return is_open_ && detail::write(handle_, source, bytes, put);
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

path const& file::path() const {
  return path_;
}

file::native_type file::handle() const {
  return handle_;
}

directory::iterator::iterator(directory* dir) : dir_{dir} {
  increment();
}

void directory::iterator::increment() {
  if (!dir_)
    return;
#ifdef VAST_POSIX
  if (!dir_->dir_) {
    dir_ = nullptr;
  } else if (auto ent = ::readdir(dir_->dir_)) {
    auto d = ent->d_name;
    VAST_ASSERT(d);
    auto dot = d[0] == '.' && d[1] == '\0';
    auto dotdot = d[0] == '.' && d[1] == '.' && d[2] == '\0';
    if (dot || dotdot)
      increment();
    else
      current_ = dir_->path_ / d;
  } else {
    dir_ = nullptr;
  }
#endif
}

path const& directory::iterator::dereference() const {
  return current_;
}

bool directory::iterator::equals(iterator const& other) const {
  return dir_ == other.dir_;
}

directory::directory(vast::path p)
  : path_{std::move(p)}, dir_{::opendir(path_.str().data())} {
}

directory::~directory() {
#ifdef VAST_POSIX
  if (dir_)
    ::closedir(dir_);
#endif
}

directory::iterator directory::begin() {
  return iterator{this};
}

directory::iterator directory::end() const {
  return {};
}

path const& directory::path() const {
  return path_;
}

std::vector<path> split(path const& p) {
  if (p.empty())
    return {};
  auto components = detail::to_strings(
    detail::split(p.str(), path::separator, "\\", -1, true));
  VAST_ASSERT(!components.empty());
  std::vector<path> result;
  size_t begin = 0;
  if (components[0].empty()) {
    // Path starts with "/".
    result.emplace_back(path::separator);
    begin = 2;
  }
  for (size_t i = begin; i < components.size(); i += 2)
    result.emplace_back(std::move(components[i]));
  return result;
}

bool exists(path const& p) {
#ifdef VAST_POSIX
  struct stat st;
  return ::lstat(p.str().data(), &st) == 0;
#else
  return false;
#endif // VAST_POSIX
}

void create_symlink(path const& target, path const& link) {
  ::symlink(target.str().c_str(), link.str().c_str());
}

bool rm(const path& p) {
  // Because a file system only offers primitives to delete empty directories,
  // we have to recursively delete all files in a directory before deleting it.
  auto t = p.kind();
  if (t == path::type::directory) {
    for (auto& entry : directory{p})
      if (! rm(entry))
        return false;
    return VAST_DELETE_DIRECTORY(p.str().data());
  }
  if (t == path::type::regular_file || t == path::type::symlink)
    return VAST_DELETE_FILE(p.str().data());
  return false;
}

maybe<void> mkdir(path const& p) {
  auto components = split(p);
  if (components.empty())
    return fail<ec::filesystem_error>("cannot mkdir empty path");
  path c;
  for (auto& comp : components) {
    c /= comp;
    if (exists(c)) {
      auto kind = c.kind();
      if (!(kind == path::directory || kind == path::symlink))
        return fail<ec::filesystem_error>("not a directory or symlink:", c);
    } else {
      if (!VAST_CREATE_DIRECTORY(c.str().data())) {
        // Because there exists a TOCTTOU issue here, we have to check again.
        if (errno == EEXIST) {
          auto kind = c.kind();
          if (!(kind == path::directory || kind == path::symlink))
            return fail<ec::filesystem_error>("not a directory or symlink:", c);
        } else {
          return fail<ec::filesystem_error>(std::strerror(errno), c);
        }
      }
    }
  }
  return {};
}

// Loads file contents into a string.
maybe<std::string> load_contents(path const& p) {
  std::string contents;
  caf::containerbuf<std::string> obuf{contents};
  std::ostream out{&obuf};
  std::ifstream in{p.str()};
  out << in.rdbuf();
  return std::move(contents);
}

} // namespace vast
