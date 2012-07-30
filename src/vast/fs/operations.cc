#include "vast/fs/operations.h"

#include <boost/filesystem.hpp>
#include "vast/fs/exception.h"

namespace vast {
namespace fs {

bool exists(path const& p)
{
  return boost::filesystem::exists(p);
}

void mkdir(path const& p)
{
  if (! boost::filesystem::create_directories(p))
    throw dir_exception("mkdir", p.string());
}

bool is_file(path const& p)
{
  return boost::filesystem::is_regular_file(p);
}

bool is_directory(path const& p)
{
  return boost::filesystem::is_directory(p);
}

bool is_symlink(path const& p)
{
  return boost::filesystem::is_symlink(p);
}

void each_dir_entry(path const& dir, std::function<void(path const&)> f)
{
  if (! fs::exists(dir))
    throw dir_exception("does not exist", dir.string());

  if (! fs::is_directory(dir))
    throw dir_exception("not a directory", dir.string());

  std::vector<path> paths;
  std::copy(boost::filesystem::directory_iterator(dir),
            boost::filesystem::directory_iterator(),
            std::back_inserter(paths));

  // Directories first, then the rest.
  std::sort(paths.begin(), paths.end(),
            [](path const& x, path const& y) -> bool
            {
              if (fs::is_directory(x) && ! fs::is_directory(y))
                return true;
              else if (! fs::is_directory(x) && fs::is_directory(y))
                return false;
              else
                return x < y;
            });

  for (auto const& path : paths)
    f(path);
}

void each_file_entry(fs::path const& dir, std::function<void(path const&)> f)
{
  each_dir_entry(
      dir,
      [f](fs::path const& p)
      {
        if (fs::is_directory(p))
          each_file_entry(p, f);
        else
          f(p);
      });
}

} // namespace fs
} // namespace vast
