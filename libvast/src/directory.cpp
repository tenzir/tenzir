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

#include "vast/directory.hpp"

#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/posix.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"

#include <caf/streambuf.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <system_error>

#if VAST_POSIX
#  include <sys/types.h>
#endif // VAST_POSIX

namespace vast {

directory::iterator::iterator(const directory* dir) : dir_{dir} {
  increment();
}

void directory::iterator::increment() {
  if (!dir_)
    return;
#if VAST_POSIX
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

const path& directory::iterator::dereference() const {
  return current_;
}

bool directory::iterator::equals(const iterator& other) const {
  return dir_ == other.dir_;
}

directory::directory(vast::path p)
  : path_{std::move(p)}, dir_{::opendir(path_.str().data())} {
}

directory::directory(directory&& d) : path_(std::move(d.path_)), dir_(nullptr) {
  std::swap(dir_, d.dir_);
}

directory::directory(const directory& d) : directory(d.path_) {
}

directory& directory::operator=(const directory& other) {
  directory tmp{other};
  std::swap(*this, tmp);
  return *this;
}

directory& directory::operator=(directory&& other) {
  directory tmp{std::move(other)};
  std::swap(*this, tmp);
  return *this;
}

directory::~directory() {
#if VAST_POSIX
  if (dir_)
    ::closedir(dir_);
#endif
}

directory::iterator directory::begin() const {
  return iterator{this};
}

directory::iterator directory::end() const {
  return {};
}

const path& directory::path() const {
  return path_;
}

caf::expected<size_t> recursive_size(const std::filesystem::path& root_dir) {
  size_t total_size = 0;

  std::error_code err{};
  auto dir = std::filesystem::recursive_directory_iterator(root_dir, err);
  if (err)
    return caf::make_error(ec::filesystem_error, err.message());

  for (const auto& f : dir) {
    if (f.is_regular_file()) {
      const auto size = f.file_size();
      VAST_TRACE("{} += {}", f.path().string(), size);
      total_size += size;
    }
  }

  return total_size;
}

caf::expected<std::vector<std::filesystem::path>>
filter_dir(const std::filesystem::path& root_dir,
           std::function<bool(const std::filesystem::path&)> filter,
           size_t max_recursion) {
  std::vector<std::filesystem::path> result;
  std::error_code err{};
  auto dir = std::filesystem::recursive_directory_iterator(root_dir, err);
  if (err)
    return caf::make_error(ec::filesystem_error, err.message());
  auto begin = std::filesystem::begin(dir);
  const auto end = std::filesystem::end(dir);
  while (begin != end) {
    const auto current_path = begin->path();
    const auto current_depth = static_cast<size_t>(begin.depth());
    if (current_depth >= max_recursion)
      return caf::make_error(ec::recursion_limit_reached,
                             fmt::format("reached recursion limit when "
                                         "filtering directory {}",
                                         root_dir));
    if (!filter || filter(current_path))
      result.push_back(current_path);
    ++begin;
  }
  std::sort(result.begin(), result.end());
  return result;
}

} // namespace vast
