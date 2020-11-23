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

#include "vast/db_version.hpp"

#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/io/read.hpp"
#include "vast/io/write.hpp"
#include "vast/logger.hpp"

#include <caf/expected.hpp>

#include <fstream>
#include <iterator>

namespace vast {

namespace {

const char* descriptions[]{
  "invalid",
  "v0",
  "v1",
};

static_assert(db_version{std::size(descriptions)} == db_version::count,
              "Mismatch between number of DB versions and descriptions");

const char* to_string(db_version v) {
  return descriptions[static_cast<uint8_t>(v)];
}

} // namespace

std::ostream& operator<<(std::ostream& str, const db_version& version) {
  return str << to_string(version);
}

db_version read_db_version(const vast::path& db_dir) {
  if (!exists(db_dir))
    return db_version::invalid;
  auto versionfile = db_dir / "VERSION";
  auto contents = io::read(versionfile);
  if (!contents)
    return db_version::invalid;
  if (contents->empty())
    return db_version::invalid;
  std::string_view sv{reinterpret_cast<const char*>(contents->data()),
                      contents->size()};
  // Only read until first newline.
  if (auto first_newline = sv.find('\n'))
    sv = std::string_view{sv.data(), first_newline};
  auto begin = std::begin(descriptions);
  auto end = std::end(descriptions);
  auto it = std::find(begin, end, sv);
  if (it == end)
    return db_version::invalid;
  return static_cast<db_version>(std::distance(begin, it));
}

caf::error initialize_db_version(const vast::path& db_dir) {
  if (!exists(db_dir))
    return make_error(ec::filesystem_error,
                      "db-directory does not exist:", db_dir.str());
  auto version_path = db_dir / "VERSION";
  // Do nothing if a VERSION file already exists.
  if (exists(version_path))
    return ec::no_error;
  std::ofstream fs(version_path.str());
  fs << to_string(db_version::latest) << std::endl;
  if (!fs)
    return make_error(ec::filesystem_error, "could not write version file");
  return ec::no_error;
}

} // namespace vast
