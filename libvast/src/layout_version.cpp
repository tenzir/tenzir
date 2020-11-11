#include "vast/layout_version.hpp"

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
};

static_assert(layout_version{std::size(descriptions)} == layout_version::count,
              "Mismatch between number of layout versions and descriptions");

const char* to_string(layout_version v) {
  return descriptions[static_cast<uint8_t>(v)];
}

} // namespace

layout_version read_layout_version(const vast::path& dbdir) {
  if (!exists(dbdir))
    return layout_version::invalid;
  auto versionfile = dbdir / "VERSION";
  auto contents = io::read(versionfile);
  if (!contents)
    return layout_version::invalid;
  if (contents->empty())
    return layout_version::invalid;
  std::string_view sv{reinterpret_cast<const char*>(contents->data()),
                      contents->size()};
  // Only read until first newline.
  if (auto first_newline = sv.find('\n'))
    sv = std::string_view{sv.data(), first_newline};
  auto begin = std::begin(descriptions);
  auto end = std::end(descriptions);
  auto it = std::find(begin, end, sv);
  if (it == end)
    return layout_version::invalid;
  return static_cast<layout_version>(std::distance(begin, it));
}

caf::error initialize_layout_version(const vast::path& dbdir) {
  if (!exists(dbdir))
    return make_error(ec::filesystem_error,
                      "db-directory does not exist:", dbdir.str());
  auto version_path = dbdir / "VERSION";
  // Do nothing if a VERSION file already exists.
  if (exists(version_path))
    return ec::no_error;
  std::ofstream fs(version_path.str());
  fs << to_string(layout_version::v0) << std::endl;
  if (!fs)
    return make_error(ec::filesystem_error, "could not write version file");
  return ec::no_error;
}

std::ostream& operator<<(std::ostream& str, const layout_version& version) {
  return str << to_string(version);
}

} // namespace vast