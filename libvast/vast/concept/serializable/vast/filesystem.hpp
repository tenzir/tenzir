#ifndef VAST_CONCEPT_SERIALIZABLE_FILESYSTEM_HPP
#define VAST_CONCEPT_SERIALIZABLE_FILESYSTEM_HPP

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

#include "vast/filesystem.hpp"

namespace vast {

inline void serialize(caf::serializer& sink, path const& p) {
  sink << p.str();
}

inline void serialize(caf::deserializer& source, path& p) {
  std::string str;
  source >> str;
  p = path{std::move(str)};
}

} // namespace vast

#endif
