#ifndef VAST_CONCEPT_PARSEABLE_VAST_DETAIL_TO_SCHEMA_H
#define VAST_CONCEPT_PARSEABLE_VAST_DETAIL_TO_SCHEMA_H

#include <string>

namespace vast {

class schema;

namespace detail {

trial<schema> to_schema(std::string const& str);

} // namespace detail
} // namespace vast

#endif
