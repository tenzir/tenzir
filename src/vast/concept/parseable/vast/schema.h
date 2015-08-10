#ifndef VAST_CONCEPT_PARSEABLE_VAST_SCHEMA_H
#define VAST_CONCEPT_PARSEABLE_VAST_SCHEMA_H

#include "vast/schema.h"

#include "vast/concept/parseable/vast/detail/schema.h"

namespace vast {

template <>
struct parser_registry<schema> {
  using type = detail::schema_parser;
};

namespace parsers {

static auto const schema = make_parser<vast::schema>();

} // namespace parsers
} // namespace vast

#endif
