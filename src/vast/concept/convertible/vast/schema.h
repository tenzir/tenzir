#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_SCHEMA_H
#define VAST_CONCEPT_CONVERTIBLE_VAST_SCHEMA_H

namespace vast {

class json;
class schema;

bool convert(schema const& s, json& j);

} // namespace vast

#endif
