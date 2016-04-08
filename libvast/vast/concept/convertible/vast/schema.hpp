#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_SCHEMA_HPP
#define VAST_CONCEPT_CONVERTIBLE_VAST_SCHEMA_HPP

namespace vast {

class json;
class schema;

bool convert(schema const& s, json& j);

} // namespace vast

#endif
