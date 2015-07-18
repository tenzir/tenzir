#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_VALUE_H
#define VAST_CONCEPT_CONVERTIBLE_VAST_VALUE_H

namespace vast {

class value;
class json;

bool convert(value const& v, json& j);

} // namespace vast

#endif
