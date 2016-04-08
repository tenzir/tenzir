#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_VALUE_HPP
#define VAST_CONCEPT_CONVERTIBLE_VAST_VALUE_HPP

namespace vast {

class value;
class json;

bool convert(value const& v, json& j);

} // namespace vast

#endif
