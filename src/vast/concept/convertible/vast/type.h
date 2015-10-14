#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_TYPE_H
#define VAST_CONCEPT_CONVERTIBLE_VAST_TYPE_H

namespace vast {

class type;

bool convert(type const& t, json& j, bool flatten = false);
bool convert(type::attribute const& a, json& j);

} // namespace vast

#endif
