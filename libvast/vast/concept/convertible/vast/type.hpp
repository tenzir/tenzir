#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_TYPE_HPP
#define VAST_CONCEPT_CONVERTIBLE_VAST_TYPE_HPP

#include "vast/type.hpp"

namespace vast {

class json;

bool convert(type const& t, json& j);
bool convert(type::attribute const& a, json& j);

} // namespace vast

#endif
