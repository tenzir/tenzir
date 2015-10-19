#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_TYPE_H
#define VAST_CONCEPT_CONVERTIBLE_VAST_TYPE_H

#include "vast/type.h"

namespace vast {

class json;

bool convert(type const& t, json& j);
bool convert(type::attribute const& a, json& j);

} // namespace vast

#endif
