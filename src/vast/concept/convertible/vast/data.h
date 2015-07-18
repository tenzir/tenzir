#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_DATA_H
#define VAST_CONCEPT_CONVERTIBLE_VAST_DATA_H

#include "vast/data.h"

namespace vast {

class json;

bool convert(vector const& v, json& j);
bool convert(set const& v, json& j);
bool convert(table const& v, json& j);
bool convert(record const& v, json& j);
bool convert(data const& v, json& j);

} // namespace vast

#endif
