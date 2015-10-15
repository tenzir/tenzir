#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_DATA_H
#define VAST_CONCEPT_CONVERTIBLE_VAST_DATA_H

namespace vast {

class data;
class json;
class set;
class record;
class table;
class type;
class vector;

bool convert(vector const& v, json& j);
bool convert(set const& v, json& j);
bool convert(table const& v, json& j);
bool convert(record const& v, json& j);
bool convert(data const& v, json& j);

/// Converts data with a type to "zipped" JSON, i.e., the JSON object for
/// records contains the field names from the type corresponding to the given
/// data.
bool convert(data const& v, json& j, type const& t);

} // namespace vast

#endif
