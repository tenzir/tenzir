#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_PATTERN_H
#define VAST_CONCEPT_CONVERTIBLE_VAST_PATTERN_H

namespace vast {

class pattern;
class json;

bool convert(pattern const& p, json& j);

} // namespace vast

#endif
