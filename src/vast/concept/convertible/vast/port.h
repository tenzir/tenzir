#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_PORT_H
#define VAST_CONCEPT_CONVERTIBLE_VAST_PORT_H

namespace vast {

class port;
class json;

bool convert(port const& p, json& j);

} // namespace vast

#endif
