#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_PORT_HPP
#define VAST_CONCEPT_CONVERTIBLE_VAST_PORT_HPP

namespace vast {

class port;
class json;

bool convert(port const& p, json& j);

} // namespace vast

#endif
