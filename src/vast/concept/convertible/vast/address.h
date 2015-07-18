#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_ADDRESS_H
#define VAST_CONCEPT_CONVERTIBLE_VAST_ADDRESS_H

namespace vast {

class address;
class json;

bool convert(address const& a, json& j);

} // namespace vast

#endif
