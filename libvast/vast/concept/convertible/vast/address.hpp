#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_ADDRESS_HPP
#define VAST_CONCEPT_CONVERTIBLE_VAST_ADDRESS_HPP

namespace vast {

class address;
class json;

bool convert(address const& a, json& j);

} // namespace vast

#endif
