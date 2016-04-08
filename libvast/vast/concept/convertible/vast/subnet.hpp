#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_SUBNET_HPP
#define VAST_CONCEPT_CONVERTIBLE_VAST_SUBNET_HPP

namespace vast {

class subnet;
class json;

bool convert(subnet const& sn, json& j);

} // namespace vast

#endif
