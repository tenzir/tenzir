#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_SUBNET_H
#define VAST_CONCEPT_CONVERTIBLE_VAST_SUBNET_H

namespace vast {

class subnet;
class json;

bool convert(subnet const& sn, json& j);

} // namespace vast

#endif
