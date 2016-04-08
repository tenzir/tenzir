#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_EVENT_HPP
#define VAST_CONCEPT_CONVERTIBLE_VAST_EVENT_HPP

namespace vast {

class event;
class json;

bool convert(event const& e, json& j);

} // namespace vast

#endif
