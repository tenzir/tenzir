#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_EVENT_H
#define VAST_CONCEPT_CONVERTIBLE_VAST_EVENT_H

namespace vast {

class event;
class json;

bool convert(event const& e, json& j);

} // namespace vast

#endif
