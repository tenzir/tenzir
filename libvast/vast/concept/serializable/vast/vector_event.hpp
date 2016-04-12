#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_VECTOR_EVENT_HPP
#define VAST_CONCEPT_SERIALIZABLE_VAST_VECTOR_EVENT_HPP

#include <vector>

#include "vast/event.hpp"

namespace caf {
class deserializer;
class serializer;
} // namespace caf

namespace vast {

void serialize(caf::serializer& sink, std::vector<event> const& events);
void serialize(caf::deserializer& source, std::vector<event>& events);

} // namespace vast

#endif
