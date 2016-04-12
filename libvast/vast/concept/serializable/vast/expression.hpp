#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_EXPRESSION_HPP
#define VAST_CONCEPT_SERIALIZABLE_VAST_EXPRESSION_HPP

#include "vast/expression.hpp"

namespace caf {
class serializer;
class deserializer;
} // namespace caf

namespace vast {

void serialize(caf::serializer& sink, predicate const& p);
void serialize(caf::deserializer& source, predicate& p);

void serialize(caf::serializer& sink, expression const& expr);
void serialize(caf::deserializer& source, expression& expr);

} // namespace vast

#endif
