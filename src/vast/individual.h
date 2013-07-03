#ifndef VAST_INDIVIDUAL_H
#define VAST_INDIVIDUAL_H

#include "vast/uuid.h"
#include "vast/fwd.h"

namespace vast {

/// An object with a unique ID.
class individual
{
public:
  /// Constructs an object with a random ID.
  /// @param id The instance's UUID.
  individual(uuid id = uuid::random());

  /// Retrieves the individual's ID.
  /// @return A UUID describing the instance.
  uuid const& id() const;

  /// Sets the individual's ID.
  /// @param id A UUID describing the instance.
  void id(uuid id);

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);
  friend void swap(individual& x, individual& y);

  uuid id_;
};

} // namespace vast

#endif
