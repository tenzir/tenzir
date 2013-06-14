#ifndef VAST_OBJECT_H
#define VAST_OBJECT_H

#include "vast/uuid.h"
#include "vast/io/fwd.h"

namespace vast {

/// An object with a unique ID.
class object
{
public:
  /// Constructs an object with a random ID.
  /// @param id The initial UUID of the object.
  object(uuid id = uuid::random());

  /// Retrieves the object's ID.
  /// @return A unique ID describing the object.
  uuid const& id() const;

  /// Sets the object's ID.
  /// @param id A unique ID describing the object.
  void id(uuid id);

  /// Sets the object's ID.
  /// @param id A UUID string.
  void id(std::string const& str);

private:
  friend io::access;
  void serialize(io::serializer& sink);
  void deserialize(io::deserializer& source);
  friend void swap(object& x, object& y);

  uuid id_;
};

} // namespace vast

#endif
