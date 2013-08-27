#ifndef VAST_EVENT_H
#define VAST_EVENT_H

#include "vast/container.h"
#include "vast/string.h"
#include "vast/value.h"
#include "vast/fwd.h"
#include "vast/util/operators.h"

namespace vast {

class event : public record,
              util::totally_ordered<event>
{
public:
  /// Constructs an empty event.
  event() = default;

  /// Constructs an event with a list of zero or more arguments.
  /// @param values The list of values.
  event(std::vector<value> values);

  /// Constructs an event with a list of zero or more arguments.
  /// @param args The initializer list with values.
  event(std::initializer_list<value> args);

  /// Retrieves the event ID.
  /// @return The name of the event.
  uint64_t id() const;

  /// Sets the event ID.
  /// @param i The new event ID.
  void id(uint64_t i);

  /// Retrieves the event name.
  /// @return The name of the event.
  string const& name() const;

  /// Sets the event name.
  /// @param str The new name of event.
  void name(string str);

  /// Retrieves the event timestamp.
  /// @return The event timestamp.
  time_point timestamp() const;

  /// Sets the event timestamp.
  /// @param time The event timestamp.
  void timestamp(time_point time);

private:
  friend bool operator==(event const& x, event const& y);
  friend bool operator<(event const& x, event const& y);
  friend void swap(event& x, event& y);

  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  uint64_t id_ = 0;
  time_point timestamp_;
  string name_;
};

std::string to_string(event const& e);
std::ostream& operator<<(std::ostream& out, event const& e);

} // namespace vast

#endif
