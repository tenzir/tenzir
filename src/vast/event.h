#ifndef VAST_EVENT_H
#define VAST_EVENT_H

#include "vast/aliases.h"
#include "vast/string.h"
#include "vast/type.h"
#include "vast/value.h"
#include "vast/util/operators.h"
#include "vast/util/print.h"

namespace vast {

/// A value with a named type plus additional meta data.
class event : public record,
              util::totally_ordered<event>,
              util::printable<event>
{
public:
  /// Constructs an empty event.
  event();

  /// Constructs an event with a list of zero or more arguments.
  /// @param values The list of values.
  event(record values);

  /// Constructs an event with a list of zero or more arguments.
  /// @param args The initializer list with values.
  event(std::initializer_list<value> list);

  /// Sets the event ID.
  /// @param i The new event ID.
  void id(event_id i);

  /// Sets the event type.
  /// @param t The event type.
  /// @pre `t != nullptr`
  void type(type_const_ptr t);

  /// Sets the event timestamp.
  /// @param time The event timestamp.
  void timestamp(time_point time);

  /// Retrieves the event ID.
  /// @returns The name of the event.
  event_id id() const;

  /// Retrieves the event timestamp.
  /// @returns The event timestamp.
  time_point timestamp() const;

  /// Retrieves the event type.
  /// @returns The type of the event.
  type_const_ptr const& type() const;

  /// Retrieves the event name. Shorthand for `type()->name()`.
  /// @returns The name of the event.
  string const& name() const;

private:
  event_id id_ = 0;
  time_point timestamp_;
  type_const_ptr type_;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    if (name().empty())
    {
      if (! render(out, "<unnamed>"))
        return false;
    }
    else
    {
      if (! render(out, name()))
        return false;
    }

    render(out, " [");
    render(out, id());
    *out++ = '|';
    render(out, timestamp());
    render(out, "] ");

    auto first = begin();
    auto last = end();
    while (first != last)
    {
      if (! render(out, *first))
        return false;
      if (++first != last)
        if (! render(out, ", "))
          return false;
    }

    return true;
  }

  friend bool operator==(event const& x, event const& y);
  friend bool operator<(event const& x, event const& y);
};

} // namespace vast

#endif
