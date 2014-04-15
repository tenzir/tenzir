#ifndef VAST_EVENT_H
#define VAST_EVENT_H

#include "vast/aliases.h"
#include "vast/string.h"
#include "vast/value.h"
#include "vast/fwd.h"
#include "vast/util/operators.h"
#include "vast/util/print.h"

namespace vast {

class event : public record,
              util::totally_ordered<event>,
              util::printable<event>
{
public:
  /// Constructs an empty event.
  event() = default;

  /// Constructs an event with a list of zero or more arguments.
  /// @param values The list of values.
  event(record values);

  /// Constructs an event with a list of zero or more arguments.
  /// @param args The initializer list with values.
  event(std::initializer_list<value> list);

  /// Retrieves the event ID.
  /// @returns The name of the event.
  event_id id() const;

  /// Sets the event ID.
  /// @param i The new event ID.
  void id(event_id i);

  /// Retrieves the event name.
  /// @returns The name of the event.
  string const& name() const;

  /// Sets the event name.
  /// @param str The new name of event.
  void name(string str);

  /// Retrieves the event timestamp.
  /// @returns The event timestamp.
  time_point timestamp() const;

  /// Sets the event timestamp.
  /// @param time The event timestamp.
  void timestamp(time_point time);

private:
  event_id id_ = 0;
  time_point timestamp_;
  string name_;

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
  friend void swap(event& x, event& y);
};

} // namespace vast

#endif
