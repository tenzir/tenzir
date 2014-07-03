#ifndef VAST_EVENT_H
#define VAST_EVENT_H

#include "vast/aliases.h"
#include "vast/string.h"
#include "vast/type.h"
#include "vast/value.h"
#include "vast/util/operators.h"

namespace vast {

/// A value with a named type plus additional meta data.
class event : public record,
              util::totally_ordered<event>
{
public:
  /// Constructs an empty event.
  event();

  /// Constructs an event with a list of zero or more arguments.
  /// @param values The list of values.
  event(record values);

  /// Constructs an event with a list of zero or more arguments.
  /// @param values The list of values.
  event(std::vector<value> values);

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
  friend trial<void> print(event const& e, Iterator&& out)
  {
    if (e.type_->name().empty())
    {
      auto t = print("<unnamed>", out);
      if (! t)
        return t.error();
    }
    else
    {
      auto t = print(e.type_->name(), out);
      if (! t)
        return t.error();
    }

    // TODO: Fix laziness.
    print(" [", out);
    print(e.id_, out);
    *out++ = '|';
    print(e.timestamp_, out);
    print(']', out);
    if (! e.empty())
      print(' ', out);

    return util::print_delimited(", ", e.begin(), e.end(), out);
  }

  friend bool operator==(event const& x, event const& y);
  friend bool operator<(event const& x, event const& y);
  friend trial<void> convert(event const& e, util::json& j);
};

} // namespace vast

#endif
