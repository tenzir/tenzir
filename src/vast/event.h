#ifndef VAST_EVENT_H
#define VAST_EVENT_H

#include "vast/aliases.h"
#include "vast/type.h"
#include "vast/value.h"
#include "vast/util/operators.h"

namespace vast {

/// A value with a named type plus additional meta data.
class event : public value, util::totally_ordered<event>
{
public:
  /// Constructs an invalid event.
  event(none = nil) {}

  /// Constructs an event.
  /// @tparam A type convertible to a value.
  /// @param x An instance of type `T`.
  /// @param t The type of the value.
  /// @post If `! t.check(*this)` then `*this = nil`.
  template <typename T>
  event(T&& x, vast::type t)
    : value{std::forward<T>(x), std::move(t)}
  {
    if (type().name().empty())
      *this = nil;
  }

  /// Sets the event ID.
  /// @param i The new event ID.
  /// @returns `true` iff *i* is in *[1, 2^64-2]*.
  bool id(event_id i);

  /// Sets the event timestamp.
  /// @param time The event timestamp.
  void timestamp(time_point time);

  /// Retrieves the event ID.
  /// @returns The name of the event.
  event_id id() const;

  /// Retrieves the event timestamp.
  /// @returns The event timestamp.
  time_point timestamp() const;

private:
  event_id id_ = invalid_event_id;
  time_point timestamp_;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(event const& e, Iterator&& out)
  {
    if (e.type().name().empty())
    {
      auto t = print("<anonymous>", out);
      if (! t)
        return t.error();
    }
    else
    {
      auto t = print(e.type().name(), out);
      if (! t)
        return t.error();
    }

    // TODO: Fix this laziness.
    print(" [", out);
    print(e.id_, out);
    *out++ = '|';
    print(e.timestamp_, out);
    print("] ", out);

    return print(static_cast<value const&>(e), out);
  }

  friend bool operator==(event const& x, event const& y);
  friend bool operator<(event const& x, event const& y);
  friend trial<void> convert(event const& e, util::json& j);
};

} // namespace vast

#endif
