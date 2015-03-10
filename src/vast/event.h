#ifndef VAST_EVENT_H
#define VAST_EVENT_H

#include "vast/aliases.h"
#include "vast/type.h"
#include "vast/value.h"
#include "vast/util/operators.h"

namespace vast {

struct access;

/// A value with a named type plus additional meta data.
class event : public value, util::totally_ordered<event>
{
  friend access;

public:
  /// Type-safe factory function to construct an event from data and type.
  /// @tparam A type convertible to a value.
  /// @param x An instance of type `T`.
  /// @param t The type of the value.
  /// @returns A valid event if *t* can successfully check *x*.
  template <typename T>
  static event make(T&& x, vast::type t)
  {
    return value::make(std::forward<T>(x), std::move(t));
  }

  /// Type-safe factory function to construct an event from an unchecked value.
  /// @param v The value to check and convert into an event.
  /// @returns A valid event according *v* if `v.type().check(v.data())`.
  static event make(value v)
  {
    return v.type().check(v.data()) ? event{std::move(v)} : nil;
  }

  /// Constructs an invalid event.
  event(none = nil);

  /// Constructs an event from a value.
  event(value v);

  friend bool operator==(event const& x, event const& y);
  friend bool operator<(event const& x, event const& y);

  /// Sets the event ID.
  /// @param i The new event ID.
  /// @returns `true` iff *i* is in *[1, 2^64-2]*.
  bool id(event_id i);

  /// Sets the event timestamp.
  /// @param time The event timestamp.
  void timestamp(time::point time);

  /// Retrieves the event ID.
  /// @returns The name of the event.
  event_id id() const;

  /// Retrieves the event timestamp.
  /// @returns The event timestamp.
  time::point timestamp() const;

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

  friend trial<void> convert(event const& e, util::json& j);

private:
  event_id id_ = invalid_event_id;
  time::point timestamp_;
};

} // namespace vast

#endif
