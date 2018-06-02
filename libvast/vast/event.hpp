/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/aliases.hpp"
#include "vast/type.hpp"
#include "vast/value.hpp"
#include "vast/detail/operators.hpp"

namespace vast {

struct access;
class json;

/// A value with a named type plus additional meta data.
class event : public value, detail::totally_ordered<event> {
  friend access;

public:
  /// Type-safe factory function to construct an event from data and type.
  /// @tparam A type convertible to a value.
  /// @param x An instance of type `T`.
  /// @param t The type of the value.
  /// @returns A valid event if *t* can successfully check *x*.
  template <class T>
  static event make(T&& x, vast::type t) {
    return value::make(std::forward<T>(x), std::move(t));
  }

  /// Type-safe factory function to construct an event from data and type with
  /// an ID.
  /// @tparam A type convertible to a value.
  /// @param x An instance of type `T`.
  /// @param t The type of the value.
  /// @param i The ID of the event.
  /// @returns A valid event if *t* can successfully check *x*.
  template <class T>
  static event make(T&& x, vast::type t, vast::id i) {
    event result = value::make(std::forward<T>(x), std::move(t));
    result.id(i);
    return result;
  }

  /// Type-safe factory function to construct an event from an unchecked value.
  /// @param v The value to check and convert into an event.
  /// @returns A valid event according *v* if `v.type().check(v.data())`.
  static event make(value v) {
    return type_check(v.type(), v.data()) ? event{std::move(v)} : nil;
  }

  /// Constructs an invalid event.
  event(none = nil);

  /// Constructs an event from a value.
  event(value v);

  /// Sets the event ID.
  /// @param i The new event ID.
  /// @returns `true` iff *i* is in *[1, 2^64-2]*.
  bool id(vast::id i);

  /// Retrieves the event ID.
  /// @returns The name of the event.
  vast::id id() const;

  /// Sets the event timestamp.
  /// @param ts The event timestamp.
  void timestamp(vast::timestamp ts);

  /// Retrieves the event timestamp.
  /// @returns The event timestamp.
  vast::timestamp timestamp() const;

  friend bool operator==(const event& x, const event& y);
  friend bool operator<(const event& x, const event& y);

  template <class Inspector>
  friend auto inspect(Inspector&f, event& e) {
    return f(static_cast<value&>(e), e.id_, e.timestamp_);
  }

private:
  vast::id id_ = invalid_id;
  vast::timestamp timestamp_;
};

bool convert(const event& e, json& j);

/// Flattens an event.
/// @param e The event to flatten.
/// @returns The flattened event.
event flatten(const event& e);

} // namespace vast

