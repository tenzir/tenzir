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

#include <utility>

#include <caf/fwd.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>

#include "vast/aliases.hpp"
#include "vast/fwd.hpp"
#include "vast/operator.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

namespace vast {

/// @relates synopsis
using synopsis_ptr = caf::intrusive_ptr<synopsis>;

/// The abstract base class for synopsis data structures.
class synopsis : public caf::ref_counted {
public:
  // -- construction & destruction ---------------------------------------------

  /// Constructs a synopsis from a type.
  /// @param x The type the synopsis should act for.
  explicit synopsis(vast::type x);

  virtual ~synopsis();

  // -- API --------------------------------------------------------------------

  /// Adds data from a table slice.
  /// @param slice The table slice to process.
  virtual void add(data_view x) = 0;

  /// Tests whether a predicate matches. The synopsis is implicitly the LHS of
  /// the predicate.
  /// @param op The operator of the predicate.
  /// @param rhs The RHS of the predicate.
  /// @returns The evaluation result of `*this op rhs`.
  virtual bool lookup(relational_operator op, data_view rhs) const = 0;

  /// Tests whether two objects are equal.
  virtual bool equals(const synopsis& other) const noexcept = 0;

  /// @returns the type this synopsis operates for.
  const vast::type& type() const;

  // -- serialization ----------------------------------------------------------

  /// @returns a unique identifier for the factory required to make instances
  ///          of this synopsis.
  virtual caf::atom_value factory_id() const noexcept;

  /// Saves the contents (excluding the layout!) of this slice to `sink`.
  virtual caf::error serialize(caf::serializer& sink) const = 0;

  /// Loads the contents for this slice from `source`.
  virtual caf::error deserialize(caf::deserializer& source) = 0;

private:
  vast::type type_;
};

/// @relates synopsis
inline bool operator==(const synopsis& x, const synopsis& y) {
  return x.equals(y);
}

/// @relates synopsis
inline bool operator!=(const synopsis& x, const synopsis& y) {
  return !(x == y);
}

/// @relates synopsis
caf::error inspect(caf::serializer& sink, synopsis_ptr& ptr);

/// @relates synopsis
caf::error inspect(caf::deserializer& source, synopsis_ptr& ptr);

/// Additional runtime information to pass to the synopsis factory.
/// @relates synopsis
using synopsis_options = caf::dictionary<caf::config_value>;

/// Constructs a synopsis for a given type. This is the default-factory
/// function. It is possible to provide a custom factory via
/// [`set_synopsis_factory`](@ref set_synopsis_factory).
/// @param x The type to construct a synopsis for.
/// @param opts Auxiliary context for constructing a synopsis.
/// @relates synopsis synopsis_factory set_synopsis_factory
/// @note The passed options *opts* may change between invocations for a given
///       type. Therefore, the type *x* should be sufficient to fully create a
///       valid synopsis instance.
synopsis_ptr make_synopsis(type x, const synopsis_options& opts = {});

/// The function to create a synopsis.
/// @relates synopsis get_synopsis_factory_fun set_synopsis_factory
using synopsis_factory = synopsis_ptr (*)(type, const synopsis_options&);

/// Deserializes a factory identifier and returns the corresponding factory
/// function if has been registered via set_synopsis_factory previously. For
/// the default identifier, the function returns [`make_synopsis`](@ref
/// make_synopsis).
/// @param source The deserializer to read from.
/// @returns A pair *(id, factory)* where *id* is the atom identifying
///          *factory*.
/// @relates synopsis synopsis_factory
expected<std::pair<caf::atom_value, synopsis_factory>>
deserialize_synopsis_factory(caf::deserializer& source);

/// Registers a synopsis factory in an actor system runtime settings map.
/// @param sys The actor system in which to register the factory.
/// @param id The factory identifier.
/// @param factory The factory function to associate with *id*.
/// @relates deserialize_synopsis_factory get_synopsis_factory
void set_synopsis_factory(caf::actor_system& sys, caf::atom_value id,
                          synopsis_factory factory);

/// Retrieves a synopsis factory from an actor system runtime settings map.
/// @param sys The actor system in which to look for a factory.
/// @returns A pair of factory identifier and factory function or `nullptr` if
///          there exists no factory in the system.
/// @relates set_synopsis_factory
caf::expected<std::pair<caf::atom_value, synopsis_factory>>
get_synopsis_factory(caf::actor_system& sys);

} // namespace vast
