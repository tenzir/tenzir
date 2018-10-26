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

#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>

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

  /// @returns the type this synopsis operates for.
  const vast::type& type() const;

  // -- serialization ----------------------------------------------------------

  /// @returns a unique identifier for the implementing class.
  //virtual caf::atom_value implementation_id() const noexcept = 0;

  /// Saves the contents (excluding the layout!) of this slice to `sink`.
  virtual caf::error serialize(caf::serializer& sink) const = 0;

  /// Loads the contents for this slice from `source`.
  virtual caf::error deserialize(caf::deserializer& source) = 0;

private:
  vast::type type_;
};

/// @relates synopsis
caf::error inspect(caf::serializer& sink, synopsis_ptr& ptr);

/// @relates synopsis
caf::error inspect(caf::deserializer& source, synopsis_ptr& ptr);

/// Constructs a synopsis for a given type.
/// @param x The type to construct a synopsis for.
/// @relates synopsis synopsis_factory
synopsis_ptr make_synopsis(type x);

/// The function to create a synopsis.
/// @relates synopsis find_synopsis_factory
using synopsis_factory = std::function<synopsis_ptr(type)>;

/// Looks for a synopsis factory in an actor system.
/// @param sys The actor system to search for a synopsis factory function.
/// @relates synopsis synopsis_factory
synopsis_factory find_synopsis_factory(caf::actor_system& sys);

/// Looks for a synopsis factory in an actor system.
/// @param sys The actor system to search for a synopsis factory tag.
/// @relates synopsis synopsis_factory find_synopsis_factory
caf::atom_value find_synopsis_factory_tag(caf::actor_system& sys);

} // namespace vast
