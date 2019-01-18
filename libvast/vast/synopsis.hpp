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
#include <type_traits>

#include <caf/detail/type_list.hpp>
#include <caf/fwd.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/make_counted.hpp>
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

/// Constructs a synopsis for a given type. This is the default-factory
/// function. It is possible to provide a custom factory via
/// [`add_synopsis_factory`](@ref add_synopsis_factory).
/// @param x The type to construct a synopsis for.
/// @param opts Auxiliary context for constructing a synopsis.
/// @relates synopsis synopsis_factory add_synopsis_factory
/// @note The passed options *opts* may change between invocations for a given
///       type. Therefore, the type *x* should be sufficient to fully create a
///       valid synopsis instance.
synopsis_ptr make_synopsis(type x, const synopsis_options& opts = {});

/// The function to create a synopsis.
/// @relates synopsis get_synopsis_factory_fun add_synopsis_factory
using synopsis_factory = synopsis_ptr (*)(type, const synopsis_options&);

/// Registers a synopsis factory.
/// @param x The type to register a factory with.
/// @param factory The factory function to associate with *x*.
/// @pre `factory != nullptr`
/// @relates get_synopsis_factory make_synopsis
void add_synopsis_factory(type x, synopsis_factory factory);

/// Registers a concrete synopsis type.
/// @tparam Synopsis The synopsis type.
/// @param x The type to register `Synopsis` with.
template <class Synopsis, class Type>
void add_synopsis_factory() {
  static_assert(caf::detail::tl_contains<concrete_types, Type>::value,
                "Type must be a concrete vast::type");
  static auto f = [](type x, const synopsis_options& opts) -> synopsis_ptr {
    if constexpr (std::is_constructible_v<Synopsis, type,
                                          const synopsis_options&>)
      return caf::make_counted<Synopsis>(std::move(x), opts);
    else
      return caf::make_counted<Synopsis>(std::move(x));
  };
  add_synopsis_factory(type{Type{}}, f);
}

/// Retrieves a synopsis factory.
/// @param x The type to retrieve a factory for.
/// @returns The factory registered with *x* or `nullptr` if *x* doesn't map to
///          a factory.
/// @relates add_synopsis_factory make_synopsis
synopsis_factory get_synopsis_factory(const type& x);

} // namespace vast
