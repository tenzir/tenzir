//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/fbs/synopsis.hpp"
#include "vast/legacy_type.hpp"
#include "vast/operator.hpp"
#include "vast/view.hpp"

#include <caf/fwd.hpp>

#include <memory>
#include <optional>

namespace vast {

/// @relates synopsis
using synopsis_ptr = std::unique_ptr<synopsis>;

/// The abstract base class for synopsis data structures.
class synopsis {
public:
  // -- construction & destruction ---------------------------------------------

  /// Constructs a synopsis from a type.
  /// @param x The type the synopsis should act for.
  explicit synopsis(vast::legacy_type x);

  virtual ~synopsis();

  // -- API --------------------------------------------------------------------

  /// Adds data from a table slice.
  /// @param slice The table slice to process.
  /// @pre `type_check(type(), x)`
  virtual void add(data_view x) = 0;

  /// Tests whether a predicate matches. The synopsis is implicitly the LHS of
  /// the predicate.
  /// @param op The operator of the predicate.
  /// @param rhs The RHS of the predicate.
  /// @pre: The query has already been type-checked.
  /// @returns The evaluation result of `*this op rhs`.
  [[nodiscard]] virtual std::optional<bool>
  lookup(relational_operator op, data_view rhs) const = 0;

  /// @returns A best-effort estimate of the size (in bytes) of this synopsis.
  [[nodiscard]] virtual size_t memusage() const = 0;

  /// Returns a new synopsis with the same data but consuming less memory,
  /// or `nullptr` if that is not possible.
  /// This currently only makes sense for the `buffered_address_synopsis`.
  [[nodiscard]] virtual synopsis_ptr shrink() const;

  /// Tests whether two objects are equal.
  [[nodiscard]] virtual bool equals(const synopsis& other) const noexcept = 0;

  /// @returns the type this synopsis operates for.
  [[nodiscard]] const vast::legacy_type& type() const;

  // -- serialization ----------------------------------------------------------

  /// Saves the contents (excluding the layout!) of this slice to `sink`.
  virtual caf::error serialize(caf::serializer& sink) const = 0;

  /// Loads the contents for this slice from `source`.
  virtual caf::error deserialize(caf::deserializer& source) = 0;

  /// Loads the contents for this slice from `source`.
  virtual bool deserialize(vast::detail::legacy_deserializer& source) = 0;

  /// @relates synopsis
  friend inline bool operator==(const synopsis& x, const synopsis& y) {
    return x.equals(y);
  }

  /// @relates synopsis
  friend inline bool operator!=(const synopsis& x, const synopsis& y) {
    return !(x == y);
  }

private:
  vast::legacy_type type_;
};

/// @relates synopsis
caf::error inspect(caf::serializer& sink, synopsis_ptr& ptr);

/// @relates synopsis
caf::error inspect(caf::deserializer& source, synopsis_ptr& ptr);

/// @relates synopsis
bool inspect(vast::detail::legacy_deserializer& source, synopsis_ptr& ptr);

/// Flatbuffer support.
[[nodiscard]] caf::expected<flatbuffers::Offset<fbs::synopsis::v0>>
pack(flatbuffers::FlatBufferBuilder& builder, const synopsis_ptr&,
     const qualified_record_field&);

[[nodiscard]] caf::error unpack(const fbs::synopsis::v0&, synopsis_ptr&);

} // namespace vast
