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
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <caf/fwd.hpp>

#include <memory>
#include <optional>
#include <variant>

namespace vast {

/// @relates synopsis
using synopsis_ptr = std::unique_ptr<synopsis>;

/// The abstract base class for synopsis data structures.
class synopsis {
public:
  using supported_inspectors
    = std::variant<std::reference_wrapper<caf::serializer>,
                   std::reference_wrapper<caf::deserializer>>;
  // -- construction & destruction ---------------------------------------------

  /// Constructs a synopsis from a type.
  /// @param x The type the synopsis should act for.
  explicit synopsis(vast::type x);

  virtual ~synopsis() noexcept = default;

  /// Returns a copy of this synopsis.
  [[nodiscard]] virtual synopsis_ptr clone() const = 0;

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
  [[nodiscard]] const vast::type& type() const;

  // -- serialization ----------------------------------------------------------

  virtual caf::error inspect_impl(supported_inspectors& inspector) = 0;

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
  vast::type type_;
};

/// @relates synopsis
bool inspect(vast::detail::legacy_deserializer& source, synopsis_ptr& ptr);

/// Flatbuffer support.
[[nodiscard]] caf::expected<flatbuffers::Offset<fbs::synopsis::LegacySynopsis>>
pack(flatbuffers::FlatBufferBuilder& builder, const synopsis_ptr&,
     const qualified_record_field&);

[[nodiscard]] caf::error
unpack(const fbs::synopsis::LegacySynopsis&, synopsis_ptr&);

/// helper function implemented in cpp as the factory<synopsis> can't be used in
/// deserialize function below (factory must include synopsis.hpp)
synopsis_ptr make_synopsis(const type& t);

/// TODO: Serializing and deserializing a synopsis still involves conversion
/// to/from legacy types. We need to change the synopsis FlatBuffers table to
/// embed a vast.fbs.Type directly. Ideally we can make the synopsis
/// memory-mappable just like table slices and types at the same time.
/// @relates synopsis

/// Loads the contents for this synopsis from `source`.
caf::error deserialize(auto& source, synopsis_ptr& ptr) {
  // Read synopsis type.
  legacy_type t;
  if (auto err = source(t))
    return err;
  // Only nullptr has an none type.
  if (!t) {
    ptr.reset();
    return caf::none;
  }
  // Deserialize into a new instance.
  auto new_ptr = make_synopsis(type::from_legacy_type(t));
  if (!new_ptr)
    return ec::invalid_synopsis_type;
  synopsis::supported_inspectors i{std::ref(source)};
  if (auto err = new_ptr->inspect_impl(i))
    return err;
  // Change `ptr` only after successfully deserializing.
  using std::swap;
  swap(ptr, new_ptr);
  return caf::none;
}

/// Saves the contents (excluding the layout!) of this slice to `sink`.
caf::error serialize(auto& sink, synopsis_ptr& ptr) {
  if (!ptr) {
    static legacy_type dummy;
    return sink(dummy);
  }
  return caf::error::eval(
    [&] {
      return sink(ptr->type().to_legacy_type());
    },
    [&] {
      synopsis::supported_inspectors i{std::ref(sink)};
      return ptr->inspect_impl(i);
    });
}

template <class Inspector>
  requires(std::is_same_v<typename Inspector::result_type, caf::error>)
caf::error inspect(Inspector& inspector, synopsis_ptr& ptr) {
  if constexpr (Inspector::writes_state) {
    return deserialize(inspector, ptr);
  } else {
    return serialize(inspector, ptr);
  }
}

} // namespace vast
