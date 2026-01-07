//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/aliases.hpp"
#include "tenzir/detail/legacy_deserialize.hpp"
#include "tenzir/fbs/synopsis.hpp"
#include "tenzir/legacy_type.hpp"
#include "tenzir/operator.hpp"
#include "tenzir/series.hpp"
#include "tenzir/type.hpp"
#include "tenzir/view.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/detail/stringification_inspector.hpp>
#include <caf/fwd.hpp>

#include <memory>
#include <optional>
#include <variant>

namespace tenzir {

/// @relates synopsis
using synopsis_ptr = std::unique_ptr<synopsis>;

/// The abstract base class for synopsis data structures.
class synopsis {
public:
  using supported_inspectors
    = std::variant<std::reference_wrapper<caf::binary_serializer>,
                   std::reference_wrapper<caf::binary_deserializer>,
                   std::reference_wrapper<caf::detail::stringification_inspector>,
                   std::reference_wrapper<detail::legacy_deserializer>>;
  // -- construction & destruction ---------------------------------------------

  /// Constructs a synopsis from a type.
  /// @param x The type the synopsis should act for.
  explicit synopsis(tenzir::type x);

  virtual ~synopsis() noexcept = default;

  /// Returns a copy of this synopsis.
  [[nodiscard]] virtual synopsis_ptr clone() const = 0;

  // -- API --------------------------------------------------------------------

  /// Adds data from a series.
  /// @param x The series to process.
  /// @pre The series type matches the synopsis type.
  virtual void add(const series& x) = 0;

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
  /// This currently only makes sense for the `buffered_ip_synopsis`.
  [[nodiscard]] virtual synopsis_ptr shrink() const;

  /// Tests whether two objects are equal.
  [[nodiscard]] virtual bool equals(const synopsis& other) const noexcept = 0;

  /// @returns the type this synopsis operates for.
  [[nodiscard]] const tenzir::type& type() const;

  // -- serialization ----------------------------------------------------------

  virtual bool inspect_impl(supported_inspectors& inspector) = 0;

  /// @relates synopsis
  friend inline bool operator==(const synopsis& x, const synopsis& y) {
    return x.equals(y);
  }

  /// @relates synopsis
  friend inline bool operator!=(const synopsis& x, const synopsis& y) {
    return ! (x == y);
  }

private:
  tenzir::type type_;
};

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
/// embed a tenzir.fbs.Type directly. Ideally we can make the synopsis
/// memory-mappable just like table slices and types at the same time.
/// @relates synopsis

/// Loads the contents for this synopsis from `source`.
bool deserialize(auto& source, synopsis_ptr& ptr) {
  // Read synopsis type
  legacy_type t;
  if (! source.apply(t)) {
    return false;
  }
  // Only nullptr has an none type.
  if (! t) {
    ptr.reset();
    return true;
  }
  // Deserialize into a new instance.
  auto new_ptr = make_synopsis(type::from_legacy_type(t));
  if (! new_ptr) {
    TENZIR_WARN("Error during synopsis deserialization {}",
                caf::make_error(ec::invalid_synopsis_type));
    return false;
  }
  synopsis::supported_inspectors i{std::ref(source)};
  if (! new_ptr->inspect_impl(i)) {
    return false;
  }
  // Change `ptr` only after successfully deserializing.
  using std::swap;
  swap(ptr, new_ptr);
  return true;
}

/// Saves the contents (excluding the schema!) of this slice to `sink`.
bool serialize(auto& sink, synopsis_ptr& ptr) {
  if (! ptr) {
    static legacy_type dummy;
    return sink.apply(dummy);
  }
  auto err = caf::error::eval(
    [&] {
      if (! sink.apply(ptr->type().to_legacy_type())
          && sink.get_error().empty()) {
        return caf::make_error(ec::serialization_error, "Apply for "
                                                        "synopsis_ptr failed");
      }
      return sink.get_error();
    },
    [&] {
      synopsis::supported_inspectors i{std::ref(sink)};
      if (! ptr->inspect_impl(i) && sink.get_error().empty()) {
        return caf::make_error(ec::serialization_error, "serialize for "
                                                        "synopsis_ptr failed");
      }
      return sink.get_error();
    });

  if (err.valid()) {
    TENZIR_WARN("Error during synopsis_ptr serialization, {}", err);
    return false;
  }
  return true;
}

template <class Inspector>
bool inspect(Inspector& inspector, synopsis_ptr& ptr) {
  if constexpr (Inspector::is_loading) {
    return deserialize(inspector, ptr);
  } else {
    return serialize(inspector, ptr);
  }
}

} // namespace tenzir
