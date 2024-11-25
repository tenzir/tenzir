//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/detail/legacy_deserialize.hpp"
#include "tenzir/error.hpp"
#include "tenzir/ewah_bitmap.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/legacy_type.hpp"
#include "tenzir/type.hpp"
#include "tenzir/view.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/detail/stringification_inspector.hpp>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>
#include <caf/settings.hpp>

#include <memory>
#include <variant>

namespace tenzir {

using value_index_ptr = std::unique_ptr<value_index>;

/// An index for a ::value that supports appending and looking up values.
/// @warning A lookup result does *not include* `null` values, regardless of the
/// relational operator. Include them requires performing an OR of the result
/// and an explit query for null, e.g., `x != 42 || x == null`.
class value_index {
public:
  using supported_inspectors
    = std::variant<std::reference_wrapper<caf::binary_deserializer>,
                   std::reference_wrapper<caf::binary_serializer>,
                   std::reference_wrapper<caf::detail::stringification_inspector>,
                   std::reference_wrapper<detail::legacy_deserializer>>;
  value_index(tenzir::type x, caf::settings opts);

  virtual ~value_index() noexcept = default;

  using size_type = typename ids::size_type;

  /// Appends a data value.
  /// @param x The data to append to the index.
  /// @returns `true` if appending succeeded.
  caf::expected<void> append(data_view x);

  /// Appends a data value.
  /// @param x The data to append to the index.
  /// @param pos The positional identifier of *x*.
  /// @returns `true` if appending succeeded.
  caf::expected<void> append(data_view x, id pos);

  /// Looks up data under a relational operator. If the value to look up is
  /// `null`, only `==` and `!=` are valid operations. The concrete index
  /// type determines validity of other values.
  /// @param op The relation operator.
  /// @param x The value to lookup.
  /// @returns The result of the lookup or an error upon failure.
  [[nodiscard]] caf::expected<ids>
  lookup(relational_operator op, data_view x) const;

  [[nodiscard]] size_t memusage() const;

  /// Merges another value index with this one.
  /// @param other The value index to merge.
  /// @returns `true` on success.
  // bool merge(const value_index& other);

  /// Retrieves the ID of the last append operation.
  /// @returns The largest ID in the index.
  [[nodiscard]] size_type offset() const;

  /// @returns the type of the index.
  [[nodiscard]] const tenzir::type& type() const;

  /// @returns the options of the index.
  [[nodiscard]] const caf::settings& options() const;

  // -- persistence -----------------------------------------------------------

  virtual bool inspect_impl(supported_inspectors& inspector);

  friend flatbuffers::Offset<fbs::ValueIndex>
  pack(flatbuffers::FlatBufferBuilder& builder, const value_index_ptr& value);

  friend caf::error unpack(const fbs::ValueIndex& from, value_index_ptr& to);

protected:
  [[nodiscard]] const ewah_bitmap& mask() const;
  [[nodiscard]] const ewah_bitmap& none() const;

private:
  virtual bool append_impl(data_view x, id pos) = 0;

  [[nodiscard]] virtual caf::expected<ids>
  lookup_impl(relational_operator op, data_view x) const = 0;

  [[nodiscard]] virtual size_t memusage_impl() const = 0;

  [[nodiscard]] virtual flatbuffers::Offset<fbs::ValueIndex> pack_impl(
    flatbuffers::FlatBufferBuilder& builder,
    flatbuffers::Offset<fbs::value_index::detail::ValueIndexBase> base_offset)
    = 0;

  [[nodiscard]] virtual caf::error unpack_impl(const fbs::ValueIndex& from) = 0;

  ewah_bitmap mask_;         ///< The position of all values excluding null.
  ewah_bitmap none_;         ///< The positions of null values.
  const tenzir::type type_;  ///< The type of this index.
  const caf::settings opts_; ///< Runtime context with additional parameters.
};

/// Serialize the value index into a chunk.
tenzir::chunk_ptr chunkify(const value_index_ptr& idx);

/// helper function implemented in cpp as the factory<value_index> can't be used
/// in deserialize function below (factory must include value_index.hpp)
value_index_ptr make_value_index(const type& t, caf::settings opts);

bool deserialize(auto& source, value_index_ptr& x) {
  legacy_type lt;
  if (!source.apply(lt))
    return false;
  if (is<legacy_none_type>(lt)) {
    x = nullptr;
    return true;
  }
  caf::settings opts;
  if (!source.apply(opts))
    return false;
  x = make_value_index(type::from_legacy_type(lt), std::move(opts));
  if (x == nullptr) {
    TENZIR_WARN("failed to construct value index");
    return false;
  }
  value_index::supported_inspectors i{std::ref(source)};
  return x->inspect_impl(i);
}

bool serialize(auto& sink, value_index_ptr& x) {
  auto lt = legacy_type{};
  if (x == nullptr)
    return sink.apply(lt);
  lt = x->type().to_legacy_type();
  auto err = caf::error::eval(
    [&] {
      if (!sink.apply(lt)) {
        auto err = sink.get_error();
        return err ? err
                   : caf::make_error(ec::serialization_error,
                                     "Apply for legacy type "
                                     "failed");
      }
      if (!sink.apply(x->options()) && !sink.get_error())
        return caf::make_error(ec::serialization_error,
                               "Apply for value_index_ptr options failed");
      return sink.get_error();
    },
    [&] {
      value_index::supported_inspectors i{std::ref(sink)};
      if (!x->inspect_impl(i) && !sink.get_error())
        return caf::make_error(ec::serialization_error,
                               "serialize for value_index_ptr failed");
      return sink.get_error();
    });
  if (err) {
    TENZIR_WARN("Error during value_index_ptr serialization, {}", err);
    return false;
  }
  return true;
}

bool inspect(auto& inspector, value_index& x) {
  value_index::supported_inspectors i{std::ref(inspector)};
  return x.inspect_impl(i);
}

template <class Inspector>
bool inspect(Inspector& inspector, value_index_ptr& ptr) {
  if constexpr (Inspector::is_loading) {
    return deserialize(inspector, ptr);
  } else {
    return serialize(inspector, ptr);
  }
}

} // namespace tenzir
