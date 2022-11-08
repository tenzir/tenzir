//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/legacy_deserialize.hpp"
#include "vast/error.hpp"
#include "vast/ewah_bitmap.hpp"
#include "vast/ids.hpp"
#include "vast/legacy_type.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>
#include <caf/settings.hpp>

#include <memory>
#include <variant>

namespace vast {

using value_index_ptr = std::unique_ptr<value_index>;

/// An index for a ::value that supports appending and looking up values.
/// @warning A lookup result does *not include* `nil` values, regardless of the
/// relational operator. Include them requires performing an OR of the result
/// and an explit query for nil, e.g., `x != 42 || x == nil`.
class value_index {
public:
  using supported_inspectors
    = std::variant<std::reference_wrapper<caf::serializer>,
                   std::reference_wrapper<caf::deserializer>>;

  value_index(vast::type x, caf::settings opts);

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
  /// `nil`, only `==` and `!=` are valid operations. The concrete index
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
  [[nodiscard]] const vast::type& type() const;

  /// @returns the options of the index.
  [[nodiscard]] const caf::settings& options() const;

  // -- persistence -----------------------------------------------------------

  virtual caf::error inspect_impl(supported_inspectors& inspector);

  virtual bool deserialize(detail::legacy_deserializer& source);

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

  ewah_bitmap mask_;         ///< The position of all values excluding nil.
  ewah_bitmap none_;         ///< The positions of nil values.
  const vast::type type_;    ///< The type of this index.
  const caf::settings opts_; ///< Runtime context with additional parameters.
};

/// @relates value_index
bool inspect(detail::legacy_deserializer& source, value_index& x);

/// @relates value_index
bool inspect(detail::legacy_deserializer& source, value_index_ptr& x);

/// Serialize the value index into a chunk.
vast::chunk_ptr chunkify(const value_index_ptr& idx);

/// helper function implemented in cpp as the factory<value_index> can't be used
/// in deserialize function below (factory must include value_index.hpp)
value_index_ptr make_value_index(const type& t, caf::settings opts);

caf::error deserialize(auto& source, value_index_ptr& x) {
  legacy_type lt;
  if (auto err = source(lt))
    return err;
  if (caf::holds_alternative<legacy_none_type>(lt)) {
    x = nullptr;
    return caf::none;
  }
  caf::settings opts;
  if (auto err = source(opts))
    return err;
  x = make_value_index(type::from_legacy_type(lt), std::move(opts));
  if (x == nullptr)
    return caf::make_error(ec::unspecified, "failed to construct value index");
  value_index::supported_inspectors i{std::ref(source)};
  return x->inspect_impl(i);
}

caf::error serialize(auto& sink, value_index_ptr& x) {
  auto lt = legacy_type{};
  if (x == nullptr)
    return sink(lt);
  lt = x->type().to_legacy_type();
  return caf::error::eval(
    [&] {
      return sink(lt, x->options());
    },
    [&] {
      value_index::supported_inspectors i{std::ref(sink)};
      return x->inspect_impl(i);
    });
}

caf::error inspect(auto& inspector, value_index& x) {
  value_index::supported_inspectors i{std::ref(inspector)};
  return x.inspect_impl(i);
}

template <class Inspector>
  requires(std::is_same_v<typename Inspector::result_type, caf::error>)
caf::error inspect(Inspector& inspector, value_index_ptr& ptr) {
  if constexpr (Inspector::writes_state) {
    return deserialize(inspector, ptr);
  } else {
    return serialize(inspector, ptr);
  }
}

} // namespace vast
