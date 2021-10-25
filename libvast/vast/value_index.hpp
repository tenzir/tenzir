//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/error.hpp"
#include "vast/ewah_bitmap.hpp"
#include "vast/ids.hpp"
#include "vast/legacy_type.hpp"
#include "vast/view.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>
#include <caf/settings.hpp>

#include <memory>

namespace vast {

using value_index_ptr = std::unique_ptr<value_index>;

/// An index for a ::value that supports appending and looking up values.
/// @warning A lookup result does *not include* `nil` values, regardless of the
/// relational operator. Include them requires performing an OR of the result
/// and an explit query for nil, e.g., `x != 42 || x == nil`.
class value_index {
public:
  value_index(vast::legacy_type x, caf::settings opts);

  virtual ~value_index();

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
  [[nodiscard]] const vast::legacy_type& type() const;

  /// @returns the options of the index.
  [[nodiscard]] const caf::settings& options() const;

  // -- persistence -----------------------------------------------------------

  virtual caf::error serialize(caf::serializer& sink) const;

  virtual caf::error deserialize(caf::deserializer& source);

protected:
  [[nodiscard]] const ewah_bitmap& mask() const;
  [[nodiscard]] const ewah_bitmap& none() const;

private:
  virtual bool append_impl(data_view x, id pos) = 0;

  [[nodiscard]] virtual caf::expected<ids>
  lookup_impl(relational_operator op, data_view x) const = 0;

  [[nodiscard]] virtual size_t memusage_impl() const = 0;

  ewah_bitmap mask_;         ///< The position of all values excluding nil.
  ewah_bitmap none_;         ///< The positions of nil values.
  const vast::legacy_type type_; ///< The type of this index.
  const caf::settings opts_; ///< Runtime context with additional parameters.
};

/// @relates value_index
caf::error inspect(caf::serializer& sink, const value_index& x);

/// @relates value_index
caf::error inspect(caf::deserializer& source, value_index& x);

/// @relates value_index
caf::error inspect(caf::serializer& sink, const value_index_ptr& x);

/// @relates value_index
caf::error inspect(caf::deserializer& source, value_index_ptr& x);

/// Serialize the value index into a chunk.
vast::chunk_ptr chunkify(const value_index_ptr& idx);

} // namespace vast
