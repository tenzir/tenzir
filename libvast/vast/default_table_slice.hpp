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

#include "vast/table.hpp"

namespace vast {

// TODO: move to vast/fwd.hpp after merge
class default_table_slice;

/// @relates default_table_slice
using default_table_slice_ptr = caf::intrusive_ptr<default_table_slice>;

namespace detail {

class default_table_slice_builder : public table_slice_builder {
public:
  default_table_slice_builder(record_type layout);

  bool append(data x);

  bool add(data_view x) final;

  table_slice_ptr finish() final;

private:
  record_type layout_;
  vector row_;
  size_t col_;
  default_table_slice_ptr slice_;
};

} // namespace detail

/// The default implementation of `table_slice`.
class default_table_slice : public table_slice {
  friend detail::default_table_slice_builder;

public:
  /// Constructs a builder that generates a default_table_slice.
  /// @param layout The layout of the table_slice.
  /// @returns The builder instance.
  static table_slice_builder_ptr make_builder(record_type layout);

  default_table_slice(record_type layout);

  virtual caf::optional<data_view> at(size_type row, size_type col) const final;

private:
  std::vector<data> xs_;
};

} // namespace vast
