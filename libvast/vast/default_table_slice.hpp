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

/// The default implementation of `table_slice`.
class default_table_slice : public table_slice {
public:
  default_table_slice(record_type layout);

  /// Enables incremental construction of a table slice.
  class builder;

  /// @relates builder
  using builder_ptr = caf::intrusive_ptr<builder>;

  virtual caf::optional<data_view> at(size_type row, size_type col) const final;

private:
  std::vector<data> xs_;
};

/// @relates default_table_slice
using default_table_slice_ptr = caf::intrusive_ptr<default_table_slice>;

class default_table_slice::builder : public table_slice::builder {
public:
  /// Factory function to construct a builder.
  /// @param layout The layout of the builder.
  static builder_ptr make(record_type layout);

  builder(record_type layout);

  bool add(data&& x);

  bool add(const data& x) final;

  table_slice_ptr finish() final;

private:
  record_type layout_;
  vector row_;
  size_t col_;
  default_table_slice_ptr slice_;
};

} // namespace vast
