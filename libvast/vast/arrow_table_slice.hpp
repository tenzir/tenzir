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

// -- v1 includes --------------------------------------------------------------

#include "vast/fwd.hpp"

// -- v0 includes --------------------------------------------------------------

#include "vast/fwd.hpp"
#include "vast/table_slice.hpp"
#include "vast/view.hpp"

#include <caf/fwd.hpp>
#include <caf/intrusive_cow_ptr.hpp>

#include <arrow/api.h>

#include <memory>

namespace vast {

// -- forward-declarations -----------------------------------------------------

namespace fbs::table_slice::arrow {

struct v0;

} // namespace fbs::table_slice::arrow

/// A table slice that stores elements encoded in the
/// [Arrow](https://arrow.org) format. The implementation stores data in
/// column-major order.
class arrow_table_slice final {
public:
  // -- constructors, destructors, and assignment operators --------------------

  arrow_table_slice(const fbs::table_slice::arrow::v0& slice) noexcept;
  ~arrow_table_slice() noexcept;

  // -- table slice facade------------------------------------------------------

  table_slice::size_type rows() const noexcept;

  table_slice::size_type columns() const noexcept;

  const record_type& layout() const noexcept;

  data_view
  at(table_slice::size_type row, table_slice::size_type column) const noexcept;

  void append_column_to_index(id offset, table_slice::size_type column,
                              value_index& idx) const;

private:
  // -- implementation details -------------------------------------------------

  /// A reference to the underlying FlatBuffers table.
  const fbs::table_slice::arrow::v0& slice_;

  /// A pointer to the record batch.
  std::shared_ptr<arrow::RecordBatch> record_batch_ = nullptr;

  /// The layout of the slice.
  record_type layout_ = {};
};

} // namespace vast
