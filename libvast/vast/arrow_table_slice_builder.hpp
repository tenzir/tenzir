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

#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

#include <memory>
#include <vector>

namespace arrow {

class Array;
class ArrayBuilder;
class DataType;
class MemoryPool;
class Schema;

} // namespace arrow

namespace vast {

class arrow_table_slice_builder final : public vast::table_slice_builder {
public:
  // -- member types -----------------------------------------------------------

  /// Base type.
  using super = vast::table_slice_builder;

  /// Wraps a type-specific Arrow builder.
  struct column_builder {
    // -- constructors, destructors, and assignment operators ------------------

    virtual ~column_builder();

    // -- pure virtual functions -----------------------------------------------

    virtual bool add(vast::data_view x) = 0;

    virtual std::shared_ptr<arrow::Array> finish() = 0;

    virtual std::shared_ptr<arrow::ArrayBuilder> arrow_builder() const = 0;
  };

  using column_builder_ptr = std::unique_ptr<column_builder>;

  // -- class properties -------------------------------------------------------

  /// @returns `arrow_table_slice::class_id`
  static caf::atom_value get_implementation_id() noexcept;

  // -- factory functions ------------------------------------------------------

  /// @returns a table_slice_builder instance.
  static vast::table_slice_builder_ptr make(vast::record_type layout);

  /// @returns a builder for columns of type `t`.
  static column_builder_ptr
  make_column_builder(const vast::type& t, arrow::MemoryPool* pool);

  /// @returns an arrow representation of `t`.
  static std::shared_ptr<arrow::Schema>
  make_arrow_schema(const vast::record_type& t);

  /// @returns an arrow representation of `t`.
  static std::shared_ptr<arrow::DataType> make_arrow_type(const vast::type& t);

  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs an Arrow table slice.
  /// @param layout The layout of the slice.
  explicit arrow_table_slice_builder(vast::record_type layout);

  ~arrow_table_slice_builder() override;

  // -- properties -------------------------------------------------------------

  vast::table_slice_ptr finish() override;

  size_t rows() const noexcept override;

  caf::atom_value implementation_id() const noexcept override;

protected:
  bool add_impl(vast::data_view x) override;

private:
  // -- member variables -------------------------------------------------------

  /// Current column index.
  size_t col_;

  /// Number of filled rows.
  size_t rows_;

  /// Builders for columnar arrays.
  std::vector<column_builder_ptr> builders_;
};

} // namespace vast
