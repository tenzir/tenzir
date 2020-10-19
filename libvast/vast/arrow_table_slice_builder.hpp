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
#include "vast/table_slice_builder.hpp"

#if VAST_HAVE_ARROW
#  include "vast/format/arrow.hpp"
#endif // VAST_HAVE_ARROW

#include <arrow/type_fwd.h>

// -- v0 includes --------------------------------------------------------------

#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

#include <memory>
#include <vector>

namespace vast {

namespace v1 {

class arrow_table_slice_builder final : public table_slice_builder {
public:
  // -- types and constants ----------------------------------------------------

#if VAST_HAVE_ARROW

  /// Wraps a type-specific Arrow builder.
  struct column_builder {
    // -- constructors, destructors, and assignment operators ------------------

    virtual ~column_builder();

    // -- pure virtual functions -----------------------------------------------

    virtual bool add(data_view x) = 0;

    [[nodiscard]] virtual std::shared_ptr<arrow::Array> finish() = 0;

    virtual std::shared_ptr<arrow::ArrayBuilder> arrow_builder() const = 0;
  };

  using column_builder_ptr = std::unique_ptr<column_builder>;

#endif // VAST_HAVE_ARROW

  // -- constructors, destructrors, and assignment operators -------------------

  /// Constructs a table slice builder from a layout.
  explicit arrow_table_slice_builder(record_type layout) noexcept;

  /// Destroys a table slice builder.
  ~arrow_table_slice_builder() noexcept override;

  // -- factory facade ---------------------------------------------------------

  /// This implementation builds Arrow-encoded table slices.
  static constexpr inline auto implementation_id = table_slice_encoding::arrow;

  // -- properties -------------------------------------------------------------

  /// @returns The current number of rows in the table slice.
  table_slice::size_type rows() const noexcept override;

  /// @returns An identifier for the implementing class.
  table_slice_encoding encoding() const noexcept override;

  /// Enables the table slice builder to allocate sufficient storage.
  /// @param num_rows The number of rows to allocate storage for.
  void reserve(table_slice::size_type num_rows) override;

private:
  // -- implementation details -------------------------------------------------

  /// Adds data to the builder.
  /// @param x the data to add.
  /// @retursn `true` on success.
  bool add_impl(data_view x) override;

  /// Constructs a table_slice from the currently accumulated state.
  /// @returns A table slice from the accumulated calls to add, or an error.
  caf::expected<chunk_ptr> finish_impl() override;

  /// Reset the builder state.
  void reset_impl() override;

#if VAST_HAVE_ARROW

  /// Current column index.
  table_slice::size_type column_ = 0;

  /// Number of filled rows.
  table_slice::size_type rows_ = 0;

  /// Builders for columnar arrays.
  std::vector<column_builder_ptr> builders_ = {};

  /// Schema of the Record Batch corresponding to the layout.
  std::shared_ptr<arrow::Schema> schema_ = nullptr;

#endif // VAST_HAVE_ARROW
};

} // namespace v1

inline namespace v0 {

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

    [[nodiscard]] virtual std::shared_ptr<arrow::Array> finish() = 0;

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

  [[nodiscard]] vast::table_slice_ptr finish() override;

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

} // namespace v0

} // namespace vast
