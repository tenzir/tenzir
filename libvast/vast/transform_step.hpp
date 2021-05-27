//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/table_slice.hpp"

#include <arrow/record_batch.h>
#include <caf/expected.hpp>

namespace vast {

/// An individual transform step. This is mainly used in the plugin API,
/// later code deals with a complete `transform`.
class transform_step {
public:
  virtual ~transform_step() = default;

  /// Apply the transformation step to the passed table slice,
  /// if neccessary converting the data to a format the step implementation can
  /// natively handle.
  [[nodiscard]] caf::expected<table_slice> apply(table_slice&&) const;
};

using transform_step_ptr = std::unique_ptr<transform_step>;

// A transform step that operates on a generic table slice.
// While the implementation is free to look at the encoding of
// the slice and dispatch to specialized functions, this overload
// requires the result of the step to be serialized back into
// a finished table slice, while the variants below can pass around
// intermediate state between the steps.
class generic_transform_step : public virtual transform_step {
public:
  [[nodiscard]] virtual caf::expected<table_slice>
  operator()(table_slice&&) const = 0;
};

// TODO: This will become useful once we implement filtering transforms,
//       where msgpack encoding allows for higher performance.
// class msgpack_transform_step : public virtual transform_step {
//   [...]
// };

class arrow_transform_step : public virtual transform_step {
public:
  /// Convenience overload that converts the table slice into arrow format and
  /// passes it to the user-defined handler.
  [[nodiscard]] caf::expected<table_slice> operator()(table_slice&&) const;

  /// Takes a record batch with a corresponding vast layout and transforms it
  /// into a new batch with a new layout.
  //  TODO: When we have implemented a way to recover the `layout` from the
  //  metadata stored in a record batch, we can drop the record type from
  //  this function signature.
  [[nodiscard]] virtual std::pair<vast::record_type,
                                  std::shared_ptr<arrow::RecordBatch>>
  operator()(vast::record_type layout,
             std::shared_ptr<arrow::RecordBatch> batch) const = 0;
};

caf::expected<transform_step_ptr>
make_transform_step(const std::string& name, const caf::settings& opts);

} // namespace vast
