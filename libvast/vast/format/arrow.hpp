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

#include "vast/format/writer.hpp"
#include "vast/fwd.hpp"
#include "vast/type.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>

#include <arrow/io/api.h>
#include <arrow/ipc/writer.h>

#include <memory>
#include <vector>

namespace vast::format::arrow {

/// An Arrow writer.
class writer : public format::writer {
public:
  using output_stream_ptr = std::shared_ptr<::arrow::io::OutputStream>;

  using batch_writer_ptr = std::shared_ptr<::arrow::ipc::RecordBatchWriter>;

  writer();
  writer(writer&&) = default;
  writer& operator=(writer&&) = default;
  ~writer() override;

  caf::error write(const table_slice& x) override;

  const char* name() const override;

  void out(output_stream_ptr ptr) {
    out_ = std::move(ptr);
  }

  bool layout(const record_type& t);

private:
  caf::error write_arrow_batches(const arrow_table_slice& x);

  output_stream_ptr out_;
  record_type current_layout_;
  table_slice_builder_ptr current_builder_;
  batch_writer_ptr current_batch_writer_;
};

} // namespace vast::format::arrow
