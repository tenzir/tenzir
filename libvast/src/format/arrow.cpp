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

#include "vast/config.hpp"

#if VAST_ENABLE_ARROW

#  include "vast/arrow_table_slice.hpp"
#  include "vast/arrow_table_slice_builder.hpp"
#  include "vast/detail/assert.hpp"
#  include "vast/detail/byte_swap.hpp"
#  include "vast/detail/fdoutbuf.hpp"
#  include "vast/detail/string.hpp"
#  include "vast/error.hpp"
#  include "vast/format/arrow.hpp"
#  include "vast/table_slice_builder.hpp"
#  include "vast/type.hpp"

#  include <caf/none.hpp>

#  include <arrow/util/config.h>
#  include <arrow/util/io_util.h>

#  include <stdexcept>

namespace vast::format::arrow {

writer::writer() {
  out_ = std::make_shared<::arrow::io::StdoutStream>();
}

writer::writer(const caf::settings&) {
  out_ = std::make_shared<::arrow::io::StdoutStream>();
}

writer::~writer() {
  // nop
}

caf::error writer::write(const table_slice& slice) {
  if (out_ == nullptr)
    return caf::make_error(ec::filesystem_error, "invalid arrow output stream");
  if (!layout(slice.layout()))
    return caf::make_error(ec::logic_error, "failed to update layout");
  // Get the Record Batch and print it.
  auto batch = as_record_batch(slice);
  VAST_ASSERT(batch != nullptr);
  if (auto status = current_batch_writer_->WriteRecordBatch(*batch);
      !status.ok())
    return caf::make_error(ec::filesystem_error, "failed to write record batch",
                           status.ToString());
  return caf::none;
}

const char* writer::name() const {
  return "arrow-writer";
}

bool writer::layout(const record_type& x) {
  auto layout = flatten(x);
  if (current_layout_ == layout)
    return true;
  if (current_batch_writer_ != nullptr) {
    if (!current_batch_writer_->Close().ok())
      return false;
    current_batch_writer_ = nullptr;
  }
  if (layout.fields.empty())
    return true;
  current_layout_ = layout;
  auto schema = make_arrow_schema(layout);
  current_builder_ = arrow_table_slice_builder::make(layout);
#  if ARROW_VERSION_MAJOR >= 2
  auto writer_result = ::arrow::ipc::MakeStreamWriter(out_.get(), schema);
#  else
  auto writer_result = ::arrow::ipc::NewStreamWriter(out_.get(), schema);
#  endif
  if (writer_result.ok()) {
    current_batch_writer_ = std::move(*writer_result);
    return true;
  }
  return false;
}

} // namespace vast::format::arrow

#endif // VAST_ENABLE_ARROW
