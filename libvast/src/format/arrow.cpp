//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/arrow.hpp"

#include "vast/arrow_table_slice.hpp"
#include "vast/config.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/fdoutbuf.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/experimental_table_slice_builder.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"

#include <arrow/api.h>
#include <arrow/io/stdio.h>
#include <caf/none.hpp>

#include <stdexcept>

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
    return caf::make_error(ec::logic_error, "invalid arrow output stream");
  auto batch = to_record_batch(slice);
  if (const auto& layout = slice.layout(); current_layout_ != layout) {
    if (!this->layout(batch->schema()))
      return caf::make_error(ec::logic_error, "failed to update layout");
    current_layout_ = layout;
  }
  VAST_ASSERT(batch != nullptr);
  if (auto status = current_batch_writer_->WriteRecordBatch(*batch);
      !status.ok())
    return caf::make_error(ec::unspecified, "failed to write record batch",
                           status.ToString());
  return caf::none;
}

const char* writer::name() const {
  return "arrow-writer";
}

bool writer::layout(const std::shared_ptr<::arrow::Schema>& schema) {
  if (current_batch_writer_ != nullptr) {
    if (!current_batch_writer_->Close().ok())
      return false;
    current_batch_writer_ = nullptr;
  }
  auto writer_result = ::arrow::ipc::MakeStreamWriter(out_.get(), schema);
  if (writer_result.ok()) {
    current_batch_writer_ = std::move(*writer_result);
    return true;
  }
  return false;
}

} // namespace vast::format::arrow
