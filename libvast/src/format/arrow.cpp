//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/arrow.hpp"

#include "vast/arrow_table_slice.hpp"
#include "vast/arrow_table_slice_builder.hpp"
#include "vast/chunk.hpp"
#include "vast/config.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/fdoutbuf.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/stdio.h>
#include <arrow/ipc/reader.h>
#include <caf/none.hpp>

#include <stdexcept>

namespace vast::format::arrow {

writer::writer() {
  out_ = std::make_shared<::arrow::io::StdoutStream>();
}

writer::writer(const caf::settings&) {
  out_ = std::make_shared<::arrow::io::StdoutStream>();
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

/// TODO: this is an exact copy from arrow_table_slice - better place?
template <class Consumer>
class record_batch_listener : public ::arrow::ipc::Listener {
public:
  ::arrow::Status OnEOS() override {
    fmt::print(stderr, "tp;EOS!\n");
    return ::arrow::Status::OK();
  }

  explicit record_batch_listener(Consumer&& consumer)
    : consumer_(std::forward<Consumer>(consumer)) {
    // noop
  }

private:
  ::arrow::Status OnRecordBatchDecoded(
    std::shared_ptr<::arrow::RecordBatch> record_batch) override {
    fmt::print(stderr, "tp; got record batch: {}\n", record_batch->num_rows());
    const auto slice = table_slice{record_batch};
    consumer_(slice);
    return ::arrow::Status::OK();
  }

  Consumer consumer_;
};

std::shared_ptr<::arrow::Buffer>
read_timeout(std::istream& in, int64_t chunk_size) {
  auto buffer = ::arrow::AllocateResizableBuffer(chunk_size).ValueOrDie();
  in.read(reinterpret_cast<char*>(buffer->mutable_data()), chunk_size);
  auto bytes_read = in.gcount();
  const auto shrink_to_fit = false;
  VAST_ASSERT(buffer->size() >= bytes_read);
  auto resize_status = buffer->Resize(in.gcount(), shrink_to_fit);
  VAST_ASSERT(resize_status.ok(), resize_status.ToString().c_str());
  return {std::move(buffer)};
}

reader::reader(const caf::settings& options, std::unique_ptr<std::istream> in)
  : vast::format::reader(options), input_(std::move(in)) {
}

template <class Callback>
auto make_record_batch_listener(Callback&& callback) {
  return std::make_shared<record_batch_listener<Callback>>(
    std::forward<Callback>(callback));
}

caf::error
reader::read_impl(size_t max_events, size_t max_slice_size, consumer& f) {
  const auto chunk_size = uint64_t{65536};
  VAST_TRACE_SCOPE("{} {}", VAST_ARG(max_events), VAST_ARG(max_slice_size));
  VAST_ASSERT(max_events > 0);
  // TODO: we currently ignore `max_slize_size` because we're just passing
  // through existing table slices / record batches from the producer system.
  VAST_ASSERT(max_slice_size > 0);
  auto listener = make_record_batch_listener(f);
  auto decoder = ::arrow::ipc::StreamDecoder{listener};
  size_t produced = 0;
  while (produced < max_events && !input_->eof()) {
    auto b = read_timeout(*input_, chunk_size);
    fmt::print(stderr, "tp;import::arrow: entering loop\n");
    if (const auto& status = decoder.Consume(b); !status.ok()) {
      fmt::print(stderr, "tp;error in consume: {}\n", status.ToString());
      caf::make_error(ec::format_error, "arrow decoder refused bytes");
    } else {
      fmt::print(stderr, "tp;successfully consumed {} bytes\n", b->size());
    }

    if (batch_events_ > 0 && batch_timeout_ > reader_clock::duration::zero()
        && last_batch_sent_ + batch_timeout_ < reader_clock::now()) {
      VAST_DEBUG("{} reached batch timeout", detail::pretty_type_name(this));
      fmt::print(stderr, "tp;import::arrow: batch timeout\n");
      return caf::make_error(ec::timeout, "reached batch timeout");
    }

    // input_->Peek(int64_t nbytes)
    // if (const auto batch = (*reader)->ReadNext(); batch.ok()) {
    //   fmt::print(stderr, "tp;import::arrow: got record batch\n");
    //   // When the schema changes and a new IPC message begins, we see one
    //   // record batch with status `OK` and `nullptr` as data and re-initialize
    //   // the reader.
    //   if (batch->batch != nullptr) {
    //     fmt::print(stderr, "tp;import::arrow: batch w/ {} events\n",
    //                batch->batch->num_rows());
    //     const auto slice = table_slice{batch->batch};
    //     f(slice);
    //     produced += slice.rows();
    //     ++batch_events_;
    //     last_batch_sent_ = reader_clock::now();
    //   } else {
    //     fmt::print(stderr, "tp;import::arrow: empty batch, schema change\n");
    //     reader = ::arrow::ipc::RecordBatchStreamReader::Open(input_.get());
    //     if (!reader.ok()) {
    //       return caf::make_error(
    //         ec::logic_error, fmt::format("error creating stream reader: '{}'",
    //                                      reader.status().ToString()));
    //     }
    //   }
    // } else {
    //   // Reading the next record batch yields an error if the input stream
    //   // ends. We check if it's actually just eof or an actual error.
    //   if (input_->closed()) {
    //     return caf::make_error(ec::end_of_input, "input exhausted");
    //   }
    //   return caf::make_error(
    //     ec::format_error, fmt::format("failed to read next record batch '{}'",
    //                                   batch.status().ToString()));
    // }
  }
  return caf::none;
}

void reader::reset(std::unique_ptr<std::istream> in) {
  input_ = std::move(in);
}

caf::error reader::module([[maybe_unused]] vast::module m) {
  // The VAST types are automatically generated and cannot be changed.
  return caf::make_error(ec::no_error, "schema is derived from the Arrow "
                                       "input and can't be changed");
}

module reader::module() const {
  return module_;
}

const char* reader::name() const {
  return "arrow-reader";
}

} // namespace vast::format::arrow
