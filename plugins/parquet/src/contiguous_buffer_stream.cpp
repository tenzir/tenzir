//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet/contiguous_buffer_stream.hpp"

#include <tenzir/fwd.hpp>
#include <tenzir/logger.hpp>

#include <arrow/array.h>
#include <arrow/compute/cast.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <arrow/table.h>

#include <memory>

namespace tenzir {
static constexpr int64_t kBufferMinimumSize = 256;

contiguous_buffer_stream::contiguous_buffer_stream()
  : is_open_(false),
    capacity_(0),
    streaming_position_(0),
    buffer_position_(0),
    mutable_data_(nullptr),
    streaming_capacity_(0),
    data_size_(0) {
}

contiguous_buffer_stream::contiguous_buffer_stream(
  const std::shared_ptr<arrow::ResizableBuffer>& buffer)
  : buffer_(buffer),
    is_open_(true),
    capacity_(buffer->size()),
    streaming_position_(0),
    buffer_position_(0),
    mutable_data_(buffer->mutable_data()),
    streaming_capacity_(buffer->size()),
    data_size_(0) {
}

auto contiguous_buffer_stream::Create(int64_t initial_capacity,
                                      arrow::MemoryPool* pool)
  -> arrow::Result<std::shared_ptr<contiguous_buffer_stream>> {
  auto ptr
    = std::shared_ptr<contiguous_buffer_stream>(new contiguous_buffer_stream);
  RETURN_NOT_OK(ptr->Reset(initial_capacity, pool));
  return ptr;
}

auto contiguous_buffer_stream::Reset(int64_t initial_capacity,
                                     arrow::MemoryPool* pool) -> arrow::Status {
  ARROW_ASSIGN_OR_RAISE(buffer_,
                        AllocateResizableBuffer(initial_capacity, pool));
  is_open_ = true;
  capacity_ = initial_capacity;
  streaming_capacity_ = initial_capacity;
  buffer_position_ = 0;
  streaming_position_ = 0;
  mutable_data_ = buffer_->mutable_data();
  return arrow::Status::OK();
}

auto contiguous_buffer_stream::PurgeReset() -> arrow::Status {
  int64_t initial_capacity = 1024;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  ARROW_ASSIGN_OR_RAISE(buffer_,
                        AllocateResizableBuffer(initial_capacity, pool));
  capacity_ = initial_capacity;
  buffer_position_ = 0;
  mutable_data_ = buffer_->mutable_data();
  return arrow::Status::OK();
}

contiguous_buffer_stream::~contiguous_buffer_stream() {
  if (buffer_) {
    // arrow::io::internal::CloseFromDestructor(this);
    arrow::Status st = this->Close();
    //   if (!st.ok()) {
    //     auto file_type = typeid(*file).name();
    // #ifdef NDEBUG
    //     ARROW_LOG(ERROR) << "Error ignored when destroying file of type " <<
    //     file_type << ": "
    //                      << st;
    // #else
    //     std::stringstream ss;
    //     ss << "When destroying file of type " << file_type << ": " <<
    //     st.message(); ARROW_LOG(FATAL) << st.WithMessage(ss.str());
    // #endif
  }
}

auto contiguous_buffer_stream::Purge()
  -> arrow::Result<std::shared_ptr<arrow::Buffer>> {
  auto sliced_buffer_result
    = arrow::SliceBufferSafe(buffer_, 0, buffer_position_);
  return std::move(sliced_buffer_result);
}

auto contiguous_buffer_stream::Close() -> arrow::Status {
  if (is_open_) {
    is_open_ = false;
    if (buffer_position_ < capacity_) {
      RETURN_NOT_OK(buffer_->Resize(buffer_position_, false)); // should be true
    }
  }
  return arrow::Status::OK();
}

auto contiguous_buffer_stream::closed() const -> bool {
  return !is_open_;
}

auto contiguous_buffer_stream::Finish()
  -> arrow::Result<std::shared_ptr<arrow::Buffer>> {
  RETURN_NOT_OK(Close());
  buffer_->ZeroPadding();
  is_open_ = false;
  return std::move(buffer_);
}

auto contiguous_buffer_stream::Tell() const -> arrow::Result<int64_t> {
  return streaming_position_;
}

auto contiguous_buffer_stream::Write(const void* data, int64_t nbytes)
  -> arrow::Status {
  if (ARROW_PREDICT_FALSE(!is_open_)) {
    return arrow::Status::IOError("OutputStream is closed");
  }
  // arrow::ARROW_DCHECK(buffer_);
  if (ARROW_PREDICT_TRUE(nbytes > 0)) {
    if (ARROW_PREDICT_FALSE(buffer_position_ + nbytes >= capacity_)) {
      RETURN_NOT_OK(Reserve(nbytes));
    }
    if (ARROW_PREDICT_FALSE(streaming_position_ + nbytes
                            >= streaming_capacity_)) {
      RETURN_NOT_OK(Streaming_Reserve(nbytes));
    }
    memcpy(mutable_data_ + buffer_position_, data, nbytes);
    buffer_position_ += nbytes;
    streaming_position_ += nbytes;
  }

  return arrow::Status::OK();
}

auto contiguous_buffer_stream::Reserve(int64_t nbytes) -> arrow::Status {
  // Always overallocate by doubling.  It seems that it is a better growth
  // strategy, at least for memory_benchmark.cc.
  // This may be because it helps match the allocator's allocation buckets
  // more exactly.  Or perhaps it hits a sweet spot in jemalloc.
  // TENZIR_WARN("Normal reserving more space for the stream");

  int64_t new_capacity = std::max(kBufferMinimumSize, capacity_);
  while (new_capacity < buffer_position_ + nbytes) {
    new_capacity = new_capacity * 2;
  }
  if (new_capacity > capacity_) {
    RETURN_NOT_OK(buffer_->Resize(new_capacity));
    capacity_ = new_capacity;
    mutable_data_ = buffer_->mutable_data();
  }

  return arrow::Status::OK();
}

auto contiguous_buffer_stream::Streaming_Reserve(int64_t nbytes)
  -> arrow::Status {
  int64_t new_capacity = std::max(kBufferMinimumSize, streaming_capacity_);
  while (new_capacity < streaming_position_ + nbytes) {
    new_capacity = new_capacity * 2;
  }
  if (new_capacity > streaming_capacity_) {
    streaming_capacity_ = new_capacity;
  }

  return arrow::Status::OK();
}
} // namespace tenzir
