//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice_builder.hpp>
#include <vast/chunk.hpp>
#include <vast/data.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/error.hpp>
#include <vast/fwd.hpp>
#include <vast/plugin.hpp>
#include <vast/store.hpp>

#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/feather.h>
#include <arrow/table.h>
#include <arrow/util/future.h>

namespace vast::plugins::feather {

namespace {

class chunk_random_access_file final : public arrow::io::RandomAccessFile {
public:
  explicit chunk_random_access_file(chunk_ptr chunk)
    : chunk_(std::move(chunk)) {
  }

  // TODO: all methods, check position < size
  arrow::Result<int64_t> GetSize() override {
    return detail::narrow_cast<int64_t>(chunk_->size());
  }

  arrow::Result<int64_t>
  ReadAt(int64_t position, int64_t nbytes, void* out) override {
    const auto clamped_size
      = std::min(chunk_->size() - detail::narrow_cast<size_t>(position),
                 detail::narrow_cast<size_t>(nbytes));
    const auto bytes = as_bytes(chunk_).subspan(position, clamped_size);
    std::memcpy(out, bytes.data(), bytes.size());
    return bytes.size();
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>>
  ReadAt(int64_t position, int64_t nbytes) override {
    const auto clamped_size
      = std::min(chunk_->size() - detail::narrow_cast<size_t>(position),
                 detail::narrow_cast<size_t>(nbytes));
    return as_arrow_buffer(chunk_->slice(position, clamped_size));
  }

  // /// EXPERIMENTAL: Read data asynchronously.
  arrow::Future<std::shared_ptr<arrow::Buffer>>
  ReadAsync(const arrow::io::IOContext&, int64_t position,
            int64_t nbytes) override {
    return arrow::Future<std::shared_ptr<arrow::Buffer>>::MakeFinished(
      ReadAt(position, nbytes));
  }

  arrow::Status WillNeed(const std::vector<arrow::io::ReadRange>&) override {
    return arrow::Status::OK();
  }

  arrow::Status Seek(int64_t position) override {
    position_ = position;
    return arrow::Status::OK();
  }

  arrow::Result<arrow::util::string_view> Peek(int64_t nbytes) override {
    const auto clamped_size = std::min(chunk_->size() - position_,
                                       detail::narrow_cast<size_t>(nbytes));
    const auto bytes = as_bytes(chunk_).subspan(position_, clamped_size);
    return arrow::util::string_view{reinterpret_cast<const char*>(bytes.data()),
                                    bytes.size()};
  }

  /// \brief Return true if InputStream is capable of zero copy Buffer reads
  ///
  /// Zero copy reads imply the use of Buffer-returning Read() overloads.
  bool supports_zero_copy() const override {
    return true;
  }

  /// \brief Read and return stream metadata
  ///
  /// If the stream implementation doesn't support metadata, empty metadata
  /// is returned.  Note that it is allowed to return a null pointer rather
  /// than an allocated empty metadata.
  arrow::Result<std::shared_ptr<const arrow::KeyValueMetadata>>
  ReadMetadata() override {
    return nullptr;
  }

  /// \brief Read stream metadata asynchronously
  arrow::Future<std::shared_ptr<const arrow::KeyValueMetadata>>
  ReadMetadataAsync(const arrow::io::IOContext&) override {
    return arrow::Future<
      std::shared_ptr<const arrow::KeyValueMetadata>>::MakeFinished(nullptr);
  }

  arrow::Status Close() override {
    closed_ = true;
    return arrow::Status::OK();
  }

  /// \brief Close the stream asynchronously
  ///
  /// By default, this will just submit the synchronous Close() to the
  /// default I/O thread pool. Subclasses may implement this in a more
  /// efficient manner.
  arrow::Future<> CloseAsync() override {
    // TODO: necessary?
    return arrow::Future<>::MakeFinished(Close());
  };

  /// \brief Close the stream abruptly
  ///
  /// This method does not guarantee that any pending data is flushed.
  /// It merely releases any underlying resource used by the stream for
  /// its operation.
  ///
  /// After Abort() is called, closed() returns true and the stream is not
  /// available for further operations.
  arrow::Status Abort() override {
    return Close();
  }

  /// \brief Return the position in this stream
  arrow::Result<int64_t> Tell() const override {
    return detail::narrow_cast<int64_t>(position_);
  }

  /// \brief Return whether the stream is closed
  bool closed() const override {
    return closed_;
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    return ReadAt(detail::narrow_cast<int64_t>(position_), nbytes, out);
  }

  /// \brief Read data from current file position.
  ///
  /// Read at most `nbytes` from the current file position. Less bytes may
  /// be read if EOF is reached. This method updates the current file position.
  ///
  /// In some cases (e.g. a memory-mapped file), this method may avoid a
  /// memory copy.
  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    return ReadAt(detail::narrow_cast<int64_t>(position_), nbytes);
  }

  /// EXPERIMENTAL: The IOContext associated with this file.
  ///
  /// By default, this is the same as default_io_context(), but it may be
  /// overriden by subclasses.
  const arrow::io::IOContext& io_context() const override {
    return arrow::io::default_io_context();
  }

private:
  chunk_ptr chunk_;
  size_t position_ = {};
  bool closed_ = false;
};

class passive_feather_store final : public passive_store {
  [[nodiscard]] caf::error load(chunk_ptr chunk) override {
    std::shared_ptr<arrow::Table> table = {};
    auto file = std::make_shared<chunk_random_access_file>(chunk);
    auto reader = arrow::ipc::feather::Reader::Open(file).ValueOrDie();
    auto status = reader->Read(&table);
    VAST_ASSERT(status.ok());
    for (auto rb : arrow::TableBatchReader(*table)) {
      /// TODO: layout should be computed once, as we're seeing many batches
      /// with the same
      if (!rb.ok())
        return caf::make_error(ec::system_error, rb.status().ToString());
      slices_.emplace_back(rb.MoveValueUnsafe());
    }
    return {};
  }

  [[nodiscard]] const std::vector<table_slice>& slices() const override {
    return slices_;
  }

private:
  std::vector<table_slice> slices_ = {};
};

class active_feather_store final : public active_store {
  [[nodiscard]] caf::error add(std::vector<table_slice> new_slices) override {
    slices_.reserve(new_slices.size() + slices_.size());
    slices_.insert(slices_.end(), std::make_move_iterator(new_slices.begin()),
                   std::make_move_iterator(new_slices.end()));
    return {};
  }

  [[nodiscard]] caf::error clear() override {
    slices_.clear();
    return {};
  }

  [[nodiscard]] caf::expected<chunk_ptr> finish() override {
    auto record_batches = arrow::RecordBatchVector{};
    record_batches.reserve(slices_.size());
    for (const auto& slice : slices_)
      record_batches.push_back(to_record_batch(slice));
    const auto table
      = ::arrow::Table::FromRecordBatches(record_batches).ValueOrDie();
    auto output_stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto write_properties = arrow::ipc::feather::WriteProperties::Defaults();
    // TODO: Set write_properties.chunksize to the expected batch size
    write_properties.compression = arrow::Compression::ZSTD;
    write_properties.compression_level
      = arrow::util::Codec::DefaultCompressionLevel(arrow::Compression::ZSTD)
          .ValueOrDie();
    auto status = ::arrow::ipc::feather::WriteTable(*table, output_stream.get(),
                                                    write_properties);
    VAST_ASSERT(status.ok());
    return chunk::make(output_stream->Finish().ValueOrDie());
  }

  [[nodiscard]] const std::vector<table_slice>& slices() const override {
    return slices_;
  }

private:
  std::vector<table_slice> slices_ = {};
};

class plugin final : public virtual store_plugin {
  /// Initializes a plugin with its respective entries from the YAML config
  /// file, i.e., `plugin.<NAME>`.
  /// @param config The relevant subsection of the configuration.
  [[nodiscard]] caf::error initialize([[maybe_unused]] data config) override {
    return {};
  }

  /// Returns the unique name of the plugin.
  [[nodiscard]] const char* name() const override {
    return "feather";
  }

  [[nodiscard]] caf::expected<std::unique_ptr<passive_store>>
  make_passive_store() const override {
    return std::make_unique<passive_feather_store>();
  }

  /// Create a store for the active partition.
  /// FIXME: docs
  [[nodiscard]] caf::expected<std::unique_ptr<active_store>>
  make_active_store() const override {
    return std::make_unique<active_feather_store>();
  }
};

} // namespace

} // namespace vast::plugins::feather

VAST_REGISTER_PLUGIN(vast::plugins::feather::plugin)
