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

class passive_feather_store final : public passive_store {
  [[nodiscard]] caf::error load(chunk_ptr chunk) override {
    std::shared_ptr<arrow::Table> table = {};
    auto file = as_arrow_file(std::move(chunk));
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
