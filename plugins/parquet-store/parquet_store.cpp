//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/chunk.hpp>
#include <vast/data.hpp>
#include <vast/error.hpp>
#include <vast/plugin.hpp>
#include <vast/query.hpp>
#include <vast/table_slice.hpp>

#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/table.h>
#include <arrow/type_fwd.h>
#include <arrow/util/key_value_metadata.h>
#include <caf/attach_stream_sink.hpp>
#include <caf/expected.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <parquet/arrow/writer.h>
#include <parquet/stream_reader.h>

#include <utility>

namespace vast::plugins::parquet_store {

struct store_builder_state {
  static constexpr const char* name = "active-parquet-store";
  uuid id_ = {};
  system::store_builder_actor::pointer self_ = {};

  /// Actor handle of the accountant.
  system::accountant_actor accountant_ = {};

  /// Actor handle of the filesystem.
  system::filesystem_actor fs_ = {};

  /// The path to where the store will be written.
  std::filesystem::path path_ = {};

  /// The record batches added to this partition
  arrow::RecordBatchVector record_batches_ = {};

  /// Number of events in this store.
  size_t num_rows_ = {};
};

struct store_state {
  static constexpr const char* name = "passive-parquet-store";
  uuid id_ = {};
  system::store_actor::pointer self_ = {};
};

std::filesystem::path store_path_for_partition(const uuid& partition_id) {
  auto store_filename = fmt::format("{}.parquet", partition_id);
  return std::filesystem::path{"archive"} / store_filename;
}

system::store_actor::behavior_type
store(system::store_actor::stateful_pointer<store_state> self, const uuid& id) {
  self->state.self_ = self;
  self->state.id_ = id;
  return {
    [](const query&) -> caf::result<uint64_t> {
      auto infile = arrow::io::ReadableFile::Open("rest.parquet").ValueOrDie();
      parquet::StreamReader parquet_reader{
        parquet::ParquetFileReader::Open(infile)};
      return ec::unimplemented;
    },
    [](atom::erase, const ids&) -> caf::result<uint64_t> {
      return ec::unimplemented;
    },
  };
}

auto init_parquet(caf::unit_t&) {
  // doing nothing: as we're writing the file in one pass at the end into a
  // memory buffer, there's no file opening going on here.
  VAST_TRACE("initializing stream");
}

auto add_table_slices(
  system::store_builder_actor::stateful_pointer<store_builder_state> self) {
  return [self](caf::unit_t&, std::vector<table_slice>& batch) {
    for (auto& slice : batch) {
      const auto& record_batch = to_record_batch(slice);
      self->state.num_rows_ += record_batch->num_rows();
      self->state.record_batches_.push_back(record_batch);
      const auto& schema_name
        = record_batch->schema()->metadata()->Get("VAST:name:0").ValueOrDie();
      self->state.path_
        = schema_name / store_path_for_partition(self->state.id_);
    }
    VAST_TRACE("[{}::{}] received batch of {} table slices", *self,
               self->state.id_, batch.size());
  };
}

std::shared_ptr<parquet::WriterProperties> writer_properties() {
  auto builder = parquet::WriterProperties::Builder{};
  builder.created_by("VAST telemetry engine")
    ->enable_dictionary()
    ->compression(parquet::Compression::ZSTD)
    ->compression_level(9)
    ->version(parquet::ParquetVersion::PARQUET_2_6);
  return builder.build();
}

std::shared_ptr<parquet::ArrowWriterProperties> arrow_writer_properties() {
  auto builder = parquet::ArrowWriterProperties::Builder{};
  builder.store_schema(); // serialize arrow schema into parquet meta data
  return builder.build();
}

auto write_parquet_buffer(arrow::RecordBatchVector batches) {
  // ArrowWriterProperties::support_deprecated_int96_timestamps()
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto table = arrow::Table::FromRecordBatches(batches).ValueOrDie();
  auto writer_props = writer_properties();
  auto arrow_writer_props = arrow_writer_properties();
  auto status
    = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink,
                                 1 << 24, writer_props, arrow_writer_props);
  fmt::print(stderr, "tp;write_parquet, status == {}\n", status);
  VAST_ASSERT(status.ok());
  return sink->Finish().ValueOrDie();
}

auto finish_parquet(
  system::store_builder_actor::stateful_pointer<store_builder_state> self) {
  return [self](caf::unit_t&, const caf::error&) {
    auto buffer = write_parquet_buffer(self->state.record_batches_);
    VAST_TRACE("[{}::{}] write triggered, w/ {} records in {} table slices, "
               "parquet file "
               "size: {} bytes",
               *self, self->state.id_, self->state.num_rows_,
               self->state.record_batches_.size(), buffer->size());
    auto c = chunk::make(buffer);
    self
      ->request(self->state.fs_, caf::infinite, atom::write_v,
                self->state.path_, c)
      .then(
        [self](atom::ok) {
          VAST_TRACE("flush archive ./vast.db/{}", self->state.path_);
          self->state.self_ = nullptr;
        },
        [self](caf::error& err) {
          VAST_ERROR("failed to flush archive {}", to_string(err));
          self->state.self_ = nullptr;
        });
  };
}

system::store_builder_actor::behavior_type store_builder(
  system::store_builder_actor::stateful_pointer<store_builder_state> self,
  system::accountant_actor accountant, system::filesystem_actor fs,
  const uuid& id) {
  auto path = store_path_for_partition(id);
  self->state.self_ = self;
  self->state.id_ = id;
  self->state.accountant_ = std::move(accountant);
  self->state.fs_ = std::move(fs);
  self->state.path_ = path;
  return {
    [](const query&) -> caf::result<uint64_t> {
      return ec::unimplemented;
    },
    [](atom::erase, const ids&) -> caf::result<uint64_t> {
      return ec::unimplemented;
    },
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      auto sink = caf::attach_stream_sink(
        self, in, init_parquet, add_table_slices(self), finish_parquet(self));
      return {};
    },
    [](atom::status, system::status_verbosity) -> caf::result<record> {
      return ec::unimplemented;
    },
  };
}

/// The plugin entrypoint for the aggregate transform plugin.
class plugin final : public store_plugin {
public:
  /// Initializes the aggregate plugin. This plugin has no general
  /// configuration, and is configured per instantiation as part of the
  /// transforms definition. We only check whether there's no unexpected
  /// configuration here.
  caf::error initialize(data options) override {
    if (caf::holds_alternative<caf::none_t>(options))
      return caf::none;
    if (const auto* rec = caf::get_if<record>(&options))
      if (rec->empty())
        return caf::none;
    return caf::make_error(ec::invalid_configuration, //
                           "expected empty configuration under "
                           "vast.plugins.parquet-store");
  }

  [[nodiscard]] const char* name() const override {
    return "parquet-store";
  };

  /// Create a store builder actor that accepts incoming table slices.
  /// @param accountant The actor handle of the accountant.
  /// @param fs The actor handle of a filesystem.
  /// @param id The partition id for which we want to create a store. Can
  ///           be used as a unique key by the implementation.
  /// @returns A store_builder actor and a chunk called the "header". The
  ///          contents of the header will be persisted on disk, and should
  ///          allow the plugin to retrieve the correct store actor when
  ///          `make_store()` below is called.
  [[nodiscard]] caf::expected<builder_and_header>
  make_store_builder(system::accountant_actor accountant,
                     system::filesystem_actor fs,
                     const vast::uuid& id) const override {
    auto actor_handle
      = fs.home_system().spawn(store_builder, accountant, fs, id);
    auto header = chunk::copy(id);
    return builder_and_header{actor_handle, header};
  }

  /// Create a store actor from the given header. Called when deserializing a
  /// partition that uses this partition as a store backend.
  /// @param accountant The actor handle the accountant.
  /// @param fs The actor handle of a filesystem.
  /// @param header The store header as found in the partition flatbuffer.
  /// @returns A new store actor.
  [[nodiscard]] caf::expected<system::store_actor>
  make_store(system::accountant_actor, system::filesystem_actor fs,
             std::span<const std::byte> header) const override {
    if (header.size() != uuid::num_bytes)
      return caf::make_error(ec::invalid_argument, "header must have size of "
                                                   "single uuid");
    auto id = uuid(std::span<const std::byte, uuid::num_bytes>(header.data(),
                                                               header.size()));
    return fs.home_system().spawn(store, id);
  }
};

} // namespace vast::plugins::parquet_store

// Finally, register our plugin.
VAST_REGISTER_PLUGIN(vast::plugins::parquet_store::plugin)
