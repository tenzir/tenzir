//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice_builder.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/data.hpp>
#include <vast/detail/base64.hpp>
#include <vast/plugin.hpp>
#include <vast/query.hpp>
#include <vast/system/report.hpp>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <caf/actor_system_config.hpp>
#include <caf/attach_stream_sink.hpp>
#include <caf/expected.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

namespace vast::plugins::parquet {

/// Configuration for the Parquet plugin.
struct configuration {
  uint64_t row_group_size{::parquet::DEFAULT_MAX_ROW_GROUP_LENGTH};
  int64_t zstd_compression_level{9};

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f(x.row_group_size, x.zstd_compression_level);
  }

  static const record_type& layout() noexcept {
    static auto result = record_type{
      {"row-group-size", count_type{}},
      {"zstd-compression-level", integer_type{}},
    };
    return result;
  }
};

struct store_builder_state {
  static constexpr const char* name = "active-parquet-store";
  uuid id_ = {};
  system::store_builder_actor::pointer self_ = {};

  /// Actor handle of the accountant.
  system::accountant_actor accountant_ = {};

  /// Actor handle of the filesystem.
  system::filesystem_actor fs_ = {};

  /// Destination parquet file name.
  std::filesystem::path path_ = {};

  /// The table slices added to this partition.
  std::vector<table_slice> table_slices_ = {};

  /// The layout of the first record batch.
  std::optional<type> vast_type_;

  /// Number of events in this store.
  size_t num_rows_ = {};

  /// Plugin configuration.
  configuration config_;
};

struct store_state {
  static constexpr const char* name = "passive-parquet-store";
  uuid id_ = {};
  system::store_actor::pointer self_ = {};

  /// Source parquet file name.
  std::filesystem::path path_ = {};

  /// Actor handle of the accountant.
  system::accountant_actor accountant_ = {};

  /// Actor handle of the filesystem.
  system::filesystem_actor fs_ = {};

  /// The table slices added to this partition.
  /// Empty before data is retrieved from the file system.
  std::optional<std::vector<table_slice>> table_slices_ = {};

  /// Number of events in this store.
  size_t num_rows_ = {};

  /// Holds requests that did arrive while the parquet dat,a
  /// was still being loaded from disk.
  using request
    = std::tuple<vast::query, caf::typed_response_promise<uint64_t>>;
  std::vector<request> deferred_requests_ = {};

  /// Plugin configuration.
  configuration config_;
};

system::store_builder_actor::behavior_type store_builder(
  system::store_builder_actor::stateful_pointer<store_builder_state> self,
  system::accountant_actor accountant, system::filesystem_actor fs,
  const uuid& id, const configuration& config);

system::store_actor::behavior_type
store(system::store_actor::stateful_pointer<store_state> self,
      const system::accountant_actor& accountant,
      const system::filesystem_actor& fs, const uuid& id,
      const configuration& config);

/// The plugin entrypoint for the parquet store plugin.
class plugin final : public store_actor_plugin {
public:
  /// Initializes the parquet plugin. This plugin has no general configuration.
  /// @param options the plugin options. Must be empty.
  caf::error initialize(data options) override {
    if (caf::holds_alternative<caf::none_t>(options))
      return caf::none;
    return convert(options, config_);
  }

  [[nodiscard]] const char* name() const override {
    return "parquet";
  }

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
      = fs.home_system().spawn(store_builder, accountant, fs, id, config_);
    auto header = chunk::copy(id);
    return builder_and_header{actor_handle, header};
  }

  /// Create a store actor from the given header. Called when deserializing a
  /// partition that uses parquet as a store backend.
  /// @param accountant The actor handle the accountant.
  /// @param fs The actor handle of a filesystem.
  /// @param header The store header as found in the partition flatbuffer.
  /// @returns A new store actor.
  [[nodiscard]] caf::expected<system::store_actor>
  make_store(system::accountant_actor accountant, system::filesystem_actor fs,
             std::span<const std::byte> header) const override {
    if (header.size() != uuid::num_bytes)
      return caf::make_error(ec::invalid_argument, "header must have size of "
                                                   "single uuid");
    auto id = uuid(std::span<const std::byte, uuid::num_bytes>(header.data(),
                                                               header.size()));
    return fs.home_system().spawn(store, accountant, fs, id, config_);
  }

private:
  configuration config_;
};
namespace {

std::filesystem::path store_path_for_partition(const uuid& partition_id) {
  auto store_filename = fmt::format("{}.parquet", partition_id);
  return std::filesystem::path{"archive"} / store_filename;
}

/// Handles an array containing enum data, transforming the data into
/// vast.enumeration extension type backed by a dictionary.
std::shared_ptr<arrow::Array>
fix_enum_array(const enumeration_type& et,
               const std::shared_ptr<arrow::Array>& arr) {
  switch (arr->type_id()) {
    case arrow::Type::DICTIONARY: {
      const auto dict = static_pointer_cast<arrow::DictionaryArray>(arr);
      auto indices = arrow::compute::Cast(*dict->indices(), arrow::uint8());
      return enumeration_type::array_type::make(
               et.to_arrow_type(), std::static_pointer_cast<arrow::UInt8Array>(
                                     indices.ValueOrDie()))
        .MoveValueUnsafe();
    }
    case arrow::Type::STRING: {
      auto values = static_pointer_cast<arrow::StringArray>(arr);
      auto builder = enumeration_type::builder_type(et.to_arrow_type());
      if (!builder.Reserve(values->length()).ok())
        die("failed to reserve builder capacity for dict indices");
      for (auto v : *values) {
        if (v) {
          if (!builder.Append(*et.resolve(v->to_string())).ok())
            die("unable to append dict value");
        } else {
          if (!builder.AppendNull().ok())
            die("unable to append null to dict indices");
        }
      }
      return builder.Finish().ValueOrDie();
    }
    default: {
      die(fmt::format("unhandled enum-parquet variation for array type '{}'",
                      arr->type()->ToString()));
    }
  }
}

/// Transform a chunked array by applying a mapping function `Mapper` over each
/// chunk and constructs a new array from the transformed chunks.
template <class Mapper, class VastType>
std::shared_ptr<arrow::ChunkedArray>
map_chunked_array(const VastType& t,
                  const std::shared_ptr<arrow::ChunkedArray>& arr, Mapper m) {
  auto chunks = arrow::ArrayVector{};
  chunks.reserve(arr->num_chunks());
  for (const auto& chunk : arr->chunks()) {
    // short-circuit: if the first transform is a no-op, skip remaining chunks
    if (std::shared_ptr<arrow::Array> c = m(t, chunk))
      chunks.push_back(std::move(c));
    else
      return arr;
  }
  auto result = arrow::ChunkedArray::Make(chunks);
  return result.ValueOrDie();
}

/// Transform an array into its canonical form for the provided vast type.
/// the Arrow parquet reader does not fully restore the schema used during
/// write. In particular, it doesn't handle extension types in map keys and
/// values, as well as dictionary indices, which are used in vast enumerations.
std::shared_ptr<arrow::Array>
align_array_to_type(const vast::type& t, std::shared_ptr<arrow::Array> array) {
  auto f = detail::overload{
    [&](const enumeration_type& et) -> std::shared_ptr<arrow::Array> {
      return fix_enum_array(et, array);
    },
    [&](const pattern_type&) -> std::shared_ptr<arrow::Array> {
      if (pattern_type::to_arrow_type()->Equals(array->type()))
        return {};
      return std::make_shared<pattern_type::array_type>(
        pattern_type::to_arrow_type(), array);
    },
    [&](const address_type&) -> std::shared_ptr<arrow::Array> {
      if (address_type::to_arrow_type()->Equals(array->type()))
        return {}; // address is not always wrong, only when inside maps
      return std::make_shared<address_type::array_type>(
        address_type::to_arrow_type(), array);
    },
    [&](const subnet_type&) -> std::shared_ptr<arrow::Array> {
      if (subnet_type::to_arrow_type()->Equals(array->type()))
        return {};
      auto sa = std::static_pointer_cast<arrow::StructArray>(array);
      auto address_array = std::make_shared<address_type::array_type>(
        address_type::to_arrow_type(), sa->field(0));
      auto inner_type = subnet_type::to_arrow_type()->storage_type();
      auto children = std::vector<std::shared_ptr<arrow::Array>>{
        address_array,
        sa->field(1),
      };
      auto struct_array
        = std::make_shared<arrow::StructArray>(inner_type, sa->length(),
                                               children, sa->null_bitmap(),
                                               sa->null_count());
      return std::make_shared<subnet_type::array_type>(
        subnet_type::to_arrow_type(), struct_array);
    },
    [&](const list_type& lt) -> std::shared_ptr<arrow::Array> {
      auto list_array = std::static_pointer_cast<arrow::ListArray>(array);
      if (auto fixed_array
          = align_array_to_type(lt.value_type(), list_array->values()))
        return std::make_shared<arrow::ListArray>(
          lt.to_arrow_type(), list_array->length(), list_array->value_offsets(),
          fixed_array, list_array->null_bitmap(), list_array->null_count());
      return {};
    },
    [&](const map_type& mt) -> std::shared_ptr<arrow::Array> {
      auto ma = std::static_pointer_cast<arrow::MapArray>(array);
      auto key_array = align_array_to_type(mt.key_type(), ma->keys());
      auto val_array = align_array_to_type(mt.value_type(), ma->items());
      if (!key_array && !val_array)
        return {};
      auto ka = key_array ? key_array : ma->keys();
      auto va = val_array ? val_array : ma->items();
      return std::make_shared<arrow::MapArray>(mt.to_arrow_type(), ma->length(),
                                               ma->value_offsets(), ka, va,
                                               ma->null_bitmap(),
                                               ma->null_count());
    },
    [&](const record_type& rt) -> std::shared_ptr<arrow::Array> {
      auto struct_array = std::static_pointer_cast<arrow::StructArray>(array);
      auto it = struct_array->fields().begin();
      auto children = std::vector<std::shared_ptr<arrow::Array>>{};
      children.reserve(rt.num_fields());
      auto modified = false;
      for (const auto& field : rt.fields()) {
        auto mapped_arr = align_array_to_type(field.type, *it);
        if (mapped_arr) {
          modified = true;
          children.push_back(mapped_arr);
        } else {
          children.push_back(*it);
        }
        ++it;
      }
      if (!modified)
        return {};
      return std::make_shared<arrow::StructArray>(
        rt.to_arrow_type(), struct_array->length(), children,
        struct_array->null_bitmap(), struct_array->null_count());
    },
    [&](const auto&) -> std::shared_ptr<arrow::Array> {
      VAST_ASSERT(t.to_arrow_type()->Equals(array->type()));
      return {};
    },
  };
  return caf::visit(f, t);
}

/// Transform a given `ChunkedArray` according to the provided VAST type
/// `ChunkedArray`s only occur at the outermost level, and the VAST type
/// that is not properly represented at this level is `enumeration_type`.
std::shared_ptr<arrow::ChunkedArray>
restore_enum_chunk_array(const vast::type& t,
                         std::shared_ptr<arrow::ChunkedArray> array) {
  auto f = detail::overload{
    [&](const enumeration_type& et) -> std::shared_ptr<arrow::ChunkedArray> {
      return map_chunked_array(et, array, fix_enum_array);
    },
    [&](const list_type&) -> std::shared_ptr<arrow::ChunkedArray> {
      return map_chunked_array(t, array, align_array_to_type);
    },
    [&](const map_type&) -> std::shared_ptr<arrow::ChunkedArray> {
      return map_chunked_array(t, array, align_array_to_type);
    },
    [&](const record_type&) -> std::shared_ptr<arrow::ChunkedArray> {
      return map_chunked_array(t, array, align_array_to_type);
    },
    [&](const auto&) -> std::shared_ptr<arrow::ChunkedArray> {
      VAST_ASSERT(t.to_arrow_type()->Equals(array->type()));
      return {};
    },
  };
  return caf::visit(f, t);
}

/// Transform the table such that it adheres to the given arrow schema
/// This is a work around for the lack of support for our extension types in the
/// arrow parquet reader.
std::shared_ptr<arrow::Table>
align_table_to_schema(const std::shared_ptr<arrow::Schema>& target_schema,
                      const std::shared_ptr<arrow::Table>& table) {
  const auto start = std::chrono::steady_clock::now();
  auto arrays = arrow::ChunkedArrayVector{};
  auto rt = caf::get<record_type>(type::from_arrow(*target_schema));
  for (int i = 0; i < table->num_columns(); ++i) {
    if (auto new_arr
        = restore_enum_chunk_array(rt.field(i).type, table->column(i)))
      arrays.push_back(new_arr);
    else
      arrays.push_back(table->column(i));
  }
  auto new_table = arrow::Table::Make(target_schema, arrays, table->num_rows());
  const auto delta = std::chrono::steady_clock::now() - start;
  VAST_DEBUG("table schema aligned in {}[ns]",
             data{std::chrono::duration_cast<duration>(delta)});
  return new_table;
}

auto derive_import_time(const std::shared_ptr<arrow::Array>& time_col) {
  auto min_max_time = arrow::compute::MinMax(time_col).ValueOrDie();
  auto max_value = min_max_time.scalar_as<arrow::StructScalar>().value[1];
  auto f = detail::overload{
    [&](const arrow::TimestampScalar& ts) -> vast::time {
      return time{duration{ts.value}};
    },
    [&](const arrow::Scalar& scalar) -> vast::time {
      die(fmt::format("import_time is not a time column, got {} instead",
                      scalar.type->ToString()));
      return time{duration{0}};
    },
  };
  return caf::visit(f, *max_value);
} // namespace vast::plugins::parquet

/// Extract event column from record batch and transform into new record batch.
/// The record batch contains a message envelope with the actual event data
/// alongside VAST-related meta data (currently limited to the import time).
/// Message envelope is unwrapped and the metadata, attached to the to-level
/// schema the input record batch is copied to the newly created record batch.
std::shared_ptr<arrow::RecordBatch>
prepare_record_batch(const std::shared_ptr<arrow::RecordBatch>& rb) {
  auto event_col = rb->GetColumnByName("event");
  auto schema_metadata = rb->schema()->GetFieldByName("event")->metadata();
  auto event_rb = arrow::RecordBatch::FromStructArray(event_col).ValueOrDie();
  return event_rb->ReplaceSchemaMetadata(schema_metadata);
}

std::vector<table_slice>
create_table_slices(const std::shared_ptr<arrow::RecordBatch>& rb,
                    int64_t max_slice_size) {
  auto final_rb = prepare_record_batch(rb);
  auto time_col = rb->GetColumnByName("import_time");
  auto slices = std::vector<table_slice>{};
  slices.reserve(rb->num_rows() / max_slice_size + 1);
  for (int64_t offset = 0; offset < rb->num_rows(); offset += max_slice_size) {
    auto rb_sliced = final_rb->Slice(offset, max_slice_size);
    auto slice = arrow_table_slice_builder::create(rb_sliced);
    slice.import_time(
      derive_import_time(time_col->Slice(offset, max_slice_size)));
    slices.push_back(slice);
  }
  return slices;
}

// Handler for `vast::query` that is shared between active and passive stores.
// Returns a the number of events that match the query.
template <typename Actor>
caf::expected<uint64_t>
handle_lookup(Actor& self, const vast::query& query,
              const std::vector<table_slice>& table_slices) {
  if (table_slices.empty())
    return 0;
  // table slices from parquet can't utilize query hints because we don't retain
  // the global ids.
  if (auto e = tailor(query.expr, table_slices[0].layout()); e) {
    expr = *e;
  }
  uint64_t num_hits = 0ull;
  auto handle_query = detail::overload{
    [&](const query::count& count) -> caf::expected<uint64_t> {
      VAST_ASSERT(count.mode != query::count::estimate);
      std::shared_ptr<arrow::RecordBatch> batch{};
      for (const auto& slice : table_slices) {
        auto result = count_matching(slice, expr, ids);
        num_hits += result;
        self->send(count.sink, result);
      }
      return num_hits;
    },
    [&](const query::extract& extract) -> caf::expected<uint64_t> {
      for (const auto& slice : table_slices) {
        auto final_slice = filter(slice, expr, ids);
        if (final_slice) {
          num_hits += final_slice->rows();
          self->send(extract.sink, *final_slice);
        }
      }
      return num_hits;
    },
  };
  return caf::visit(handle_query, query.cmd);
}

std::shared_ptr<arrow::Schema> parse_arrow_schema_from_metadata(
  const std::shared_ptr<::parquet::FileMetaData>& parquet_metadata) {
  if (!parquet_metadata)
    return {};
  auto arrow_metadata
    = parquet_metadata->key_value_metadata()->Get("ARROW:schema");
  if (!arrow_metadata.ok())
    return {};
  auto decoded = detail::base64::decode(*arrow_metadata);
  auto schema_buf = arrow::Buffer(decoded);
  arrow::ipc::DictionaryMemo dict_memo;
  arrow::io::BufferReader input(schema_buf);
  auto arrow_schema = arrow::ipc::ReadSchema(&input, &dict_memo);
  if (!arrow_schema.ok())
    return {};
  return *arrow_schema;
}

caf::expected<std::shared_ptr<arrow::Table>>
read_parquet_buffer(const chunk_ptr& chunk) {
  VAST_ASSERT(chunk);
  auto bufr = std::make_shared<arrow::io::BufferReader>(as_arrow_buffer(chunk));
  auto parquet_reader = ::parquet::ParquetFileReader::Open(bufr);
  auto arrow_schema
    = parse_arrow_schema_from_metadata(parquet_reader->metadata());
  std::unique_ptr<::parquet::arrow::FileReader> file_reader{};
  if (auto st = ::parquet::arrow::OpenFile(bufr, arrow::default_memory_pool(),
                                           &file_reader);
      !st.ok())
    return caf::make_error(ec::parse_error, st.ToString());
  std::shared_ptr<arrow::Table> table{};
  if (auto st = file_reader->ReadTable(&table); !st.ok())
    return caf::make_error(ec::parse_error, st.ToString());
  return align_table_to_schema(arrow_schema, table);
}

auto init_parquet(caf::unit_t&) {
  // doing nothing: as we're writing the file in one pass at the end into a
  // memory buffer, there's no file opening going on here.
}

auto add_table_slices(
  system::store_builder_actor::stateful_pointer<store_builder_state> self) {
  return [self](caf::unit_t&, std::vector<table_slice>& batch) {
    for (auto& slice : batch) {
      if (self->state.vast_type_) {
        VAST_ASSERT(*self->state.vast_type_ == slice.layout());
      } else {
        self->state.vast_type_ = slice.layout();
      }
      self->state.num_rows_ += slice.rows();
      self->state.table_slices_.push_back(slice);
    }
    VAST_TRACE("{} received batch of {} table slices for partition {}", *self,
               batch.size(), self->state.id_);
  };
}

std::shared_ptr<::parquet::WriterProperties>
writer_properties(const configuration& config) {
  auto builder = ::parquet::WriterProperties::Builder{};
  builder.created_by("VAST")
    ->enable_dictionary()
    ->compression(::parquet::Compression::ZSTD)
    ->compression_level(detail::narrow_cast<int>(config.zstd_compression_level))
    ->version(::parquet::ParquetVersion::PARQUET_2_6);
  return builder.build();
}

std::shared_ptr<::parquet::ArrowWriterProperties> arrow_writer_properties() {
  auto builder = ::parquet::ArrowWriterProperties::Builder{};
  builder.store_schema(); // serialize arrow schema into parquet meta data
  return builder.build();
}

auto make_import_time_col(const time& import_time, int64_t rows) {
  auto v = import_time.time_since_epoch().count();
  auto builder = time_type::make_arrow_builder(arrow::default_memory_pool());
  if (auto status = builder->Reserve(rows); !status.ok())
    die(fmt::format("make time column failed: '{}'", status.ToString()));
  for (int i = 0; i < rows; ++i) {
    auto status = builder->Append(v);
    VAST_ASSERT(status.ok());
  }
  return builder->Finish().ValueOrDie();
}

auto create_record_batch(const table_slice& slice)
  -> std::shared_ptr<arrow::RecordBatch> {
  auto rb = to_record_batch(slice);
  auto md = rb->schema()->metadata();
  auto event_array = rb->ToStructArray().ValueOrDie();
  auto time_col = make_import_time_col(slice.import_time(), rb->num_rows());
  auto schema = arrow::schema(
    {arrow::field("import_time", time_type::to_arrow_type()),
     arrow::field("event", event_array->type(), rb->schema()->metadata())});
  auto new_rb
    = arrow::RecordBatch::Make(schema, rb->num_rows(), {time_col, event_array});
  return new_rb;
}

auto write_parquet_buffer(const std::vector<table_slice>& slices,
                          const configuration& config) {
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto batches = arrow::RecordBatchVector{};
  for (const auto& slice : slices)
    batches.push_back(create_record_batch(slice));
  auto table = arrow::Table::FromRecordBatches(batches).ValueOrDie();
  auto writer_props = writer_properties(config);
  auto arrow_writer_props = arrow_writer_properties();
  auto status
    = ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink,
                                   1 << 24, writer_props, arrow_writer_props);
  VAST_ASSERT(status.ok(), status.ToString().c_str());
  return sink->Finish().ValueOrDie();
}

auto finish_parquet(
  system::store_builder_actor::stateful_pointer<store_builder_state> self) {
  return [self](caf::unit_t&, const caf::error&) {
    auto buffer
      = write_parquet_buffer(self->state.table_slices_, self->state.config_);
    VAST_TRACE("{} writes partition {} with {} events in {} table slices, ",
               *self, self->state.id_, self->state.num_rows_,
               self->state.table_slices_.size());
    auto c = chunk::make(buffer);
    auto bytes = std::span<const std::byte>{c->data(), c->size()};
    self
      ->request(self->state.fs_, caf::infinite, atom::write_v,
                self->state.path_, c)
      .then(
        [self](atom::ok) {
          VAST_TRACE("{} flushed archive to {}", *self, self->state.path_);
          self->state.self_ = nullptr;
        },
        [self](caf::error& err) {
          VAST_ERROR("{} failed to flush archive {}", *self, to_string(err));
          self->state.self_ = nullptr;
        });
  };
}

} // namespace

system::store_actor::behavior_type
store(system::store_actor::stateful_pointer<store_state> self,
      const system::accountant_actor& accountant,
      const system::filesystem_actor& fs, const uuid& id,
      const configuration& config) {
  self->state.self_ = self;
  self->state.id_ = id;
  self->state.accountant_ = accountant;
  self->state.fs_ = fs;
  self->state.path_ = store_path_for_partition(id);
  self->state.config_ = config;
  self->request(self->state.fs_, caf::infinite, atom::mmap_v, self->state.path_)
    .then(
      [self](const chunk_ptr& chunk) {
        if (auto table = read_parquet_buffer(chunk)) {
          auto table_slices = std::vector<table_slice>{};
          for (const auto& rb : arrow::TableBatchReader(*table)) {
            if (!rb.ok())
              self->send_exit(
                self, caf::make_error(ec::format_error,
                                      fmt::format("unable to read record "
                                                  "batch: "
                                                  "{} ",
                                                  rb.status().ToString())));
            auto slices
              = create_table_slices(*rb, detail::narrow_cast<int64_t>(
                                           self->state.config_.row_group_size));
            for (const auto& slice : slices)
              table_slices.push_back(slice);
            self->state.num_rows_ += (*rb)->num_rows();
          }
          self->state.table_slices_ = std::move(table_slices);
        } else {
          self->send_exit(self, table.error());
        }
        for (auto&& [query, rp] :
             std::exchange(self->state.deferred_requests_, {})) {
          VAST_TRACE("{} delegates {} (pending: {})", *self, query,
                     rp.pending());
          rp.delegate(static_cast<system::store_actor>(self), std::move(query));
        }
      },
      [self](caf::error& err) {
        VAST_ERROR("failed to read archive {}: {}", self->state.path_,
                   to_string(err));
        self->state.self_ = nullptr;
      });
  return {
    [self](const query& query) -> caf::result<uint64_t> {
      if (!self->state.table_slices_) {
        auto rp = self->make_response_promise<uint64_t>();
        self->state.deferred_requests_.emplace_back(query, rp);
        return rp;
      }
      auto start = std::chrono::steady_clock::now();
      auto num_hits = handle_lookup(self, query, *self->state.table_slices_);
      duration runtime = std::chrono::steady_clock::now() - start;
      auto id_str = fmt::to_string(query.id);
      self->send(self->state.accountant_, "parquet-store.lookup.runtime",
                 runtime,
                 vast::system::metrics_metadata{{"query", id_str},
                                                {"store-type", "passive"}});
      self->send(self->state.accountant_, "parquet-store.lookup.hits",
                 *num_hits,
                 vast::system::metrics_metadata{{"query", id_str},
                                                {"store-type", "passive"}});
      return num_hits;
    },
    [self](atom::erase, const ids& xs) -> caf::result<uint64_t> {
      auto num_rows = rank(xs);
      VAST_ASSERT(
        num_rows == 0
        || num_rows
             == detail::narrow_cast<unsigned long>(self->state.num_rows_));
      auto rp = self->make_response_promise<uint64_t>();
      self
        ->request(self->state.fs_, caf::infinite, atom::erase_v,
                  self->state.path_)
        .then(
          [rp, num_rows](atom::done) mutable {
            rp.deliver(num_rows);
          },
          [rp](caf::error& err) mutable {
            rp.deliver(std::move(err));
          });
      return rp;
    },
  };
}

system::store_builder_actor::behavior_type store_builder(
  system::store_builder_actor::stateful_pointer<store_builder_state> self,
  system::accountant_actor accountant, system::filesystem_actor fs,
  const uuid& id, const configuration& config) {
  self->state.self_ = self;
  self->state.id_ = id;
  self->state.accountant_ = std::move(accountant);
  self->state.fs_ = std::move(fs);
  self->state.path_ = store_path_for_partition(self->state.id_);
  self->state.config_ = config;
  return {
    [self](const query& query) -> caf::result<uint64_t> {
      return handle_lookup(self, query, self->state.table_slices_);
    },
    [self](atom::erase, const ids& ids) -> caf::result<uint64_t> {
      self->state.table_slices_ = {};
      self->state.num_rows_ = 0;
      return rank(ids);
    },
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      auto sink = caf::attach_stream_sink(
        self, in, init_parquet, add_table_slices(self), finish_parquet(self));
      return {};
    },
    [self](atom::status, system::status_verbosity) -> caf::result<record> {
      auto result = record{};
      auto store = record{};
      store["events"] = count{self->state.num_rows_};
      store["path"] = self->state.path_.string();
      result["parquet-store"] = std::move(store);
      return result;
    },
  };
}

} // namespace vast::plugins::parquet

// Finally, register our plugin.
VAST_REGISTER_PLUGIN(vast::plugins::parquet::plugin)
