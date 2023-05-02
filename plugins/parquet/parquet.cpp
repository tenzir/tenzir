//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_compat.hpp>
#include <vast/arrow_table_slice.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/detail/base64.hpp>
#include <vast/detail/inspection_common.hpp>
#include <vast/plugin.hpp>
#include <vast/store.hpp>

#include <arrow/array.h>
#include <arrow/compute/cast.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>
#include <caf/expected.hpp>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

namespace vast::plugins::parquet {

/// Configuration for the Parquet plugin.
struct configuration {
  uint64_t row_group_size{defaults::import::table_slice_size};
  int64_t zstd_compression_level{
    arrow::util::Codec::DefaultCompressionLevel(arrow::Compression::ZSTD)
      .ValueOrDie()};

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return detail::apply_all(f, x.row_group_size, x.zstd_compression_level);
  }

  static const record_type& schema() noexcept {
    static auto result = record_type{
      {"row-group-size", uint64_type{}},
      {"zstd-compression-level", int64_type{}},
    };
    return result;
  }
};

namespace {

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
          if (!builder.Append(*et.resolve(arrow_compat::align_type(*v))).ok())
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
    [&](const ip_type&) -> std::shared_ptr<arrow::Array> {
      if (ip_type::to_arrow_type()->Equals(array->type()))
        return {}; // address is not always wrong, only when inside maps
      return std::make_shared<ip_type::array_type>(ip_type::to_arrow_type(),
                                                   array);
    },
    [&](const subnet_type&) -> std::shared_ptr<arrow::Array> {
      if (subnet_type::to_arrow_type()->Equals(array->type()))
        return {};
      auto sa = std::static_pointer_cast<arrow::StructArray>(array);
      auto ip_array = std::make_shared<ip_type::array_type>(
        ip_type::to_arrow_type(), sa->field(0));
      auto inner_type = subnet_type::to_arrow_type()->storage_type();
      auto children = std::vector<std::shared_ptr<arrow::Array>>{
        ip_array,
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
  return value_at(time_type{}, *time_col, time_col->length() - 1);
}

/// Extract event column from record batch and transform into new record batch.
/// The record batch contains a message envelope with the actual event data
/// alongside VAST-related meta data (currently limited to the import time).
/// Message envelope is unwrapped and the metadata, attached to the to-level
/// schema the input record batch is copied to the newly created record batch.
std::shared_ptr<arrow::RecordBatch>
unwrap_record_batch(const std::shared_ptr<arrow::RecordBatch>& rb) {
  auto event_col = rb->GetColumnByName("event");
  auto schema_metadata = rb->schema()->GetFieldByName("event")->metadata();
  auto event_rb = arrow::RecordBatch::FromStructArray(event_col).ValueOrDie();
  return event_rb->ReplaceSchemaMetadata(schema_metadata);
}

/// Create multiple table slices for a record batch, splitting at `max_slice_size`
std::vector<table_slice>
create_table_slices(const std::shared_ptr<arrow::RecordBatch>& rb,
                    int64_t max_slice_size) {
  auto final_rb = unwrap_record_batch(rb);
  auto time_col = rb->GetColumnByName("import_time");
  auto slices = std::vector<table_slice>{};
  slices.reserve(rb->num_rows() / max_slice_size + 1);
  auto schema = type::from_arrow(*final_rb->schema());
  for (int64_t offset = 0; offset < rb->num_rows(); offset += max_slice_size) {
    auto rb_sliced = final_rb->Slice(offset, max_slice_size);
    auto& slice = slices.emplace_back(rb_sliced, schema);
    slice.import_time(
      derive_import_time(time_col->Slice(offset, max_slice_size)));
    slice.offset(detail::narrow_cast<id>(offset));
  }
  return slices;
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
  const auto options = ::parquet::default_reader_properties();
  auto parquet_reader = ::parquet::ParquetFileReader::Open(bufr, options);
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

/// Create a constant column for the given import time with `rows` rows
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

/// Wrap a record batch into an event envelope containing the event data
/// as a nested struct alongside metadata as separate columns, containing
/// the `import_time`.
auto wrap_record_batch(const table_slice& slice)
  -> std::shared_ptr<arrow::RecordBatch> {
  auto rb = to_record_batch(slice);
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
    batches.push_back(wrap_record_batch(slice));
  auto table = arrow::Table::FromRecordBatches(batches).ValueOrDie();
  auto writer_props = writer_properties(config);
  auto arrow_writer_props = arrow_writer_properties();
  auto status
    = ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink,
                                   1 << 24, writer_props, arrow_writer_props);
  VAST_ASSERT(status.ok(), status.ToString().c_str());
  return sink->Finish().ValueOrDie();
}

class passive_parquet_store final : public passive_store {
public:
  explicit passive_parquet_store(const configuration& config)
    : parquet_config_{config} {
  }

  /// Load the store contents from the given chunk.
  /// @param chunk The chunk pointing to the store's persisted data.
  /// @returns An error on failure.
  [[nodiscard]] caf::error load(chunk_ptr chunk) override {
    auto table = read_parquet_buffer(chunk);
    if (!table)
      return table.error();
    for (const auto& rb : arrow::TableBatchReader(*table)) {
      if (!rb.ok())
        return caf::make_error(ec::system_error,
                               fmt::format("unable to read record batch: {}",
                                           rb.status().ToString()));
      auto slices_for_batch = create_table_slices(
        *rb, detail::narrow_cast<int64_t>(parquet_config_.row_group_size));
      slices_.reserve(slices_for_batch.size() + slices_.size());
      slices_.insert(slices_.end(),
                     std::make_move_iterator(slices_for_batch.begin()),
                     std::make_move_iterator(slices_for_batch.end()));
      num_rows_ += (*rb)->num_rows();
    }
    return {};
  }

  /// Retrieve all of the store's slices.
  /// @returns The store's slices.
  [[nodiscard]] generator<table_slice> slices() const override {
    // We need to make a copy of the slices here because the slices_ vector may
    // get invalidated while we iterate over it.
    auto slices = slices_;
    for (auto& slice : slices)
      co_yield std::move(slice);
  }

  [[nodiscard]] uint64_t num_events() const override {
    return num_rows_;
  }

private:
  std::vector<table_slice> slices_ = {};
  configuration parquet_config_ = {};
  uint64_t num_rows_ = {};
};

class active_parquet_store final : public active_store {
public:
  explicit active_parquet_store(const configuration& config)
    : parquet_config_{config} {
  }

  /// Add a set of slices to the store.
  /// @returns An error on failure.
  [[nodiscard]] caf::error add(std::vector<table_slice> new_slices) override {
    slices_.reserve(new_slices.size() + slices_.size());
    for (auto& slice : new_slices) {
      // The index already sets the correct offset for this slice, but in some
      // unit tests we test this component separately, causing incoming table
      // slices not to have an offset at all. We should fix the unit tests
      // properly, but that takes time we did not want to spend when migrating
      // to partition-local ids. -- DL
      if (slice.offset() == invalid_id)
        slice.offset(num_rows_);
      VAST_ASSERT(slice.offset() == num_rows_);
      num_rows_ += slice.rows();
      slices_.push_back(std::move(slice));
    }
    return {};
  }

  /// Persist the store contents to a contiguous buffer.
  /// @returns A chunk containing the serialized store contents, or an error on
  /// failure.
  [[nodiscard]] caf::expected<chunk_ptr> finish() override {
    auto buffer = write_parquet_buffer(slices_, parquet_config_);
    auto chunk = chunk::make(buffer);
    return chunk;
  }

  /// Retrieve all of the store's slices.
  /// @returns The store's slices.
  [[nodiscard]] generator<table_slice> slices() const override {
    for (const auto& slice : slices_)
      co_yield slice;
  }

  [[nodiscard]] uint64_t num_events() const override {
    return num_rows_;
  }

private:
  uint64_t num_rows_ = {};
  std::vector<table_slice> slices_ = {};
  configuration parquet_config_ = {};
};

class plugin final : public virtual store_plugin {
  caf::error initialize(const record& plugin_config,
                        const record& global_config) override {
    const auto default_compression_level
      = arrow::util::Codec::DefaultCompressionLevel(arrow::Compression::ZSTD)
          .ValueOrDie();
    auto level = try_get_or(global_config, "vast.zstd-compression-level",
                            default_compression_level);
    if (!level) {
      return std::move(level.error());
    }
    zstd_compression_level_ = *level;
    return convert(plugin_config, parquet_config_);
  }

  [[nodiscard]] std::string name() const override {
    return "parquet";
  }

  [[nodiscard]] caf::expected<std::unique_ptr<passive_store>>
  make_passive_store() const override {
    return std::make_unique<passive_parquet_store>(parquet_config_);
  }

  auto make_active_store() const
    -> caf::expected<std::unique_ptr<active_store>> override {
    return std::make_unique<active_parquet_store>(
      configuration{parquet_config_.row_group_size, zstd_compression_level_});
  }

private:
  int zstd_compression_level_ = {};
  configuration parquet_config_ = {};
};

} // namespace

} // namespace vast::plugins::parquet

// Finally, register our plugin.
VAST_REGISTER_PLUGIN(vast::plugins::parquet::plugin)
