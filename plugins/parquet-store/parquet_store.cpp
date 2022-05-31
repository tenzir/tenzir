//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet_store.hpp"

#include <vast/arrow_table_slice_builder.hpp>
#include <vast/chunk.hpp>
#include <vast/data.hpp>
#include <vast/detail/base64.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/detail/zip_iterator.hpp>
#include <vast/error.hpp>
#include <vast/io/write.hpp>
#include <vast/plugin.hpp>
#include <vast/query.hpp>
#include <vast/system/report.hpp>
#include <vast/table_slice.hpp>
#include <vast/type.hpp>

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

#include <memory>
#include <utility>

// TODO: play with smaller (slice-aligned row group sizes)
namespace vast::plugins::parquet_store {

std::filesystem::path store_path_for_partition(const uuid& partition_id) {
  auto store_filename = fmt::format("{}.parquet", partition_id);
  return std::filesystem::path{"archive"} / store_filename;
}

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
      return arr;
    }
  }
}

/// Transform a chunked array by applying a mapping function `Mapper` over each
/// chunk and constructs a new array from the transformed chunks.
template <typename Mapper, class VastType>
std::shared_ptr<arrow::ChunkedArray>
map_chunked_array(const VastType& t,
                  const std::shared_ptr<arrow::ChunkedArray>& arr, Mapper m) {
  auto chunks = arrow::ArrayVector{};
  chunks.reserve(arr->num_chunks());
  for (const auto& chunk : arr->chunks()) {
    if (std::shared_ptr<arrow::Array> c = m(t, chunk))
      chunks.push_back(c);
    else
      return arr;
  }
  auto result = arrow::ChunkedArray::Make(chunks);
  return result.ValueOrDie();
}

std::shared_ptr<arrow::Array>
map_array(const vast::type& t, std::shared_ptr<arrow::Array> array) {
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

      auto inner_type
        = arrow::struct_(std::vector<std::shared_ptr<arrow::Field>>{
          arrow::field("address", address_type::to_arrow_type()),
          arrow::field("length", arrow::uint8()),
        });
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
      if (auto fixed_array = map_array(lt.value_type(), list_array->values()))
        return std::make_shared<arrow::ListArray>(
          lt.to_arrow_type(), list_array->length(), list_array->value_offsets(),
          fixed_array, list_array->null_bitmap(), list_array->null_count());
      return {};
    },
    [&](const map_type& mt) -> std::shared_ptr<arrow::Array> {
      auto ma = std::static_pointer_cast<arrow::MapArray>(array);
      auto key_array = map_array(mt.key_type(), ma->keys());
      auto val_array = map_array(mt.value_type(), ma->items());
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
        auto mapped_arr = map_array(field.type, *it);
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
/// `ChunkedArray`s only occurr at the outermost level, and the VAST type
/// that is not properly represented at this level is `enumeration_type`.
std::shared_ptr<arrow::ChunkedArray>
restore_enum_chunk_array(const vast::type& t,
                         std::shared_ptr<arrow::ChunkedArray> array) {
  auto f = detail::overload{
    [&](const enumeration_type& et) -> std::shared_ptr<arrow::ChunkedArray> {
      return map_chunked_array(et, array, fix_enum_array);
    },
    [&](const list_type&) -> std::shared_ptr<arrow::ChunkedArray> {
      return map_chunked_array(t, array, map_array);
    },
    [&](const map_type&) -> std::shared_ptr<arrow::ChunkedArray> {
      return map_chunked_array(t, array, map_array);
    },
    [&](const record_type&) -> std::shared_ptr<arrow::ChunkedArray> {
      return map_chunked_array(t, array, map_array);
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

/// Transform a record batch into a table slice
table_slice create_table_slice(const std::shared_ptr<arrow::RecordBatch>& rb) {
  auto time_col = rb->GetColumnByName("import_time");
  auto min_max_time = arrow::compute::MinMax(time_col).ValueOrDie();
  auto max_value = min_max_time.scalar_as<arrow::StructScalar>().value[1];
  auto event_col = rb->GetColumnByName("event");
  auto schema_metadata = rb->schema()->GetFieldByName("event")->metadata();
  auto event_rb = arrow::RecordBatch::FromStructArray(event_col).ValueOrDie();
  auto slice = arrow_table_slice_builder::create(
    event_rb->ReplaceSchemaMetadata(schema_metadata));
  auto f = detail::overload{
    [&](const arrow::TimestampScalar& ts) -> void {
      slice.import_time(vast::time{duration{ts.value}});
    },
    [&](const arrow::Scalar&) -> void {},
  };
  caf::visit(f, *max_value);
  return slice;
}

template <typename Actor>
caf::expected<uint64_t>
handle_lookup(Actor& self, const vast::query& query,
              const std::shared_ptr<arrow::Table>& table) {
  auto table_slices = std::vector<table_slice>{};
  for (const auto& rb : arrow::TableBatchReader(*table)) {
    if (!rb.ok())
      return caf::make_error(ec::format_error,
                             fmt::format("unable to read record batch: {} ",
                                         rb.status().ToString()));
    table_slices.push_back(create_table_slice(*rb));
  }
  return handle_lookup(self, query, table_slices);
}

// Handler for `vast::query` that is shared between active and passive stores.
// Returns a the number of events that match the query.
// Precondition: Query type is either `count` or `extract`.
template <typename Actor>
caf::expected<uint64_t>
handle_lookup(Actor& self, const vast::query& query,
              const std::vector<table_slice>& table_slices) {
  if (table_slices.empty())
    return 0;
  // table slices from parquet can't utilize query hints because we don't retain
  // the global ids.
  const auto& ids = vast::ids{};
  auto expr = expression{};
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
  const std::shared_ptr<parquet::FileMetaData>& parquet_metadata) {
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
  auto parquet_reader = parquet::ParquetFileReader::Open(bufr);
  auto arrow_schema
    = parse_arrow_schema_from_metadata(parquet_reader->metadata());
  std::unique_ptr<parquet::arrow::FileReader> file_reader{};
  if (!parquet::arrow::OpenFile(bufr, arrow::default_memory_pool(),
                                &file_reader)
         .ok())
    return caf::exit_reason::unhandled_exception;
  std::shared_ptr<arrow::Table> table{};
  if (!file_reader->ReadTable(&table).ok())
    return caf::exit_reason::unhandled_exception;
  return align_table_to_schema(arrow_schema, table);
}

system::store_actor::behavior_type
store(system::store_actor::stateful_pointer<store_state> self,
      const system::accountant_actor& accountant,
      const system::filesystem_actor& fs, const uuid& id) {
  self->state.self_ = self;
  self->state.id_ = id;
  self->state.accountant_ = accountant;
  self->state.fs_ = fs;
  self->state.path_ = store_path_for_partition(id);
  self->request(self->state.fs_, caf::infinite, atom::mmap_v, self->state.path_)
    .then(
      [self](const chunk_ptr& chunk) {
        if (auto table = read_parquet_buffer(chunk); table) {
          self->state.table_ = *table;
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
      if (!self->state.table_) {
        auto rp = self->make_response_promise<uint64_t>();
        self->state.deferred_requests_.emplace_back(query, rp);
        return rp;
      }
      auto start = std::chrono::steady_clock::now();
      auto slices = std::vector<table_slice>{};
      auto num_hits = handle_lookup(self, query, self->state.table_);
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
      // TODO: segment store tracks in-flight erase requests similar to queries.
      // however, in our case we don't support erasing a subset of the data, so
      // we can just delete the file directly?
      auto num_rows = rank(xs);
      VAST_ASSERT(
        num_rows == 0
        || num_rows
             == static_cast<unsigned long>(self->state.table_->num_rows()));
      auto rp = self->make_response_promise<uint64_t>();
      self
        ->request(self->state.fs_, caf::infinite, atom::erase_v,
                  self->state.path_)
        .then(
          [rp, num_rows](atom::done) mutable {
            rp.deliver(num_rows);
          },
          [&](caf::error& err) mutable {
            rp.deliver(std::move(err));
          });
      return rp;
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
      if (self->state.vast_type_) {
        VAST_ASSERT(*self->state.vast_type_ == slice.layout());
      } else {
        self->state.vast_type_ = slice.layout();
      }
      self->state.num_rows_ += slice.rows();
      self->state.table_slices_.push_back(slice);
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

// TODO: make this static or something - no need to recompute every time
std::shared_ptr<parquet::ArrowWriterProperties> arrow_writer_properties() {
  auto builder = parquet::ArrowWriterProperties::Builder{};
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

auto write_parquet_buffer(const std::vector<table_slice>& slices) {
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto batches = arrow::RecordBatchVector{};
  for (const auto& slice : slices)
    batches.push_back(create_record_batch(slice));
  auto table = arrow::Table::FromRecordBatches(batches).ValueOrDie();
  auto writer_props = writer_properties();
  auto arrow_writer_props = arrow_writer_properties();
  auto status
    = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink,
                                 1 << 24, writer_props, arrow_writer_props);
  VAST_ASSERT(status.ok(), status.ToString().c_str());
  return sink->Finish().ValueOrDie();
}

auto finish_parquet(
  system::store_builder_actor::stateful_pointer<store_builder_state> self) {
  return [self](caf::unit_t&, const caf::error&) {
    auto buffer = write_parquet_buffer(self->state.table_slices_);
    VAST_TRACE("[{}::{}] write triggered, w/ {} records in {} table slices, "
               "parquet file "
               "size: {} bytes",
               *self, self->state.id_, self->state.num_rows_,
               self->state.table_slices_.size(), buffer->size());
    auto c = chunk::make(buffer);
    auto bytes = std::span<const std::byte>{c->data(), c->size()};
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
  self->state.self_ = self;
  self->state.id_ = id;
  self->state.accountant_ = std::move(accountant);
  self->state.fs_ = std::move(fs);
  self->state.path_ = store_path_for_partition(self->state.id_);
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

caf::error plugin::initialize(data options) {
  if (caf::holds_alternative<caf::none_t>(options))
    return caf::none;
  if (const auto* rec = caf::get_if<record>(&options))
    if (rec->empty())
      return caf::none;
  return caf::make_error(ec::invalid_configuration, //
                         "expected empty configuration under "
                         "vast.plugins.parquet-store");
}

[[nodiscard]] const char* plugin::name() const {
  return "parquet-store";
}

[[nodiscard]] caf::expected<plugin::builder_and_header>
plugin::make_store_builder(system::accountant_actor accountant,
                           system::filesystem_actor fs,
                           const vast::uuid& id) const {
  auto actor_handle = fs.home_system().spawn(store_builder, accountant, fs, id);
  auto header = chunk::copy(id);
  return builder_and_header{actor_handle, header};
}

[[nodiscard]] caf::expected<system::store_actor>
plugin::make_store(system::accountant_actor accountant,
                   system::filesystem_actor fs,
                   std::span<const std::byte> header) const {
  if (header.size() != uuid::num_bytes)
    return caf::make_error(ec::invalid_argument, "header must have size of "
                                                 "single uuid");
  auto id = uuid(
    std::span<const std::byte, uuid::num_bytes>(header.data(), header.size()));
  return fs.home_system().spawn(store, accountant, fs, id);
}

} // namespace vast::plugins::parquet_store

// Finally, register our plugin.
VAST_REGISTER_PLUGIN(vast::plugins::parquet_store::plugin)
