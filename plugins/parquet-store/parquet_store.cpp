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

#include <arrow/array/array_dict.h>
#include <arrow/compute/cast.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/ipc/reader.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/util/key_value_metadata.h>
#include <caf/actor_system_config.hpp>
#include <caf/attach_stream_sink.hpp>
#include <caf/expected.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <memory>
#include <utility>

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
    [&](const auto& t) -> std::shared_ptr<arrow::Array> {
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

// Handler for `vast::query` that is shared between active and passive stores.
// Returns a the number of events that match the query.
// Precondition: Query type is either `count` or `extract`.
template <typename Actor>
caf::expected<uint64_t>
handle_lookup(Actor& self, const vast::query& query,
              const std::shared_ptr<arrow::Table>& table) {
  const auto& ids = query.ids;
  const auto layout = type::from_arrow(*table->schema());
  auto expr = expression{};
  if (auto e = tailor(query.expr, layout); e) {
    expr = *e;
  }
  uint64_t num_hits = 0ull;
  auto record_batch_reader = arrow::TableBatchReader(*table);
  auto handle_query = detail::overload{
    [&](const query::count& count) -> caf::expected<uint64_t> {
      if (count.mode == query::count::estimate)
        die("logic error detected - count estimate should not load "
            "partition");
      std::shared_ptr<arrow::RecordBatch> batch{};
      for (const auto& rb : record_batch_reader) {
        if (rb.ok()) {
          const auto slice = arrow_table_slice_builder::create(*rb);
          auto result = count_matching(slice, expr, ids);
          num_hits += result;
          self->send(count.sink, result);
        } else {
          return caf::make_error(ec::format_error,
                                 fmt::format("unable to read record batch: {}",
                                             rb.status().ToString()));
        }
      }
      return num_hits;
    },
    [&](const query::extract& extract) -> caf::expected<uint64_t> {
      if (extract.policy == query::extract::preserve_ids) {
        self->send(extract.sink, table_slice{});
        return caf::make_error(ec::invalid_query, "preserve_ids not supported "
                                                  "in parquet format");
      }
      for (const auto& rb : record_batch_reader) {
        if (rb.ok()) {
          const auto slice = arrow_table_slice_builder::create(*rb);
          auto final_slice = filter(slice, expr, ids);
          if (final_slice) {
            num_hits += final_slice->rows();
            self->send(extract.sink, *final_slice);
          }
        } else {
          return caf::make_error(ec::format_error,
                                 fmt::format("unable to read record batch: {}",
                                             rb.status().ToString()));
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
      // if (!num_hits)
      //   return num_hits.error();
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
      return *num_hits;
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

auto write_parquet_buffer(const arrow::RecordBatchVector& batches) {
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto table = arrow::Table::FromRecordBatches(batches).ValueOrDie();
  auto writer_props = writer_properties();
  auto arrow_writer_props = arrow_writer_properties();
  auto status
    = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink,
                                 1 << 24, writer_props, arrow_writer_props);
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
    auto path = store_path_for_partition(self->state.id_);
    auto c = chunk::make(buffer);
    auto bytes = std::span<const std::byte>{c->data(), c->size()};
    // TODO: remove extra-write to local fs for debugging ;)
    vast::io::write(std::filesystem::path{"/tmp/archive"} / path, bytes);
    self->request(self->state.fs_, caf::infinite, atom::write_v, path, c)
      .then(
        [self, path](atom::ok) {
          VAST_TRACE("flush archive ./vast.db/{}", path);
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
  make_store(system::accountant_actor accountant, system::filesystem_actor fs,
             std::span<const std::byte> header) const override {
    if (header.size() != uuid::num_bytes)
      return caf::make_error(ec::invalid_argument, "header must have size of "
                                                   "single uuid");
    auto id = uuid(std::span<const std::byte, uuid::num_bytes>(header.data(),
                                                               header.size()));
    return fs.home_system().spawn(store, accountant, fs, id);
  }
};

} // namespace vast::plugins::parquet_store

// Finally, register our plugin.
VAST_REGISTER_PLUGIN(vast::plugins::parquet_store::plugin)
