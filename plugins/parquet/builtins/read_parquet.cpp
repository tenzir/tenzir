//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet/row_group_pruning.hpp"
#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/option.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/async/bounded_queue.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/enum.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/nova/arrow_import.hpp>
#include <tenzir/nova/arrow_metadata.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/read_detection.hpp>
#include <tenzir/read_pushdown.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/cast.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/arrow/reader.h>

namespace tenzir::plugins::parquet {

TENZIR_ENUM(decimal_format, string, float_);

struct ReadParquetArgs {
  Option<located<std::string>> decimal_format;
  OptimizationArgs<opt::Filter, opt::Limit, opt::Projection> optimization;
};

namespace {

using namespace si_literals;

auto format_decimal_type(std::shared_ptr<arrow::DataType> type,
                         decimal_format format)
  -> std::shared_ptr<arrow::DataType> {
  switch (type->id()) {
    case arrow::Type::DECIMAL128:
      return format == decimal_format::string ? arrow::utf8()
                                              : arrow::float64();
    case arrow::Type::STRUCT: {
      auto fields = type->fields();
      auto changed = false;
      for (auto& field : fields) {
        auto field_type = format_decimal_type(field->type(), format);
        changed |= field_type != field->type();
        field = field->WithType(std::move(field_type));
      }
      return changed ? arrow::struct_(std::move(fields)) : std::move(type);
    }
    case arrow::Type::LIST: {
      auto list_type = std::static_pointer_cast<arrow::ListType>(type);
      auto value_type = format_decimal_type(list_type->value_type(), format);
      if (value_type == list_type->value_type()) {
        return type;
      }
      return arrow::list(
        list_type->value_field()->WithType(std::move(value_type)));
    }
    case arrow::Type::MAP: {
      if (format == decimal_format::float_) {
        return type;
      }
      auto map_type = std::static_pointer_cast<arrow::MapType>(type);
      auto key_type = format_decimal_type(map_type->key_type(), format);
      auto item_type = format_decimal_type(map_type->item_type(), format);
      if (key_type == map_type->key_type()
          and item_type == map_type->item_type()) {
        return type;
      }
      return std::make_shared<arrow::MapType>(
        map_type->key_field()->WithType(std::move(key_type)),
        map_type->item_field()->WithType(std::move(item_type)),
        map_type->keys_sorted());
    }
    default:
      return type;
  }
}

auto format_decimal_arrays(std::shared_ptr<arrow::RecordBatch> batch,
                           decimal_format format)
  -> arrow::Result<std::shared_ptr<arrow::RecordBatch>> {
  auto arrays = arrow::ArrayVector{};
  auto fields = arrow::FieldVector{};
  auto changed = false;
  arrays.reserve(batch->num_columns());
  fields.reserve(batch->num_columns());
  for (auto index = 0; index < batch->num_columns(); ++index) {
    auto array = batch->column(index);
    auto target_type = format_decimal_type(array->type(), format);
    if (target_type != array->type()) {
      ARROW_ASSIGN_OR_RAISE(
        auto result, arrow::compute::Cast(array, std::move(target_type)));
      array = result.make_array();
      changed = true;
    }
    arrays.push_back(array);
    fields.push_back(batch->schema()->field(index)->WithType(array->type()));
  }
  if (not changed) {
    return batch;
  }
  auto schema = std::make_shared<arrow::Schema>(std::move(fields),
                                                batch->schema()->endianness(),
                                                batch->schema()->metadata());
  return arrow::RecordBatch::Make(std::move(schema), batch->num_rows(),
                                  std::move(arrays));
}

auto inject_tenzir_metadata(std::shared_ptr<arrow::RecordBatch> batch)
  -> std::shared_ptr<arrow::RecordBatch> {
  auto needs_name = true;
  auto needs_stripping = false;
  auto metadata = batch->schema()->metadata();
  auto keys = metadata ? metadata->keys() : std::vector<std::string>{};
  auto values = metadata ? metadata->values() : std::vector<std::string>{};
  for (const auto& key : keys) {
    if (key == "TENZIR:name:0") {
      needs_name = false;
      continue;
    }
    if (not key.starts_with("TENZIR:")) {
      needs_stripping = true;
    }
  }
  if (not needs_name and not needs_stripping) {
    return batch;
  }
  if (needs_stripping) {
    auto kit = keys.begin();
    auto vit = values.begin();
    while (kit != keys.end()) {
      if (not kit->starts_with("TENZIR:")) {
        vit = values.erase(vit);
        kit = keys.erase(kit);
        continue;
      }
      ++kit;
      ++vit;
    }
  }
  if (needs_name) {
    keys.emplace_back("TENZIR:name:0");
    values.emplace_back("tenzir.parquet");
  }
  TENZIR_ASSERT(keys.size() == values.size());
  return batch->ReplaceSchemaMetadata(
    arrow::key_value_metadata(std::move(keys), std::move(values)));
}

/// State and steps that the byte-stream and the file reader share: the
/// pushed-down filter, limit, and projection, and batch conversion.
class ParquetDecoding {
protected:
  explicit ParquetDecoding(ReadParquetArgs args)
    : filter_{std::move(args.optimization.filter)},
      remaining_{args.optimization.limit},
      decimal_format_{args.decimal_format ? from_string<decimal_format>(
                                              args.decimal_format->inner)
                                              .value_or(decimal_format::string)
                                          : decimal_format::string},
      projection_{
        read_projection(std::move(args.optimization.projection), filter_)} {
  }

  /// Prepares the pushed-down filters, once per reader.
  auto make_filters(nova::InstantiateCtx ctx) -> failure_or<void> {
    for (auto const& filter : filter_) {
      TRY(auto evaluator, nova::Evaluator::make(filter, ctx));
      filters_.push_back(std::move(evaluator));
    }
    return {};
  }

  /// Batch conversion is independent of how the file bytes were obtained.
  auto convert(std::shared_ptr<arrow::RecordBatch> batch,
               diagnostic_handler& dh) -> Option<nova::Events> {
    auto columns = batch->ToStructArray();
    if (not columns.ok()) {
      diagnostic::error("failed to read parquet columns")
        .note("{}", columns.status().ToStringWithoutContextLines())
        .emit(dh);
      return {};
    }
    // Retain only schema metadata, releasing the batch's aliases before Nova
    // adopts the decoded buffers. Do not inspect the Arrow columns afterward.
    auto metadata = nova::ArrowMetadata::from_arrow(*batch->schema());
    batch.reset();
    auto imported = nova::import_arrow_array(std::move(*columns));
    if (not imported) {
      diagnostic::error("parquet file contains unsupported types")
        .note("{}", imported.unwrap_err())
        .emit(dh);
      return {};
    }
    auto records = imported.unwrap().try_as<nova::Record>();
    TENZIR_ASSERT(records);
    auto length = records->length();
    auto meta = metadata.to_meta(length);
    return apply_read_pushdown(nova::Events{std::move(*records),
                                            nova::storage::BitMap{length, true},
                                            std::move(meta)},
                               filters_, remaining_, dh);
  }

  ir::OptimizeFilter filter_;
  Option<uint64_t> remaining_;
  decimal_format decimal_format_ = decimal_format::string;
  Option<std::vector<std::string>> projection_;
  std::vector<nova::Evaluator> filters_;
};

class ReadParquet final : public Operator<chunk_ptr, nova::Events>,
                          ParquetDecoding {
public:
  explicit ReadParquet(ReadParquetArgs args)
    : ParquetDecoding{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (remaining_ == uint64_t{0}) {
      co_return;
    }
    std::ignore = make_filters(nova::InstantiateCtx{ctx.dh(), ctx.reg()});
  }

  auto process(chunk_ptr input, Push<nova::Events>&, OpCtx&)
    -> Task<void> override {
    // NOTE: The parquet format stores key decoding metadata in the file
    // footer. With plain streaming bytes, we cannot decode row groups before
    // seeing the footer, so we buffer and parse in `finalize()`. This also
    // means checkpointing is currently unsupported: restoring would require
    // persisting potentially huge buffered input and parser progress.
    //
    // Keep byte buffering separate from batch conversion and pushdown, which
    // `ReadParquetFile` shares when it gets the file as a whole.
    if (remaining_ == uint64_t{0} or not input or input->size() == 0) {
      co_return;
    }
    chunks_.push_back(std::move(input));
  }

  auto finalize(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (remaining_ == uint64_t{0}) {
      co_return FinalizeBehavior::done;
    }
    auto parquet_chunk = join_chunks(std::move(chunks_));
    if (parquet_chunk->size() == 0) {
      co_return FinalizeBehavior::done;
    }
    auto input_file = as_arrow_file(std::move(parquet_chunk));
    auto parquet_reader_properties
      = ::parquet::ReaderProperties(arrow_memory_pool());
    parquet_reader_properties.enable_buffered_stream();
    auto arrow_reader_properties = ::parquet::ArrowReaderProperties();
    arrow_reader_properties.set_batch_size(defaults::import::table_slice_size);
    // The input already is an in-memory buffer. Pre-buffering would coalesce
    // and copy the selected column chunks a second time without any I/O win.
    arrow_reader_properties.set_pre_buffer(false);
    std::unique_ptr<::parquet::arrow::FileReader> out_buffer;
    try {
      auto input_buffer = ::parquet::ParquetFileReader::Open(
        std::move(input_file), parquet_reader_properties);
      auto out_buffer_result = ::parquet::arrow::FileReader::Make(
        arrow_memory_pool(), std::move(input_buffer), arrow_reader_properties);
      if (not out_buffer_result.ok()) {
        diagnostic::error(
          "{}", out_buffer_result.status().ToStringWithoutContextLines())
          .emit(ctx);
        co_return FinalizeBehavior::done;
      }
      out_buffer = std::move(out_buffer_result).MoveValueUnsafe();
    } catch (const ::parquet::ParquetInvalidOrCorruptedFileException& err) {
      diagnostic::error("invalid or corrupted parquet file: {}", err.what())
        .emit(ctx);
      co_return FinalizeBehavior::done;
    }
    auto metadata = out_buffer->parquet_reader()->metadata();
    auto columns = std::vector<int>{};
    for (auto i = 0; i < metadata->num_columns(); ++i) {
      auto const& name = metadata->schema()->GetColumnRoot(i)->name();
      if (not projection_
          or std::ranges::find(*projection_, name) != projection_->end()) {
        columns.push_back(i);
      }
    }
    // Zero-column batches preserve cardinality without decoding unrequested
    // columns, which may contain unsupported types or corrupt data.
    auto selected = select_row_groups(filter_, *metadata);
    auto row_groups = std::vector<int>{};
    auto available = uint64_t{0};
    for (auto i = 0; i < metadata->num_row_groups(); ++i) {
      if (not selected.get(i)) {
        continue;
      }
      row_groups.push_back(i);
      available += metadata->RowGroup(i)->num_rows();
      if (filter_.empty() and remaining_ and available >= *remaining_) {
        break;
      }
    }
    if (row_groups.empty()) {
      co_return FinalizeBehavior::done;
    }
    auto rb_reader = out_buffer->GetRecordBatchReader(row_groups, columns);
    if (not rb_reader.ok()) {
      diagnostic::error("{}", rb_reader.status().ToStringWithoutContextLines())
        .note("failed create record batches from input data")
        .emit(ctx);
      co_return FinalizeBehavior::done;
    }
    auto reader = std::move(*rb_reader);
    auto maybe_batch = reader->Next();
    while (true) {
      if (not maybe_batch.ok()) {
        diagnostic::error("{}",
                          maybe_batch.status().ToStringWithoutContextLines())
          .note("failed to read record batch")
          .emit(ctx);
        co_return FinalizeBehavior::done;
      }
      auto batch = maybe_batch.MoveValueUnsafe();
      if (not batch) {
        break;
      }
      available -= batch->num_rows();
      auto next = arrow::Result<std::shared_ptr<arrow::RecordBatch>>{
        std::shared_ptr<arrow::RecordBatch>{}};
      if (available == 0
          or (filter_.empty() and remaining_
              and *remaining_ <= static_cast<uint64_t>(batch->num_rows()))) {
        reader.reset();
        out_buffer.reset();
      } else {
        // Arrow retains the last decoded table. One-batch lookahead releases
        // its aliases so Nova can adopt the current buffers. Defer errors in
        // the next batch until it is needed; a filter or limit may stop here.
        next = reader->Next();
      }
      auto formatted = format_decimal_arrays(std::move(batch), decimal_format_);
      if (not formatted.ok()) {
        diagnostic::error("failed to format parquet decimals")
          .note("{}", formatted.status().ToStringWithoutContextLines())
          .emit(ctx);
        co_return FinalizeBehavior::done;
      }
      batch = inject_tenzir_metadata(std::move(*formatted));
      if (not co_await process_batch(std::move(batch), push, ctx)) {
        co_return FinalizeBehavior::done;
      }
      if (remaining_ == uint64_t{0}) {
        break;
      }
      maybe_batch = std::move(next);
    }
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return remaining_ == uint64_t{0} ? OperatorState::done
                                     : OperatorState::normal;
  }

  auto snapshot(Serde&) -> void override {
    // Checkpointing this operator would require persisting the buffered parquet
    // bytes until the footer arrives, which can be arbitrarily large. When it
    // gets the file as a whole, `ReadParquetFile` runs instead and checkpoints
    // its position.
    diagnostic::error("read_parquet does not support checkpoints yet").throw_();
  }

private:
  auto process_batch(std::shared_ptr<arrow::RecordBatch> batch,
                     Push<nova::Events>& push, OpCtx& ctx) -> Task<bool> {
    auto events = convert(std::move(batch), ctx.dh());
    if (not events) {
      co_return false;
    }
    if (events->active_count() != 0) {
      co_await push(std::move(*events));
    }
    co_return true;
  }

  std::vector<chunk_ptr> chunks_;
};

/// Where a scan stands, for resuming it after a restart.
struct ScanPosition {
  /// Rows of the file consumed so far, including rows the filter dropped.
  uint64_t rows = 0;
  /// The part of the pushed-down limit that is still open, if any.
  Option<uint64_t> remaining = None{};

  friend auto inspect(auto& f, ScanPosition& x) -> bool {
    return f.object(x).fields(f.field("rows", x.rows),
                              f.field("remaining", x.remaining));
  }
};

/// A batch of events together with the position right after it.
struct ScanBatch {
  nova::Events events;
  ScanPosition position;
};

/// Reads a seekable Parquet file: the footer first, then only the selected
/// column chunks, one row group at a time, fetching the next row group while
/// the current one decodes.
class ParquetScan final : ParquetDecoding {
public:
  explicit ParquetScan(ReadParquetArgs args)
    : ParquetDecoding{std::move(args)} {
  }

  /// Prepares the filters and reads the footer, starting at `from` if set.
  auto open(std::shared_ptr<arrow::io::RandomAccessFile> file,
            Option<ScanPosition> from, nova::InstantiateCtx ctx)
    -> Task<failure_or<void>> {
    if (from) {
      remaining_ = from->remaining;
    }
    if (remaining_ == uint64_t{0}) {
      co_return {};
    }
    CO_TRY(make_filters(ctx));
    auto metadata = co_await spawn_blocking(
      [file] -> arrow::Result<std::shared_ptr<::parquet::FileMetaData>> {
        // Like the byte path, treat an empty file as an empty input.
        ARROW_ASSIGN_OR_RAISE(auto size, file->GetSize());
        if (size == 0) {
          return nullptr;
        }
        try {
          auto properties = ::parquet::ReaderProperties{arrow_memory_pool()};
          properties.set_footer_read_size(256_Ki);
          auto reader = ::parquet::ParquetFileReader::Open(file, properties);
          return reader->metadata();
        } catch (
          ::parquet::ParquetInvalidOrCorruptedFileException const& error) {
          return arrow::Status::Invalid("invalid or corrupted parquet file: ",
                                        error.what());
        } catch (::parquet::ParquetException const& error) {
          return arrow::Status::IOError(error.what());
        }
      });
    if (not metadata.ok()) {
      diagnostic::error("{}", metadata.status().message()).emit(ctx);
      co_return failure::promise();
    }
    if (not *metadata) {
      co_return {};
    }
    scan_file_ = std::move(file);
    scan_metadata_ = std::move(*metadata);
    for (auto i = 0; i < scan_metadata_->num_columns(); ++i) {
      auto const& name = scan_metadata_->schema()->GetColumnRoot(i)->name();
      if (not projection_
          or std::ranges::find(*projection_, name) != projection_->end()) {
        scan_columns_.push_back(i);
      }
    }
    // Every row group starts at the rows of the ones before, which positions
    // count even if they are skipped.
    auto start = uint64_t{0};
    for (auto i = 0; i < scan_metadata_->num_row_groups(); ++i) {
      scan_starts_.push_back(start);
      start
        += detail::narrow<uint64_t>(scan_metadata_->RowGroup(i)->num_rows());
    }
    scan_selected_ = select_row_groups(filter_, *scan_metadata_);
    if (from) {
      // Skip the row groups consumed before, and the consumed rows of the
      // row group to resume in.
      auto rows = from->rows;
      while (scan_row_group_ < scan_metadata_->num_row_groups()) {
        auto group_rows = detail::narrow<uint64_t>(
          scan_metadata_->RowGroup(scan_row_group_)->num_rows());
        if (rows < group_rows) {
          break;
        }
        rows -= group_rows;
        ++scan_row_group_;
      }
      // The skipped rows count again as they are read and dropped. Nothing
      // needs to be dropped from a row group that is skipped altogether.
      auto resumed = scan_row_group_ < scan_metadata_->num_row_groups()
                     and scan_selected_.get(scan_row_group_);
      scan_skip_ = resumed ? detail::narrow<int64_t>(rows) : 0;
      scan_consumed_ = from->rows - rows;
    }
    co_return {};
  }

  /// Returns the next nonempty batch, or None at exhaustion or after reporting
  /// an error. Rows that were read ahead are not part of the position.
  auto next(diagnostic_handler& dh) -> Task<Option<ScanBatch>> {
    while (remaining_ != uint64_t{0} and scan_file_) {
      if (not scan_batches_) {
        if (not scan_prefetch_) {
          auto group = next_row_group();
          if (not group) {
            break;
          }
          scan_prefetch_ = co_await open_row_group(*group);
        }
        auto opened = std::move(*scan_prefetch_);
        scan_prefetch_ = None{};
        if (not opened.ok()) {
          diagnostic::error("{}", opened.status().ToStringWithoutContextLines())
            .emit(dh);
          break;
        }
        scan_batches_ = std::move(*opened);
        scan_rows_ = scan_batches_->rows;
        scan_consumed_ = scan_starts_[scan_batches_->group];
        // Fetch the next row group while this one decodes, unless the limit
        // is certain to be met before.
        auto needs_next
          = not filter_.empty() or not remaining_
            or *remaining_ > static_cast<uint64_t>(scan_rows_ - scan_skip_);
        if (needs_next) {
          if (auto group = next_row_group()) {
            scan_prefetch_ = co_await open_row_group(*group);
          }
        }
        scan_next_ = co_await read_scan_batch();
      }
      if (not scan_next_.ok()) {
        diagnostic::error("{}",
                          scan_next_.status().ToStringWithoutContextLines())
          .note("failed to read record batch")
          .emit(dh);
        break;
      }
      auto batch = std::move(*scan_next_);
      if (not batch) {
        scan_batches_.reset();
        continue;
      }
      scan_rows_ -= batch->num_rows();
      scan_consumed_ += detail::narrow<uint64_t>(batch->num_rows());
      if (scan_skip_ > 0) {
        // Drop the rows that were consumed before a restart.
        auto skipped = std::min(scan_skip_, batch->num_rows());
        scan_skip_ -= skipped;
        batch = skipped == batch->num_rows() ? nullptr : batch->Slice(skipped);
      }
      if (scan_rows_ == 0
          or (batch and filter_.empty() and remaining_
              and *remaining_ <= static_cast<uint64_t>(batch->num_rows()))) {
        // Release Arrow's aliases before the importer adopts the buffers.
        scan_batches_.reset();
      } else {
        // As in the byte path, defer lookahead errors until the batch is needed.
        scan_next_ = co_await read_scan_batch();
      }
      if (not batch) {
        continue;
      }
      auto formatted = format_decimal_arrays(std::move(batch), decimal_format_);
      if (not formatted.ok()) {
        diagnostic::error("failed to format parquet decimals")
          .note("{}", formatted.status().ToStringWithoutContextLines())
          .emit(dh);
        break;
      }
      auto events = convert(inject_tenzir_metadata(std::move(*formatted)), dh);
      if (not events) {
        break;
      }
      if (events->active_count() != 0) {
        co_return ScanBatch{
          std::move(*events),
          ScanPosition{scan_consumed_, remaining_},
        };
      }
    }
    scan_prefetch_ = None{};
    scan_batches_.reset();
    scan_metadata_.reset();
    scan_file_.reset();
    co_return None{};
  }

private:
  struct ScanBatches {
    std::unique_ptr<::parquet::arrow::FileReader> reader;
    std::shared_ptr<arrow::RecordBatchReader> batches;
    int64_t rows;
    int group;
  };

  /// The next row group to read, past the ones whose statistics rule out the
  /// filter.
  auto next_row_group() -> Option<int> {
    while (scan_row_group_ < scan_metadata_->num_row_groups()) {
      auto group = scan_row_group_++;
      if (scan_selected_.get(group)) {
        return group;
      }
    }
    return None{};
  }

  /// Opens a decoder for one row group and starts fetching its selected column
  /// chunks without waiting for them.
  ///
  /// Every row group gets its own decoder and range cache, reusing the footer.
  /// Arrow's cache retains everything it fetched until destruction, so one
  /// cache for the whole file would retain the entire file.
  auto open_row_group(int group)
    -> Task<arrow::Result<std::shared_ptr<ScanBatches>>> {
    co_return co_await spawn_blocking(
      [file = scan_file_, metadata = scan_metadata_, group,
       columns
       = scan_columns_]() -> arrow::Result<std::shared_ptr<ScanBatches>> {
        try {
          auto properties = ::parquet::ReaderProperties{arrow_memory_pool()};
          properties.enable_buffered_stream();
          auto arrow_properties = ::parquet::ArrowReaderProperties{};
          arrow_properties.set_batch_size(defaults::import::table_slice_size);
          arrow_properties.set_pre_buffer(true);
          // The lazy default defers fetching until the first read.
          arrow_properties.set_cache_options(
            arrow::io::CacheOptions::Defaults());
          auto rows = metadata->RowGroup(group)->num_rows();
          auto parquet
            = ::parquet::ParquetFileReader::Open(file, properties, metadata);
          ARROW_ASSIGN_OR_RAISE(
            auto reader, ::parquet::arrow::FileReader::Make(arrow_memory_pool(),
                                                            std::move(parquet),
                                                            arrow_properties));
          ARROW_ASSIGN_OR_RAISE(auto batches,
                                reader->GetRecordBatchReader({group}, columns));
          return std::make_shared<ScanBatches>(std::move(reader),
                                               std::move(batches), rows, group);
        } catch (::parquet::ParquetException const& error) {
          return arrow::Status::Invalid(error.what());
        }
      });
  }

  auto read_scan_batch()
    -> Task<arrow::Result<std::shared_ptr<arrow::RecordBatch>>> {
    co_return co_await spawn_blocking(
      [state
       = scan_batches_] -> arrow::Result<std::shared_ptr<arrow::RecordBatch>> {
        try {
          return state->batches->Next();
        } catch (::parquet::ParquetException const& error) {
          return arrow::Status::Invalid(error.what());
        }
      });
  }

  std::shared_ptr<arrow::io::RandomAccessFile> scan_file_;
  std::shared_ptr<::parquet::FileMetaData> scan_metadata_;
  std::vector<int> scan_columns_;
  /// The first row of every row group within the file.
  std::vector<uint64_t> scan_starts_;
  /// Whether a row group may contain rows that satisfy the filter.
  nova::storage::BitMap scan_selected_{0, true};
  int scan_row_group_ = 0;
  int64_t scan_rows_ = 0;
  /// Rows to drop from the start of the next row group when resuming.
  int64_t scan_skip_ = 0;
  /// Rows of the file consumed through the last decoded batch.
  uint64_t scan_consumed_ = 0;
  std::shared_ptr<ScanBatches> scan_batches_;
  /// The next row group, whose column chunks are being fetched.
  Option<arrow::Result<std::shared_ptr<ScanBatches>>> scan_prefetch_;
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> scan_next_{
    std::shared_ptr<arrow::RecordBatch>{}};
};

/// The identity of a file, to recognize it after a restore.
struct FileIdentity {
  std::string path;
  Option<time> mtime;
  int64_t size = 0;

  friend auto operator==(FileIdentity const&, FileIdentity const&) -> bool
    = default;

  friend auto inspect(auto& f, FileIdentity& x) -> bool {
    return f.object(x).fields(f.field("path", x.path),
                              f.field("mtime", x.mtime),
                              f.field("size", x.size));
  }
};

/// Reads a Parquet file that a file source hands over as a whole, through
/// range requests instead of buffering it.
///
/// The scan runs between executor calls, one batch per `await_task()`, so
/// checkpoints can happen between batches. Ownership of the scan moves with
/// each step: `pending_` holds it until `await_task()` takes it to read the
/// next batch, and `process_task()` puts it back after forwarding the batch.
class ReadParquetFile final : public Operator<FileHandle, nova::Events> {
public:
  explicit ReadParquetFile(ReadParquetArgs args) : args_{std::move(args)} {
  }

  auto process(FileHandle input, Push<nova::Events>&, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_ASSERT(not received_, "expected a single file");
    received_ = true;
    auto identity = FileIdentity{input.path, input.mtime, input.size};
    if (file_ and *file_ != identity) {
      // The position after a restore only applies to the same file.
      diagnostic::error("file `{}` was modified since the last checkpoint",
                        input.path)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    file_ = std::move(identity);
    if (done_) {
      co_return;
    }
    auto scan = Box<ParquetScan>{std::in_place, std::move(args_)};
    auto opened
      = co_await scan->open(std::move(input.file), position_,
                            nova::InstantiateCtx{ctx.dh(), ctx.reg()});
    if (not opened) {
      done_ = true;
      co_return;
    }
    co_await pending_->enqueue(std::move(scan));
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    auto scan = co_await pending_->dequeue();
    auto batch = co_await scan->next(dh);
    co_return Step{std::move(scan), std::move(batch)};
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    auto step = std::move(result).as<Step>();
    if (not step.batch) {
      done_ = true;
      co_return;
    }
    co_await push(std::move(step.batch->events));
    position_ = step.batch->position;
    if (position_->remaining == uint64_t{0}) {
      done_ = true;
      co_return;
    }
    co_await pending_->enqueue(std::move(step.scan));
  }

  auto finalize(Push<nova::Events>&, OpCtx&)
    -> Task<FinalizeBehavior> override {
    // The file source closes the input right after handing over the file, but
    // the scan only starts then.
    co_return received_ and not done_ ? FinalizeBehavior::continue_
                                      : FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("file", file_);
    serde("position", position_);
    serde("done", done_);
  }

private:
  struct Step {
    Box<ParquetScan> scan;
    Option<ScanBatch> batch;
  };

  ReadParquetArgs args_;
  /// The file being read, persisted to validate the file after a restore.
  Option<FileIdentity> file_;
  /// The position after the last forwarded batch.
  Option<ScanPosition> position_;
  bool done_ = false;
  bool received_ = false;
  mutable Box<BoundedQueue<Box<ParquetScan>>> pending_{std::in_place, 1};
};

// Preserve the pre-Nova reader independently so it can be deleted as a unit.
namespace legacy {

auto format_decimal_type(std::shared_ptr<arrow::DataType> type,
                         decimal_format format)
  -> std::shared_ptr<arrow::DataType> {
  switch (type->id()) {
    case arrow::Type::DECIMAL128:
      return format == decimal_format::string ? arrow::utf8()
                                              : arrow::float64();
    case arrow::Type::STRUCT: {
      auto fields = type->fields();
      auto changed = false;
      for (auto& field : fields) {
        auto field_type = format_decimal_type(field->type(), format);
        changed |= field_type != field->type();
        field = field->WithType(std::move(field_type));
      }
      return changed ? arrow::struct_(std::move(fields)) : std::move(type);
    }
    case arrow::Type::LIST: {
      auto list_type = std::static_pointer_cast<arrow::ListType>(type);
      auto value_type = format_decimal_type(list_type->value_type(), format);
      if (value_type == list_type->value_type()) {
        return type;
      }
      return arrow::list(
        list_type->value_field()->WithType(std::move(value_type)));
    }
    case arrow::Type::MAP: {
      if (format == decimal_format::float_) {
        return type;
      }
      auto map_type = std::static_pointer_cast<arrow::MapType>(type);
      auto key_type = format_decimal_type(map_type->key_type(), format);
      auto item_type = format_decimal_type(map_type->item_type(), format);
      if (key_type == map_type->key_type()
          and item_type == map_type->item_type()) {
        return type;
      }
      return std::make_shared<arrow::MapType>(
        map_type->key_field()->WithType(std::move(key_type)),
        map_type->item_field()->WithType(std::move(item_type)),
        map_type->keys_sorted());
    }
    default:
      return type;
  }
}

auto format_decimal_arrays(std::shared_ptr<arrow::RecordBatch> batch,
                           decimal_format format)
  -> arrow::Result<std::shared_ptr<arrow::RecordBatch>> {
  auto arrays = arrow::ArrayVector{};
  auto fields = arrow::FieldVector{};
  auto changed = false;
  arrays.reserve(batch->num_columns());
  fields.reserve(batch->num_columns());
  for (auto index = 0; index < batch->num_columns(); ++index) {
    auto array = batch->column(index);
    auto target_type = format_decimal_type(array->type(), format);
    if (target_type != array->type()) {
      ARROW_ASSIGN_OR_RAISE(
        auto result, arrow::compute::Cast(array, std::move(target_type)));
      array = result.make_array();
      changed = true;
    }
    arrays.push_back(array);
    fields.push_back(batch->schema()->field(index)->WithType(array->type()));
  }
  if (not changed) {
    return batch;
  }
  auto schema = std::make_shared<arrow::Schema>(std::move(fields),
                                                batch->schema()->endianness(),
                                                batch->schema()->metadata());
  return arrow::RecordBatch::Make(std::move(schema), batch->num_rows(),
                                  std::move(arrays));
}

auto inject_tenzir_metadata(std::shared_ptr<arrow::RecordBatch> batch)
  -> std::shared_ptr<arrow::RecordBatch> {
  auto needs_name = true;
  auto needs_stripping = false;
  auto metadata = batch->schema()->metadata();
  auto keys = metadata ? metadata->keys() : std::vector<std::string>{};
  auto values = metadata ? metadata->values() : std::vector<std::string>{};
  for (const auto& key : keys) {
    if (key == "TENZIR:name:0") {
      needs_name = false;
      continue;
    }
    if (not key.starts_with("TENZIR:")) {
      needs_stripping = true;
    }
  }
  if (not needs_name and not needs_stripping) {
    return batch;
  }
  if (needs_stripping) {
    auto kit = keys.begin();
    auto vit = values.begin();
    while (kit != keys.end()) {
      if (not kit->starts_with("TENZIR:")) {
        vit = values.erase(vit);
        kit = keys.erase(kit);
        continue;
      }
      ++kit;
      ++vit;
    }
  }
  if (needs_name) {
    keys.emplace_back("TENZIR:name:0");
    values.emplace_back("tenzir.parquet");
  }
  TENZIR_ASSERT(keys.size() == values.size());
  return batch->ReplaceSchemaMetadata(
    arrow::key_value_metadata(std::move(keys), std::move(values)));
}

class ReadParquet final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadParquet(ReadParquetArgs args)
    : decimal_format_{args.decimal_format ? from_string<decimal_format>(
                                              args.decimal_format->inner)
                                              .value_or(decimal_format::string)
                                          : decimal_format::string},
      filter_{std::move(args.optimization.filter)},
      remaining_{args.optimization.limit},
      projection_{
        read_projection(std::move(args.optimization.projection), filter_)} {
  }

  auto process(chunk_ptr input, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    // NOTE: The parquet format stores key decoding metadata in the file
    // footer. With plain streaming bytes, we cannot decode row groups before
    // seeing the footer, so we buffer and parse in `finalize()`. This also
    // means checkpointing is currently unsupported: restoring would require
    // persisting potentially huge buffered input and parser progress.
    //
    // This operator is the streaming fallback and must keep working behind
    // any byte source, including non-seekable ones such as `decompress_gzip`
    // or `load_tcp`. Avoiding the whole-file buffer for seekable sources is
    // planned as a separate random-access scan that the planner substitutes
    // for eligible `from_file { read_parquet }` compositions (TNZ-1034). The
    // pushed-down filter, limit, and projection below are the inputs that scan
    // consumes as well; keep them reader-agnostic.
    if (remaining_ == uint64_t{0} or not input or input->size() == 0) {
      co_return;
    }
    chunks_.push_back(std::move(input));
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (remaining_ == uint64_t{0}) {
      co_return FinalizeBehavior::done;
    }
    auto parquet_chunk = join_chunks(std::move(chunks_));
    if (parquet_chunk->size() == 0) {
      co_return FinalizeBehavior::done;
    }
    auto input_file = as_arrow_file(std::move(parquet_chunk));
    auto parquet_reader_properties
      = ::parquet::ReaderProperties(arrow_memory_pool());
    parquet_reader_properties.enable_buffered_stream();
    auto arrow_reader_properties = ::parquet::ArrowReaderProperties();
    arrow_reader_properties.set_batch_size(defaults::import::table_slice_size);
    // The input already is an in-memory buffer. Pre-buffering would coalesce
    // and copy the selected column chunks a second time without any I/O win.
    arrow_reader_properties.set_pre_buffer(false);
    std::unique_ptr<::parquet::arrow::FileReader> out_buffer;
    try {
      auto input_buffer = ::parquet::ParquetFileReader::Open(
        std::move(input_file), parquet_reader_properties);
      auto out_buffer_result = ::parquet::arrow::FileReader::Make(
        arrow_memory_pool(), std::move(input_buffer), arrow_reader_properties);
      if (not out_buffer_result.ok()) {
        diagnostic::error(
          "{}", out_buffer_result.status().ToStringWithoutContextLines())
          .emit(ctx);
        co_return FinalizeBehavior::done;
      }
      out_buffer = std::move(out_buffer_result).MoveValueUnsafe();
    } catch (const ::parquet::ParquetInvalidOrCorruptedFileException& err) {
      diagnostic::error("invalid or corrupted parquet file: {}", err.what())
        .emit(ctx);
      co_return FinalizeBehavior::done;
    }
    auto metadata = out_buffer->parquet_reader()->metadata();
    auto columns = std::vector<int>{};
    for (auto i = 0; i < metadata->num_columns(); ++i) {
      auto const& name = metadata->schema()->GetColumnRoot(i)->name();
      if (not projection_
          or std::ranges::find(*projection_, name) != projection_->end()) {
        columns.push_back(i);
      }
    }
    // Keep cardinality and missing-field diagnostics when no field matches.
    if (columns.empty()) {
      for (auto i = 0; i < metadata->num_columns(); ++i) {
        columns.push_back(i);
      }
    }
    auto row_groups = std::vector<int>{};
    auto available = uint64_t{0};
    for (auto i = 0; i < metadata->num_row_groups(); ++i) {
      row_groups.push_back(i);
      available += metadata->RowGroup(i)->num_rows();
      if (filter_.empty() and remaining_ and available >= *remaining_) {
        break;
      }
    }
    auto rb_reader = out_buffer->GetRecordBatchReader(row_groups, columns);
    if (not rb_reader.ok()) {
      diagnostic::error("{}", rb_reader.status().ToStringWithoutContextLines())
        .note("failed create record batches from input data")
        .emit(ctx);
      co_return FinalizeBehavior::done;
    }
    for (auto maybe_batch : **rb_reader) {
      if (not maybe_batch.ok()) {
        diagnostic::error("{}",
                          maybe_batch.status().ToStringWithoutContextLines())
          .note("failed read record batch")
          .emit(ctx);
        co_return FinalizeBehavior::done;
      }
      auto batch = maybe_batch.MoveValueUnsafe();
      auto formatted_batch
        = format_decimal_arrays(std::move(batch), decimal_format_);
      if (not formatted_batch.ok()) {
        diagnostic::error("failed to format parquet decimals")
          .note("{}", formatted_batch.status().ToStringWithoutContextLines())
          .emit(ctx);
        co_return FinalizeBehavior::done;
      }
      batch = std::move(formatted_batch).MoveValueUnsafe();
      /// We need to perform some cleanup, in case the parquet files were not
      /// written by us. Specifically we need to ensure that the slice has a
      /// name and that only metadata that are tenzir attributes exist.
      batch = inject_tenzir_metadata(std::move(batch));
      auto maybe_slice = table_slice::try_from(batch);
      if (not maybe_slice) {
        diagnostic::error("parquet file contains unsupported types")
          .note("{}", maybe_slice.error().message)
          .emit(ctx);
        co_return FinalizeBehavior::done;
      }
      auto slice = apply_read_pushdown(std::move(*maybe_slice), filter_,
                                       remaining_, ctx.dh());
      if (slice.rows() != 0) {
        co_await push(std::move(slice));
      }
      if (remaining_ == uint64_t{0}) {
        break;
      }
    }
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return remaining_ == uint64_t{0} ? OperatorState::done
                                     : OperatorState::normal;
  }

  auto snapshot(Serde&) -> void override {
    // Checkpointing this operator would require persisting the buffered parquet
    // bytes until the footer arrives, which can be arbitrarily large. Until we
    // have a seekable parquet path, we fail checkpoints explicitly.
    diagnostic::error("read_parquet does not support checkpoints yet").throw_();
  }

private:
  decimal_format decimal_format_ = decimal_format::string;
  std::vector<chunk_ptr> chunks_;
  ir::OptimizeFilter filter_;
  Option<uint64_t> remaining_;
  Option<std::vector<std::string>> projection_;
};

} // namespace legacy

class Plugin final : public virtual ReadOperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_parquet";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadParquetArgs, legacy::ReadParquet, ReadParquet,
                       ReadParquetFile>{};
    auto decimal_format_arg
      = d.named("decimal_format", &ReadParquetArgs::decimal_format);
    d.validate([decimal_format_arg](DescribeCtx& ctx) -> Empty {
      if (auto value = ctx.get(decimal_format_arg);
          value and not from_string<decimal_format>(value->inner)) {
        diagnostic::error("unsupported decimal format `{}`", value->inner)
          .primary(value->source)
          .note("supported decimal formats are `string` and `float`")
          .emit(ctx);
      }
      return {};
    });
    d.optimization(&ReadParquetArgs::optimization);
    return d.without_optimize();
  }

  auto read_detection_candidates() const
    -> std::vector<read_detection_candidate> override {
    return {
      read_detection::candidate(
        "read_parquet", read_detection::specificity::magic,
        [](read_detection_input input) {
          return read_detection::magic_prefix(input, "PAR1");
        }),
    };
  }
};

} // namespace

} // namespace tenzir::plugins::parquet

TENZIR_REGISTER_PLUGIN(tenzir::plugins::parquet::Plugin)
