//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Drives the `read_parquet` implementation for files that are handed over as a
// whole against a file that records every byte range it is asked for. Output
// equality alone cannot tell a scan from a whole-file read, so these tests look
// at the reads.

#include <tenzir/arrow_utils.hpp>
#include <tenzir/async.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/file_handle.hpp>
#include <tenzir/forwarding_file.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/materialize.hpp>
#include <tenzir/nova_flag.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/session.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/tql2/parser.hpp>

#include <arrow/api.h>
#include <arrow/extension_type.h>
#include <arrow/io/memory.h>
#include <arrow/json/from_string.h>
#include <arrow/util/future.h>
#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <folly/coro/BlockingWait.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>

#include <array>
#include <cmath>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <mutex>
#include <ranges>
#include <span>

using namespace tenzir;

namespace {

struct Range {
  int64_t offset;
  int64_t length;

  auto end() const -> int64_t {
    return offset + length;
  }

  auto overlaps(Range other) const -> bool {
    return offset < other.end() and other.offset < end();
  }
};

/// An in-memory file that records the byte ranges requested from it and the
/// buffers it handed out.
class RecordingFile final : public ForwardingFile {
public:
  explicit RecordingFile(std::shared_ptr<arrow::Buffer> buffer)
    : ForwardingFile{
        std::make_shared<arrow::io::BufferReader>(std::move(buffer))} {
  }

  auto reads() const -> std::vector<Range> {
    auto lock = std::scoped_lock{mutex_};
    return reads_;
  }

  /// Makes the file appear truncated to `size` bytes from now on. Simulates a
  /// file that shrinks after its footer was read.
  auto truncate(int64_t size) -> void {
    auto lock = std::scoped_lock{mutex_};
    truncated_ = size;
  }

  /// The number of buffers handed out by `ReadAsync` that are still alive.
  /// Data ranges are fetched asynchronously, so this counts the row group data
  /// the reader currently holds on to.
  auto live_async_buffers() const -> size_t {
    auto lock = std::scoped_lock{mutex_};
    return std::ranges::count_if(async_buffers_, [](auto const& weak) {
      return not weak.expired();
    });
  }

  auto peak_async_buffers() const -> size_t {
    auto lock = std::scoped_lock{mutex_};
    return peak_async_buffers_;
  }

  auto Read(int64_t nbytes, void* out) -> arrow::Result<int64_t> override {
    ARROW_ASSIGN_OR_RAISE(auto position, Tell());
    record(position, nbytes);
    return ForwardingFile::Read(nbytes, out);
  }

  auto Read(int64_t nbytes)
    -> arrow::Result<std::shared_ptr<arrow::Buffer>> override {
    ARROW_ASSIGN_OR_RAISE(auto position, Tell());
    record(position, nbytes);
    return ForwardingFile::Read(nbytes);
  }

  auto ReadAt(int64_t position, int64_t nbytes, void* out)
    -> arrow::Result<int64_t> override {
    record(position, nbytes);
    return ForwardingFile::ReadAt(position, nbytes, out);
  }

  auto ReadAt(int64_t position, int64_t nbytes)
    -> arrow::Result<std::shared_ptr<arrow::Buffer>> override {
    record(position, nbytes);
    return ForwardingFile::ReadAt(position, nbytes);
  }

  auto
  ReadAsync(const arrow::io::IOContext& ctx, int64_t position, int64_t nbytes)
    -> arrow::Future<std::shared_ptr<arrow::Buffer>> override {
    record(position, nbytes);
    {
      auto lock = std::scoped_lock{mutex_};
      if (truncated_) {
        nbytes = std::clamp(*truncated_ - position, int64_t{0}, nbytes);
      }
    }
    return ForwardingFile::ReadAsync(ctx, position, nbytes)
      .Then([this](std::shared_ptr<arrow::Buffer> const& buffer) {
        auto lock = std::scoped_lock{mutex_};
        async_buffers_.emplace_back(buffer);
        auto live = std::ranges::count_if(async_buffers_, [](auto const& weak) {
          return not weak.expired();
        });
        peak_async_buffers_
          = std::max(peak_async_buffers_, static_cast<size_t>(live));
        return buffer;
      });
  }

private:
  auto record(int64_t position, int64_t nbytes) -> void {
    auto lock = std::scoped_lock{mutex_};
    reads_.push_back(Range{position, nbytes});
  }

  mutable std::mutex mutex_;
  std::vector<Range> reads_;
  std::vector<std::weak_ptr<arrow::Buffer>> async_buffers_;
  Option<int64_t> truncated_;
  size_t peak_async_buffers_ = 0;
};

constexpr auto row_groups = int64_t{6};
constexpr auto rows_per_group = int64_t{100};
constexpr auto payload_size = size_t{2048};

/// A file with a narrow `id` column and a fat `payload` column, split into
/// several row groups. The file is a few times larger than the reader's
/// speculative footer read, so that read alone does not cover it.
auto make_file() -> std::shared_ptr<arrow::Buffer> {
  auto ids = arrow::Int64Builder{};
  auto payloads = arrow::StringBuilder{};
  for (auto i = int64_t{0}; i < row_groups * rows_per_group; ++i) {
    REQUIRE(ids.Append(i).ok());
    // Unique values keep the payload incompressible for the encoder.
    auto payload = std::to_string(i);
    payload.resize(payload_size, static_cast<char>('a' + i % 26));
    REQUIRE(payloads.Append(payload).ok());
  }
  auto schema = arrow::schema({
    arrow::field("id", arrow::int64()),
    arrow::field("payload", arrow::utf8()),
  });
  auto table = arrow::Table::Make(schema, {
                                            ids.Finish().ValueOrDie(),
                                            payloads.Finish().ValueOrDie(),
                                          });
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto properties = ::parquet::WriterProperties::Builder{}
                      .compression(::parquet::Compression::UNCOMPRESSED)
                      ->disable_dictionary()
                      ->build();
  REQUIRE(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                       sink, rows_per_group, properties)
            .ok());
  return sink->Finish().ValueOrDie();
}

/// The byte range of one column chunk, the way the reader computes it.
auto column_chunk(::parquet::FileMetaData const& metadata, int row_group,
                  int column) -> Range {
  auto chunk = metadata.RowGroup(row_group)->ColumnChunk(column);
  auto start = chunk->data_page_offset();
  if (chunk->has_dictionary_page() and chunk->dictionary_page_offset() > 0
      and chunk->dictionary_page_offset() < start) {
    start = chunk->dictionary_page_offset();
  }
  return Range{start, chunk->total_compressed_size()};
}

auto touches(std::vector<Range> const& reads, Range range) -> bool {
  return std::ranges::any_of(reads, [&](Range read) {
    return read.overlaps(range);
  });
}

/// The reads that fetch data, i.e., everything after the footer read. The
/// footer read speculatively covers the tail of the file, which may include
/// column chunks of the last row group regardless of what is selected.
auto data_reads(std::vector<Range> reads) -> std::vector<Range> {
  REQUIRE(not reads.empty());
  reads.erase(reads.begin());
  return reads;
}

auto total(std::vector<Range> const& reads) -> int64_t {
  auto result = int64_t{0};
  for (auto read : reads) {
    result += read.length;
  }
  return result;
}

/// Compiles a byte-input pipeline and optimizes it with the given request.
auto compile(std::string_view text, ir::OptimizeRequest request, base_ctx ctx)
  -> ir::pipeline {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ast = parse_pipeline_with_location_override(text, location::unknown,
                                                   provider.as_session());
  REQUIRE(ast);
  auto compiled = std::move(*ast).compile(compile_ctx::make_root(ctx));
  REQUIRE(compiled);
  auto instantiated = ir::instantiate(std::move(*compiled), ctx);
  REQUIRE(instantiated);
  return std::move(*instantiated).optimize(std::move(request), {}).replacement;
}

auto projection_of(std::initializer_list<std::string_view> fields)
  -> Option<ir::OptimizeProjection> {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto s = session{provider};
  auto result = Option<ir::OptimizeProjection>{ir::OptimizeProjection{}};
  for (auto field : fields) {
    auto expr
      = parse_expression_with_location_override(field, location::unknown, s);
    REQUIRE(expr);
    ir::add_to_projection(result, *ast::field_path::try_from(*expr));
  }
  return result;
}

auto projection_of(std::string_view field) -> Option<ir::OptimizeProjection> {
  return projection_of({field});
}

/// The identity of the file in the operator's checkpoint.
struct CheckpointIdentity {
  std::string path;
  Option<tenzir::time> mtime;
  int64_t size = 0;

  friend auto inspect(auto& f, CheckpointIdentity& x) -> bool {
    return f.object(x).fields(f.field("path", x.path),
                              f.field("mtime", x.mtime),
                              f.field("size", x.size));
  }
};

/// The position in the operator's checkpoint.
struct CheckpointPosition {
  uint64_t rows = 0;
  Option<uint64_t> remaining = None{};

  friend auto inspect(auto& f, CheckpointPosition& x) -> bool {
    return f.object(x).fields(f.field("rows", x.rows),
                              f.field("remaining", x.remaining));
  }
};

/// The operator's state, mirrored to write checkpoints that no read reaches by
/// itself, such as one within a row group.
struct Checkpoint {
  Option<CheckpointIdentity> file;
  Option<CheckpointPosition> position;
  bool done = false;

  auto snapshot(Serde& serde) -> void {
    serde("file", file);
    serde("position", position);
    serde("done", done);
  }
};

constexpr auto path = std::string_view{"events.parquet"};

template <class T>
auto save(T& state) -> caf::byte_buffer {
  auto buffer = caf::byte_buffer{};
  auto f = caf::binary_serializer{buffer};
  REQUIRE(f.begin_object(caf::invalid_type_id, ""));
  auto serde = Serde{f};
  state.snapshot(serde);
  REQUIRE(f.end_object());
  return buffer;
}

template <class T>
auto load(T& state, caf::byte_buffer const& buffer) -> void {
  auto f = caf::binary_deserializer{
    caf::const_byte_span{buffer.data(), buffer.size()}};
  REQUIRE(f.begin_object(caf::invalid_type_id, ""));
  auto serde = Serde{f};
  state.snapshot(serde);
  REQUIRE(f.end_object());
}

/// A checkpoint of `buffer` taken after `rows` rows, with `remaining` rows of
/// the limit left.
auto checkpoint_at(std::shared_ptr<arrow::Buffer> const& buffer, uint64_t rows,
                   Option<uint64_t> remaining = None{}) -> caf::byte_buffer {
  auto state = Checkpoint{
    .file = CheckpointIdentity{std::string{path}, None{}, buffer->size()},
    .position = CheckpointPosition{rows, remaining},
  };
  return save(state);
}

/// Just enough of an operator context for a reader that neither spawns
/// subpipelines nor tasks.
class ReaderCtx final : public OpCtx {
public:
  ReaderCtx(diagnostic_handler& dh, registry const& reg) : dh_{dh}, reg_{reg} {
  }

  auto actor_system() -> caf::actor_system& override {
    panic("unexpected call");
  }

  auto dh() -> diagnostic_handler& override {
    return dh_;
  }

  auto reg() -> registry const& override {
    return reg_;
  }

  auto resolve_secrets(std::vector<secret_request>)
    -> Task<failure_or<void>> override {
    panic("unexpected call");
  }

  auto spawn_sub(SubKey, ir::Plan, DiagnosticBehavior, bool)
    -> Task<AnySubHandle&> override {
    panic("unexpected call");
  }

  auto get_sub(SubKeyView) -> Option<AnySubHandle&> override {
    return None{};
  }

  auto io_executor() -> folly::Executor::KeepAlive<folly::IOExecutor> override {
    panic("unexpected call");
  }

  auto spawn_task(Task<void>) -> AsyncHandle<void> override {
    panic("unexpected call");
  }

  auto save_checkpoint(chunk_ptr) -> Task<void> override {
    panic("unexpected call");
  }

  auto load_checkpoint() -> Task<chunk_ptr> override {
    panic("unexpected call");
  }

  auto flush() -> Task<void> override {
    panic("unexpected call");
  }

  auto make_counter(MetricsLabel, MetricsDirection, MetricsVisibility,
                    MetricsUnit) -> MetricsCounter override {
    return {};
  }

  auto metrics_receiver() const -> metrics_receiver_actor override {
    return {};
  }

  auto is_hidden() const -> bool override {
    return true;
  }

  auto has_terminal() const -> bool override {
    return false;
  }

  auto checkpoint_settings() const
    -> Option<CheckpointSettings const&> override {
    return None{};
  }

private:
  diagnostic_handler& dh_;
  registry const& reg_;
};

class CallbackPush final : public Push<nova::Events> {
public:
  explicit CallbackPush(std::function<void(nova::Events)> f)
    : f_{std::move(f)} {
  }

  auto operator()(nova::Events events) -> Task<void> override {
    f_(std::move(events));
    co_return;
  }

private:
  std::function<void(nova::Events)> f_;
};

struct Run {
  std::shared_ptr<RecordingFile> file;
  std::vector<nova::Events> events;
  std::vector<diagnostic> diagnostics;
  /// The most row group data buffers alive at once, sampled after every batch.
  size_t max_live_buffers = 0;
  /// The byte ranges requested by the time the first batch was returned.
  std::vector<Range> reads_at_first_batch;
  /// The operator's checkpoint after every batch.
  std::vector<caf::byte_buffer> checkpoints;
};

struct RunOptions {
  /// Runs after the file was handed over, before the first batch.
  std::function<void(RecordingFile&)> after_open = {};
  /// A checkpoint to restore before the file is handed over.
  Option<caf::byte_buffer> restore = None{};
  /// The size that the handle reports, if not the actual one.
  Option<int64_t> size = None{};
  /// The reader and its arguments.
  std::string_view pipeline = "read_parquet";
};

/// Reads `buffer` through the file implementation of a `read_parquet`
/// optimized with `request`, the way the executor runs it behind a file
/// source: one handle, then the end of the input, then one step per batch.
auto run(std::shared_ptr<arrow::Buffer> buffer, ir::OptimizeRequest request,
         RunOptions options = {}) -> Run {
  // Only the new data model has an implementation for files.
  set_nova_enabled(true);
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto const& reg = provider.as_session().reg();
  auto pipe = compile(options.pipeline, std::move(request), base_ctx{dh, reg});
  REQUIRE_EQUAL(pipe.operators.size(), 1u);
  auto spawned = pipe.operators.front()->spawn(tag_v<FileHandle>);
  auto op = std::move(as<Box<Operator<FileHandle, nova::Events>>>(spawned));
  if (options.restore) {
    load(*op, *options.restore);
  }
  auto result = Run{};
  auto size = options.size.value_or(buffer->size());
  result.file = std::make_shared<RecordingFile>(std::move(buffer));
  auto ctx = ReaderCtx{dh, reg};
  auto push = CallbackPush{[&](nova::Events events) {
    if (result.events.empty()) {
      result.reads_at_first_batch = result.file->reads();
    }
    result.events.push_back(std::move(events));
    result.max_live_buffers
      = std::max(result.max_live_buffers, result.file->peak_async_buffers());
  }};
  folly::coro::blockingWait([&]() -> Task<void> {
    co_await op->start(JobId{}, ctx);
    co_await op->process(
      FileHandle{
        .file = result.file,
        .path = std::string{path},
        .mtime = None{},
        .size = size,
      },
      push, ctx);
    if (options.after_open) {
      options.after_open(*result.file);
    }
    // The file source closes the input right after handing over the file.
    auto finalized = co_await op->finalize(push, ctx);
    while (finalized == FinalizeBehavior::continue_
           and op->state() != OperatorState::done) {
      auto batches = result.events.size();
      auto step = co_await op->await_task(dh);
      co_await op->process_task(std::move(step), push, ctx);
      if (result.events.size() != batches) {
        result.checkpoints.push_back(save(*op));
      }
    }
    // Once done, the scan finalizes for good.
    CHECK_EQUAL(co_await op->finalize(push, ctx), FinalizeBehavior::done);
  }());
  result.diagnostics = std::move(dh).collect();
  return result;
}

auto total_rows(std::vector<nova::Events> const& batches) -> uint64_t {
  auto result = uint64_t{0};
  for (auto const& batch : batches) {
    result += batch.active_count();
  }
  return result;
}

auto id_filter(std::string_view text) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto s = session{provider};
  auto filter
    = parse_expression_with_location_override(text, location::unknown, s);
  REQUIRE(filter);
  return std::move(*filter);
}

} // namespace

TEST("only readers that read a file as a whole accept one") {
  set_nova_enabled(true);
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ctx = base_ctx{dh, provider.as_session().reg()};
  auto request
    = ir::OptimizeRequest{.filter = {}, .order = EventOrder::ordered};
  auto accepts_files = [&](std::string_view text) {
    auto ndh = null_diagnostic_handler{};
    auto output
      = compile(text, request, ctx).infer_type(tag_v<FileHandle>, ndh);
    return output.is_success() and output->is<nova::Events>();
  };
  CHECK(accepts_files("read_parquet"));
  CHECK(accepts_files("read_parquet | where id > 1"));
  // Wrappers pass their input through to their subpipeline.
  CHECK(accepts_files("strict { read_parquet }"));
  CHECK(accepts_files("quiet { read_parquet } | head 1"));
  // Anything in front of the reader needs the bytes.
  CHECK(not accepts_files("decompress_gzip | read_parquet"));
  CHECK(not accepts_files("strict { decompress_gzip | read_parquet }"));
  CHECK(not accepts_files("read_json"));
  CHECK(not accepts_files("read_parquet | write_json"));
}

TEST("the reader reads the footer first and yields every row") {
  auto buffer = make_file();
  auto size = buffer->size();
  auto result = run(buffer, {.filter = {}, .order = EventOrder::ordered});
  CHECK(result.diagnostics.empty());
  CHECK_EQUAL(total_rows(result.events), uint64_t{row_groups * rows_per_group});
  auto reads = result.file->reads();
  REQUIRE(not reads.empty());
  // Nothing precedes the footer, and the footer read is anchored at the end.
  CHECK_EQUAL(reads.front().end(), size);
  CHECK(reads.front().length < size);
  // Without a projection every column chunk is needed, but never the file as
  // one piece.
  for (auto read : reads) {
    CHECK(read.length < size);
  }
}

TEST("a file that shrinks after its footer was read fails cleanly") {
  auto buffer = make_file();
  auto metadata = ::parquet::ReadMetaData(
    std::make_shared<arrow::io::BufferReader>(buffer));
  // Cut the file in the middle of the second row group's first column chunk.
  auto cut = column_chunk(*metadata, 1, 0);
  auto result = run(buffer, {.filter = {}, .order = EventOrder::ordered},
                    {.after_open = [&](RecordingFile& file) {
                      file.truncate(cut.offset + cut.length / 2);
                    }});
  // The first row group is intact and decodes; the second one comes up short,
  // which surfaces as an error rather than as a read past the buffer.
  CHECK_EQUAL(total_rows(result.events), uint64_t{rows_per_group});
  REQUIRE_EQUAL(result.diagnostics.size(), 1u);
  CHECK_EQUAL(result.diagnostics.front().severity, severity::error);
  CHECK_EQUAL(result.file->live_async_buffers(), size_t{0});
}

TEST("a full read releases each row group's range cache") {
  auto buffer = make_file();
  auto result = run(buffer, {.filter = {}, .order = EventOrder::ordered});
  CHECK(result.diagnostics.empty());
  CHECK_EQUAL(total_rows(result.events), uint64_t{row_groups * rows_per_group});
  // The two column chunks of a row group are adjacent and coalesce into one
  // read, so every row group's data is one buffer. All of them were fetched...
  CHECK_EQUAL(data_reads(result.file->reads()).size(), size_t{row_groups});
  // ...but at most the current and the next row group's encoded bytes were
  // alive at a time.
  CHECK_EQUAL(result.max_live_buffers, size_t{2});
  CHECK_EQUAL(result.file->live_async_buffers(), size_t{0});
}

TEST("the next row group is fetched while the current one decodes") {
  auto buffer = make_file();
  auto metadata = ::parquet::ReadMetaData(
    std::make_shared<arrow::io::BufferReader>(buffer));
  auto result = run(buffer, {.filter = {}, .order = EventOrder::ordered});
  CHECK(result.diagnostics.empty());
  auto const& reads = result.reads_at_first_batch;
  CHECK(touches(reads, column_chunk(*metadata, 0, 0)));
  CHECK(touches(reads, column_chunk(*metadata, 1, 0)));
  CHECK(not touches(reads, column_chunk(*metadata, 2, 0)));
}

TEST("a projection skips the column chunks of unselected columns") {
  auto buffer = make_file();
  auto metadata = ::parquet::ReadMetaData(
    std::make_shared<arrow::io::BufferReader>(buffer));
  auto result = run(buffer, {.filter = {},
                             .order = EventOrder::ordered,
                             .projection = projection_of("id")});
  CHECK(result.diagnostics.empty());
  CHECK_EQUAL(total_rows(result.events), uint64_t{row_groups * rows_per_group});
  auto reads = result.file->reads();
  for (auto group = 0; group < row_groups; ++group) {
    CHECK(touches(reads, column_chunk(*metadata, group, 0)));
  }
  for (auto group = 0; group < row_groups; ++group) {
    CHECK(not touches(data_reads(reads), column_chunk(*metadata, group, 1)));
  }
  CHECK(total(reads) < buffer->size() / 2);
}

TEST("a zero limit does not even fetch the footer") {
  auto result
    = run(make_file(),
          {.filter = {}, .order = EventOrder::ordered, .limit = uint64_t{0}});
  CHECK(result.diagnostics.empty());
  CHECK(result.events.empty());
  CHECK(result.file->reads().empty());
}

TEST("an empty file yields nothing, like an empty byte stream") {
  auto result = run(std::make_shared<arrow::Buffer>(nullptr, 0),
                    {.filter = {}, .order = EventOrder::ordered});
  CHECK(result.diagnostics.empty());
  CHECK(result.events.empty());
  CHECK(result.file->reads().empty());
}

TEST("an empty projection preserves cardinality without reading column "
     "chunks") {
  auto result = run(make_file(), {.filter = {},
                                  .order = EventOrder::ordered,
                                  .projection = ir::OptimizeProjection{}});
  CHECK(result.diagnostics.empty());
  CHECK_EQUAL(total_rows(result.events), uint64_t{row_groups * rows_per_group});
  CHECK(data_reads(result.file->reads()).empty());
}

TEST("a limit stops fetching row groups once it is satisfied") {
  auto buffer = make_file();
  auto metadata = ::parquet::ReadMetaData(
    std::make_shared<arrow::io::BufferReader>(buffer));
  auto result = run(buffer, {.filter = {},
                             .order = EventOrder::ordered,
                             .limit = uint64_t{1},
                             .projection = projection_of("id")});
  CHECK(result.diagnostics.empty());
  CHECK_EQUAL(total_rows(result.events), uint64_t{1});
  auto reads = data_reads(result.file->reads());
  CHECK(touches(reads, column_chunk(*metadata, 0, 0)));
  // The reader opens one row group at a time and stops once the limit is
  // satisfied, so the later ones are never fetched.
  for (auto group = 1; group < row_groups; ++group) {
    CHECK(not touches(reads, column_chunk(*metadata, group, 0)));
  }
  for (auto group = 0; group < row_groups; ++group) {
    CHECK(not touches(reads, column_chunk(*metadata, group, 1)));
  }
}

TEST("a filter keeps reading row groups until the limit is met") {
  auto buffer = make_file();
  auto metadata = ::parquet::ReadMetaData(
    std::make_shared<arrow::io::BufferReader>(buffer));
  // Matches start in the third row group. The statistics cannot decide
  // arithmetic, so every row group remains a candidate.
  auto result = run(buffer, {.filter = {id_filter("id / 10 >= 25")},
                             .order = EventOrder::ordered,
                             .limit = uint64_t{1},
                             .projection = projection_of("id")});
  CHECK(result.diagnostics.empty());
  CHECK_EQUAL(total_rows(result.events), uint64_t{1});
  auto reads = data_reads(result.file->reads());
  for (auto group = 0; group <= 2; ++group) {
    CHECK(touches(reads, column_chunk(*metadata, group, 0)));
  }
  // The filter may reject the rest of a row group, so the next one is fetched
  // speculatively. Nothing after it is.
  for (auto group = 4; group < row_groups; ++group) {
    CHECK(not touches(reads, column_chunk(*metadata, group, 0)));
  }
  for (auto group = 0; group < row_groups; ++group) {
    CHECK(not touches(reads, column_chunk(*metadata, group, 1)));
  }
}

/// The row groups that a read fetched data from.
auto read_row_groups(Run const& result,
                     std::shared_ptr<arrow::Buffer> const& buffer)
  -> std::vector<int> {
  auto metadata = ::parquet::ReadMetaData(
    std::make_shared<arrow::io::BufferReader>(buffer));
  auto reads = data_reads(result.file->reads());
  auto groups = std::vector<int>{};
  for (auto group = 0; group < metadata->num_row_groups(); ++group) {
    if (touches(reads, column_chunk(*metadata, group, 0))) {
      groups.push_back(group);
    }
  }
  return groups;
}

/// Reads `buffer` with the filter `text`, projected to `id`.
auto run_filter(std::shared_ptr<arrow::Buffer> const& buffer,
                std::string_view text) -> Run {
  auto result = run(buffer, {.filter = {id_filter(text)},
                             .order = EventOrder::ordered,
                             .projection = projection_of("id")});
  CHECK(result.diagnostics.empty());
  return result;
}

TEST("row groups whose statistics rule out the filter are never read") {
  auto buffer = make_file();
  auto result = run_filter(buffer, "id >= 250");
  CHECK_EQUAL(total_rows(result.events), uint64_t{350});
  CHECK_EQUAL(read_row_groups(result, buffer), (std::vector{2, 3, 4, 5}));
  // A constant on the left compares the other way around.
  result = run_filter(buffer, "250 <= id");
  CHECK_EQUAL(total_rows(result.events), uint64_t{350});
  CHECK_EQUAL(read_row_groups(result, buffer), (std::vector{2, 3, 4, 5}));
  result = run_filter(buffer, "id < 150");
  CHECK_EQUAL(total_rows(result.events), uint64_t{150});
  CHECK_EQUAL(read_row_groups(result, buffer), (std::vector{0, 1}));
}

TEST("an equality reads only the row groups that may contain the value") {
  auto buffer = make_file();
  auto result = run_filter(buffer, "id == 342");
  CHECK_EQUAL(total_rows(result.events), uint64_t{1});
  CHECK_EQUAL(read_row_groups(result, buffer), (std::vector{3}));
  // No row group is all 342.
  result = run_filter(buffer, "id != 342");
  CHECK_EQUAL(total_rows(result.events), uint64_t{599});
  CHECK_EQUAL(read_row_groups(result, buffer).size(), size_t{row_groups});
}

TEST("conjunctions and disjunctions combine their operands") {
  auto buffer = make_file();
  auto result = run_filter(buffer, "id >= 150 and id < 250");
  CHECK_EQUAL(total_rows(result.events), uint64_t{100});
  CHECK_EQUAL(read_row_groups(result, buffer), (std::vector{1, 2}));
  result = run_filter(buffer, "id < 50 or id >= 550");
  CHECK_EQUAL(total_rows(result.events), uint64_t{100});
  CHECK_EQUAL(read_row_groups(result, buffer), (std::vector{0, 5}));
}

TEST("strings and nulls compare against their statistics") {
  auto buffer = make_file();
  // Every payload starts with a digit, which sorts before letters.
  auto result = run(buffer, {.filter = {id_filter("payload == \"x\"")},
                             .order = EventOrder::ordered});
  CHECK(result.diagnostics.empty());
  CHECK(result.events.empty());
  CHECK(data_reads(result.file->reads()).empty());
  // The file has no nulls.
  result = run_filter(buffer, "id == null");
  CHECK(result.events.empty());
  CHECK(data_reads(result.file->reads()).empty());
}

TEST("filters that the statistics cannot decide read every row group") {
  auto buffer = make_file();
  for (auto text : {"not (id < 250)", "id + 0 >= 250", "id >= 250.0"}) {
    auto result = run_filter(buffer, text);
    CHECK_EQUAL(total_rows(result.events), uint64_t{350});
    CHECK_EQUAL(read_row_groups(result, buffer).size(), size_t{row_groups});
  }
}

TEST("an inequality keeps float row groups with NaNs that statistics omit") {
  // The first row group holds 1 and NaN, whose statistics say min = max = 1.
  auto builder = arrow::DoubleBuilder{};
  for (auto x : {1.0, std::numeric_limits<double>::quiet_NaN(), 2.0, 2.0}) {
    REQUIRE(builder.Append(x).ok());
  }
  auto table
    = arrow::Table::Make(arrow::schema({arrow::field("x", arrow::float64())}),
                         {builder.Finish().ValueOrDie()});
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  REQUIRE(
    ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 2)
      .ok());
  auto buffer = sink->Finish().ValueOrDie();
  auto result = run(buffer, {.filter = {id_filter("x != 1.0")},
                             .order = EventOrder::ordered});
  CHECK(result.diagnostics.empty());
  CHECK_EQUAL(total_rows(result.events), uint64_t{3});
  // An equality cannot match a NaN, so it still skips.
  result = run(buffer, {.filter = {id_filter("x == 2.0")},
                        .order = EventOrder::ordered});
  CHECK(result.diagnostics.empty());
  CHECK_EQUAL(total_rows(result.events), uint64_t{2});
  CHECK_EQUAL(data_reads(result.file->reads()).size(), size_t{1});
}

/// Writes one column in row groups of `rows_per_row_group` rows, optionally
/// embedding the Arrow schema that restores the column's Arrow type.
auto write_column(std::string name, std::shared_ptr<arrow::Array> array,
                  int64_t rows_per_row_group, bool store_schema = false)
  -> std::shared_ptr<arrow::Buffer> {
  auto table = arrow::Table::Make(
    arrow::schema({arrow::field(std::move(name), array->type())}), {array});
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto arrow_properties = ::parquet::ArrowWriterProperties::Builder{};
  if (store_schema) {
    arrow_properties.store_schema();
  }
  REQUIRE(::parquet::arrow::WriteTable(
            *table, arrow::default_memory_pool(), sink, rows_per_row_group,
            ::parquet::default_writer_properties(), arrow_properties.build())
            .ok());
  return sink->Finish().ValueOrDie();
}

/// Writes `columns` in row groups of `rows_per_group` rows, embedding the
/// Arrow schema.
auto write_table(std::vector<std::shared_ptr<arrow::Field>> fields,
                 std::vector<std::shared_ptr<arrow::Array>> columns)
  -> std::shared_ptr<arrow::Buffer> {
  auto table = arrow::Table::Make(arrow::schema(std::move(fields)), columns);
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto properties = ::parquet::WriterProperties::Builder{}
                      .compression(::parquet::Compression::UNCOMPRESSED)
                      ->disable_dictionary()
                      ->build();
  REQUIRE(::parquet::arrow::WriteTable(
            *table, arrow::default_memory_pool(), sink, rows_per_group,
            properties,
            ::parquet::ArrowWriterProperties::Builder{}.store_schema()->build())
            .ok());
  return sink->Finish().ValueOrDie();
}

/// A record of a narrow `x` and a fat `payload`, in several row groups.
auto make_record() -> std::shared_ptr<arrow::Array> {
  auto xs = arrow::Int64Builder{};
  auto payloads = arrow::StringBuilder{};
  for (auto i = int64_t{0}; i < row_groups * rows_per_group; ++i) {
    REQUIRE(xs.Append(i).ok());
    auto payload = std::to_string(i);
    payload.resize(payload_size, static_cast<char>('a' + i % 26));
    REQUIRE(payloads.Append(payload).ok());
  }
  return arrow::StructArray::Make({xs.Finish().ValueOrDie(),
                                   payloads.Finish().ValueOrDie()},
                                  std::vector<std::string>{"x", "payload"})
    .ValueOrDie();
}

/// The leaf columns whose chunks a read fetched data from in any row group.
auto read_columns(Run const& result,
                  std::shared_ptr<arrow::Buffer> const& buffer)
  -> std::vector<int> {
  auto metadata = ::parquet::ReadMetaData(
    std::make_shared<arrow::io::BufferReader>(buffer));
  auto reads = data_reads(result.file->reads());
  auto columns = std::vector<int>{};
  for (auto column = 0; column < metadata->num_columns(); ++column) {
    for (auto group = 0; group < metadata->num_row_groups(); ++group) {
      if (touches(reads, column_chunk(*metadata, group, column))) {
        columns.push_back(column);
        break;
      }
    }
  }
  return columns;
}

/// Reads `buffer` with the given projection and optional filter.
auto run_projection(std::shared_ptr<arrow::Buffer> const& buffer,
                    Option<ir::OptimizeProjection> projection,
                    ir::OptimizeFilter filter = {}) -> Run {
  return run(buffer, {.filter = std::move(filter),
                      .order = EventOrder::ordered,
                      .projection = std::move(projection)});
}

TEST("a nested projection reads only the selected fields of a record") {
  auto record = make_record();
  auto buffer = write_table({arrow::field("nested", record->type())}, {record});
  auto result = run_projection(buffer, projection_of("nested.x"));
  CHECK(result.diagnostics.empty());
  CHECK_EQUAL(total_rows(result.events), uint64_t{row_groups * rows_per_group});
  CHECK_EQUAL(read_columns(result, buffer), (std::vector{0}));
  // Selecting the record selects all of its fields.
  result = run_projection(buffer, projection_of("nested"));
  CHECK_EQUAL(read_columns(result, buffer), (std::vector{0, 1}));
  // So does a field that the record lacks, which then reports it downstream.
  result = run_projection(buffer, projection_of("nested.absent"));
  CHECK_EQUAL(read_columns(result, buffer), (std::vector{0, 1}));
  // A field that the file lacks selects nothing.
  result = run_projection(buffer, projection_of("absent"));
  CHECK_EQUAL(total_rows(result.events), uint64_t{row_groups * rows_per_group});
  CHECK(read_columns(result, buffer).empty());
}

TEST("a nested projection keeps the fields that the filter needs") {
  auto record = make_record();
  auto buffer = write_table({arrow::field("nested", record->type())}, {record});
  auto result = run_projection(buffer, projection_of("nested.x"),
                               {id_filter("nested.payload != \"\"")});
  CHECK(result.diagnostics.empty());
  CHECK_EQUAL(total_rows(result.events), uint64_t{row_groups * rows_per_group});
  CHECK_EQUAL(read_columns(result, buffer), (std::vector{0, 1}));
}

TEST("records inside lists are read whole") {
  auto record = make_record();
  auto offsets = arrow::Int32Builder{};
  for (auto i = int32_t{0}; i <= row_groups * rows_per_group; ++i) {
    REQUIRE(offsets.Append(i).ok());
  }
  auto records
    = arrow::ListArray::FromArrays(*offsets.Finish().ValueOrDie(), *record)
        .ValueOrDie();
  auto buffer
    = write_table({arrow::field("records", records->type())}, {records});
  auto result = run_projection(buffer, projection_of("records.x"));
  CHECK_EQUAL(read_columns(result, buffer), (std::vector{0, 1}));
}

/// An extension type that is stored like a record, as Tenzir stores subnets.
class RecordLike final : public arrow::ExtensionType {
public:
  explicit RecordLike(std::shared_ptr<arrow::DataType> storage)
    : ExtensionType{std::move(storage)} {
  }

  auto extension_name() const -> std::string override {
    return "tenzir.test.record_like";
  }

  auto ExtensionEquals(ExtensionType const& other) const -> bool override {
    return other.extension_name() == extension_name();
  }

  auto MakeArray(std::shared_ptr<arrow::ArrayData> data) const
    -> std::shared_ptr<arrow::Array> override {
    return std::make_shared<arrow::ExtensionArray>(std::move(data));
  }

  auto Deserialize(std::shared_ptr<arrow::DataType> storage,
                   std::string const&) const
    -> arrow::Result<std::shared_ptr<arrow::DataType>> override {
    return std::make_shared<RecordLike>(std::move(storage));
  }

  auto Serialize() const -> std::string override {
    return {};
  }
};

TEST("extension types stored like records are read whole") {
  auto record = make_record();
  auto type = std::make_shared<RecordLike>(record->type());
  if (not arrow::GetExtensionType(type->extension_name())) {
    REQUIRE(arrow::RegisterExtensionType(type).ok());
  }
  auto array = std::static_pointer_cast<arrow::Array>(
    std::make_shared<arrow::ExtensionArray>(type, record));
  auto buffer = write_table({arrow::field("value", type)}, {array});
  // The import rejects the unknown type, but only after reading the chunks.
  auto result = run_projection(buffer, projection_of("value.x"));
  CHECK_EQUAL(read_columns(result, buffer), (std::vector{0, 1}));
}

/// Whether the string field `name` of `events` is a constant.
auto is_constant_string(nova::Events const& events, std::string_view name)
  -> bool {
  auto field = events.data.field(name);
  REQUIRE(field);
  auto strings = field->data.get_alternative<nova::String>();
  REQUIRE(strings);
  return is<nova::storage::ConstantStorage<std::string, std::string_view>>(
    strings->data.storage());
}

TEST("chunks with a single value import as constants") {
  // Two row groups of a constant `group` each, and a `mixed` that varies.
  auto groups = arrow::StringBuilder{};
  auto mixed = arrow::StringBuilder{};
  for (auto i = int64_t{0}; i < 2 * rows_per_group; ++i) {
    REQUIRE(groups.Append(i < rows_per_group ? "first" : "second").ok());
    REQUIRE(mixed.Append(std::to_string(i % 3)).ok());
  }
  auto table = arrow::Table::Make(
    arrow::schema({arrow::field("group", arrow::utf8()),
                   arrow::field("mixed", arrow::utf8())}),
    {groups.Finish().ValueOrDie(), mixed.Finish().ValueOrDie()});
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  // The default properties encode both columns as dictionaries.
  REQUIRE(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                       sink, rows_per_group)
            .ok());
  auto result = run(sink->Finish().ValueOrDie(),
                    {.filter = {}, .order = EventOrder::ordered});
  CHECK(result.diagnostics.empty());
  REQUIRE_EQUAL(result.events.size(), size_t{2});
  auto index = size_t{0};
  for (auto const& events : result.events) {
    CHECK(is_constant_string(events, "group"));
    CHECK(not is_constant_string(events, "mixed"));
    auto group = events.data.field("group")->data.get(0);
    CHECK_EQUAL(nova::materialize_legacy(group),
                data{index == 0 ? "first" : "second"});
    ++index;
  }
}

TEST("orderings that warn at runtime read every row group") {
  auto buffer = make_file();
  // Strings have no order. The statistics rule the filter out, but reading
  // the rows reports the comparison.
  auto result = run(buffer, {.filter = {id_filter("payload > \"x\"")},
                             .order = EventOrder::ordered});
  CHECK(result.events.empty());
  CHECK(not result.diagnostics.empty());
  CHECK_EQUAL(data_reads(result.file->reads()).size(), size_t{row_groups});
  // Nulls do not order either. The first row group holds 1 and null, the
  // second one 10 and 11.
  auto builder = arrow::Int64Builder{};
  REQUIRE(builder.Append(1).ok());
  REQUIRE(builder.AppendNull().ok());
  REQUIRE(builder.Append(10).ok());
  REQUIRE(builder.Append(11).ok());
  buffer = write_column("x", builder.Finish().ValueOrDie(), 2);
  result = run(buffer,
               {.filter = {id_filter("x > 5")}, .order = EventOrder::ordered});
  CHECK_EQUAL(total_rows(result.events), uint64_t{2});
  CHECK(not result.diagnostics.empty());
  CHECK_EQUAL(data_reads(result.file->reads()).size(), size_t{2});
}

TEST("a predicate that may warn keeps what later predicates rule out") {
  auto buffer = make_file();
  auto request = [](ir::OptimizeFilter filter) {
    return ir::OptimizeRequest{.filter = std::move(filter),
                               .order = EventOrder::ordered};
  };
  // The left operand warns for every row, so it must see every row group.
  for (auto filter : {
         ir::OptimizeFilter{id_filter("payload > \"x\" and id > 1000")},
         ir::OptimizeFilter{id_filter("payload > \"x\""), id_filter("id > "
                                                                    "1000")},
         ir::OptimizeFilter{id_filter("id > 1000 or payload > \"x\"")},
       }) {
    auto result = run(buffer, request(std::move(filter)));
    CHECK(result.events.empty());
    CHECK(not result.diagnostics.empty());
    CHECK_EQUAL(data_reads(result.file->reads()).size(), size_t{row_groups});
  }
  // The runtime never evaluates the right operand if the left one rejects
  // every row, so skipping hides nothing.
  auto result
    = run(buffer, request({id_filter("id > 1000 and payload > \"x\"")}));
  CHECK(result.events.empty());
  CHECK(result.diagnostics.empty());
  CHECK(data_reads(result.file->reads()).empty());
}

TEST("columns that the reader restores as another type are not pruned") {
  // Arrow stores durations as plain INT64 and restores them from the schema it
  // embeds, so the statistics of `d` read as the integers 1 and 2.
  auto builder = arrow::DurationBuilder{arrow::duration(arrow::TimeUnit::NANO),
                                        arrow::default_memory_pool()};
  REQUIRE(builder.Append(1).ok());
  REQUIRE(builder.Append(2).ok());
  auto durations = builder.Finish().ValueOrDie();
  auto buffer = write_column("d", durations, 2, true);
  auto result = run(buffer, {.filter = {id_filter("d == 100")},
                             .order = EventOrder::ordered});
  // Comparing a duration with an integer warns, which skipping would hide.
  CHECK(result.events.empty());
  CHECK(not result.diagnostics.empty());
  // Without the embedded schema, the column imports as integers, which the
  // statistics then describe correctly.
  buffer = write_column("d", durations, 2);
  result = run(buffer, {.filter = {id_filter("d == 100")},
                        .order = EventOrder::ordered});
  CHECK(result.events.empty());
  CHECK(result.diagnostics.empty());
}

TEST("columns that the import rejects are not pruned") {
  // Arrow restores large strings from the schema it embeds, which the import
  // rejects. Skipping the row group would hide that.
  auto builder = arrow::LargeStringBuilder{};
  REQUIRE(builder.Append("a").ok());
  REQUIRE(builder.Append("b").ok());
  auto buffer = write_column("s", builder.Finish().ValueOrDie(), 2, true);
  auto result = run(buffer, {.filter = {id_filter("s == \"x\"")},
                             .order = EventOrder::ordered});
  CHECK(result.events.empty());
  REQUIRE_EQUAL(result.diagnostics.size(), 1u);
  CHECK_EQUAL(result.diagnostics.front().severity, severity::error);
}

TEST("duplicate column paths are not pruned") {
  // The import keeps the last of duplicate fields, so `x` is 2, although the
  // statistics of the first `x` say 1.
  auto ones = arrow::Int64Builder{};
  auto twos = arrow::Int64Builder{};
  REQUIRE(ones.Append(1).ok());
  REQUIRE(twos.Append(2).ok());
  auto table = arrow::Table::Make(
    arrow::schema(
      {arrow::field("x", arrow::int64()), arrow::field("x", arrow::int64())}),
    {ones.Finish().ValueOrDie(), twos.Finish().ValueOrDie()});
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  REQUIRE(
    ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 1)
      .ok());
  auto buffer = sink->Finish().ValueOrDie();
  auto result = run(buffer, {.filter = {id_filter("x == 2")},
                             .order = EventOrder::ordered});
  CHECK_EQUAL(total_rows(result.events), uint64_t{1});
}

TEST("leaves beneath duplicate parents are not pruned") {
  // The import keeps the second `x`, so `x.a` does not exist, which warns,
  // although the statistics of the first `x.a` say 1.
  auto ones = arrow::Int64Builder{};
  auto twos = arrow::Int64Builder{};
  REQUIRE(ones.Append(1).ok());
  REQUIRE(twos.Append(2).ok());
  auto a = arrow::StructArray::Make({ones.Finish().ValueOrDie()},
                                    std::vector<std::string>{"a"})
             .ValueOrDie();
  auto b = arrow::StructArray::Make({twos.Finish().ValueOrDie()},
                                    std::vector<std::string>{"b"})
             .ValueOrDie();
  auto table = arrow::Table::Make(
    arrow::schema({arrow::field("x", a->type()), arrow::field("x", b->type())}),
    {a, b});
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  REQUIRE(
    ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 1)
      .ok());
  auto buffer = sink->Finish().ValueOrDie();
  auto result = run(buffer, {.filter = {id_filter("x.a == 2")},
                             .order = EventOrder::ordered});
  CHECK(result.events.empty());
  CHECK(not result.diagnostics.empty());
}

TEST("row groups with timestamps that nanoseconds cannot represent are read") {
  // Year 2500 in milliseconds, which exceeds nanoseconds since the epoch.
  auto builder
    = arrow::TimestampBuilder{arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"),
                              arrow::default_memory_pool()};
  REQUIRE(builder.Append(16'725'225'600'000).ok());
  auto buffer = write_column("time", builder.Finish().ValueOrDie(), 1);
  // Multiplied without a check, the statistic wraps around to 1915.
  auto result = run(buffer, {.filter = {id_filter("time > 2026-01-01")},
                             .order = EventOrder::ordered});
  // The row group is read, so the import reports the timestamp.
  CHECK(result.events.empty());
  REQUIRE_EQUAL(result.diagnostics.size(), 1u);
  CHECK_EQUAL(result.diagnostics.front().severity, severity::error);
  // With 2026 and 2500 in one row group, only the maximum overflows, but the
  // row group fails to import all the same.
  builder
    = arrow::TimestampBuilder{arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"),
                              arrow::default_memory_pool()};
  REQUIRE(builder.Append(1'767'225'600'000).ok());
  REQUIRE(builder.Append(16'725'225'600'000).ok());
  buffer = write_column("time", builder.Finish().ValueOrDie(), 2);
  result = run(buffer, {.filter = {id_filter("time < 1900-01-01")},
                        .order = EventOrder::ordered});
  CHECK(result.events.empty());
  REQUIRE_EQUAL(result.diagnostics.size(), 1u);
  CHECK_EQUAL(result.diagnostics.front().severity, severity::error);
}

TEST("timestamps compare against time constants") {
  // Three row groups of 100 events, one second apart.
  auto builder
    = arrow::TimestampBuilder{arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                              arrow::default_memory_pool()};
  auto start = int64_t{1'767'225'600'000'000}; // 2026-01-01T00:00:00Z
  for (auto i = int64_t{0}; i < 3 * rows_per_group; ++i) {
    REQUIRE(builder.Append(start + i * 1'000'000).ok());
  }
  auto table
    = arrow::Table::Make(arrow::schema({arrow::field("time", builder.type())}),
                         {builder.Finish().ValueOrDie()});
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  REQUIRE(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                       sink, rows_per_group)
            .ok());
  auto buffer = sink->Finish().ValueOrDie();
  auto result
    = run(buffer, {.filter = {id_filter("time >= 2026-01-01T00:03:20Z")},
                   .order = EventOrder::ordered});
  CHECK(result.diagnostics.empty());
  CHECK_EQUAL(total_rows(result.events), uint64_t{100});
  CHECK_EQUAL(read_row_groups(result, buffer), (std::vector{2}));
}

TEST("timestamps of seconds compare in the unit of their statistics") {
  // Parquet has no seconds, so Arrow writes milliseconds, which the
  // statistics hold, too. The embedded schema says seconds, but the reader
  // restores milliseconds.
  auto builder
    = arrow::TimestampBuilder{arrow::timestamp(arrow::TimeUnit::SECOND, "UTC"),
                              arrow::default_memory_pool()};
  auto start = int64_t{1'767'225'600}; // 2026-01-01T00:00:00Z
  for (auto i = int64_t{0}; i < 3 * rows_per_group; ++i) {
    REQUIRE(builder.Append(start + i).ok());
  }
  auto buffer
    = write_column("time", builder.Finish().ValueOrDie(), rows_per_group, true);
  auto result
    = run(buffer, {.filter = {id_filter("time >= 2026-01-01T00:03:20Z")},
                   .order = EventOrder::ordered});
  CHECK(result.diagnostics.empty());
  CHECK_EQUAL(total_rows(result.events), uint64_t{100});
  CHECK_EQUAL(read_row_groups(result, buffer), (std::vector{2}));
}

TEST("a restore resumes after the batch of its checkpoint") {
  auto buffer = make_file();
  auto metadata = ::parquet::ReadMetaData(
    std::make_shared<arrow::io::BufferReader>(buffer));
  auto request
    = ir::OptimizeRequest{.filter = {}, .order = EventOrder::ordered};
  auto full = run(buffer, request);
  CHECK(full.diagnostics.empty());
  // Every row group is one batch, followed by one checkpoint.
  REQUIRE_EQUAL(full.checkpoints.size(), size_t{row_groups});
  for (auto i = 0; i < row_groups; ++i) {
    auto resumed = run(buffer, request, {.restore = full.checkpoints[i]});
    CHECK(resumed.diagnostics.empty());
    CHECK_EQUAL(total_rows(resumed.events),
                uint64_t((row_groups - i - 1) * rows_per_group));
    // Only the footer tells that nothing is left after the last row group.
    auto reads = data_reads(resumed.file->reads());
    if (i + 1 == row_groups) {
      CHECK(reads.empty());
      continue;
    }
    for (auto group = 0; group <= i; ++group) {
      CHECK(not touches(reads, column_chunk(*metadata, group, 0)));
    }
  }
}

TEST("a restore does not read the rows that the filter dropped again") {
  auto buffer = make_file();
  auto metadata = ::parquet::ReadMetaData(
    std::make_shared<arrow::io::BufferReader>(buffer));
  // The first batch holds the matches of the third row group.
  auto request = ir::OptimizeRequest{.filter = {id_filter("id >= 250")},
                                     .order = EventOrder::ordered,
                                     .projection = projection_of("id")};
  auto full = run(buffer, request);
  CHECK(full.diagnostics.empty());
  REQUIRE(not full.checkpoints.empty());
  REQUIRE(not full.events.empty());
  CHECK_EQUAL(full.events.front().active_count(), 50);
  auto resumed = run(buffer, request, {.restore = full.checkpoints.front()});
  CHECK(resumed.diagnostics.empty());
  CHECK_EQUAL(total_rows(resumed.events), uint64_t{3 * rows_per_group});
  auto reads = data_reads(resumed.file->reads());
  for (auto group = 0; group < 3; ++group) {
    CHECK(not touches(reads, column_chunk(*metadata, group, 0)));
  }
}

TEST("a restore continues within a row group") {
  auto buffer = make_file();
  auto metadata = ::parquet::ReadMetaData(
    std::make_shared<arrow::io::BufferReader>(buffer));
  auto result = run(buffer, {.filter = {}, .order = EventOrder::ordered},
                    {.restore = checkpoint_at(buffer, rows_per_group + 50)});
  CHECK(result.diagnostics.empty());
  CHECK_EQUAL(total_rows(result.events),
              uint64_t{row_groups * rows_per_group - rows_per_group - 50});
  REQUIRE(not result.events.empty());
  CHECK_EQUAL(result.events.front().active_count(), 50);
  CHECK(not touches(data_reads(result.file->reads()),
                    column_chunk(*metadata, 0, 0)));
  // The checkpoint after the first batch lies at the end of its row group.
  REQUIRE(not result.checkpoints.empty());
  auto resumed = run(buffer, {.filter = {}, .order = EventOrder::ordered},
                     {.restore = result.checkpoints.front()});
  CHECK_EQUAL(total_rows(resumed.events),
              uint64_t{(row_groups - 2) * rows_per_group});
}

TEST("a restore keeps the remaining limit") {
  auto buffer = make_file();
  auto result
    = run(buffer,
          {.filter = {}, .order = EventOrder::ordered, .limit = uint64_t{10}},
          {.restore = checkpoint_at(buffer, rows_per_group, uint64_t{5})});
  CHECK(result.diagnostics.empty());
  CHECK_EQUAL(total_rows(result.events), uint64_t{5});
}

TEST("a restore after the limit was met reads nothing") {
  auto buffer = make_file();
  auto request = ir::OptimizeRequest{
    .filter = {}, .order = EventOrder::ordered, .limit = uint64_t{10}};
  auto full = run(buffer, request);
  CHECK_EQUAL(total_rows(full.events), uint64_t{10});
  REQUIRE_EQUAL(full.checkpoints.size(), size_t{1});
  auto resumed = run(buffer, request, {.restore = full.checkpoints.front()});
  CHECK(resumed.diagnostics.empty());
  CHECK(resumed.events.empty());
  CHECK(resumed.file->reads().empty());
}

TEST("a restore rejects a file that changed since the checkpoint") {
  auto buffer = make_file();
  auto request
    = ir::OptimizeRequest{.filter = {}, .order = EventOrder::ordered};
  auto full = run(buffer, request);
  REQUIRE(not full.checkpoints.empty());
  auto resumed
    = run(buffer, request,
          {.restore = full.checkpoints.front(), .size = buffer->size() + 1});
  CHECK(resumed.events.empty());
  CHECK(resumed.file->reads().empty());
  REQUIRE_EQUAL(resumed.diagnostics.size(), 1u);
  CHECK_EQUAL(resumed.diagnostics.front().severity, severity::error);
}

// -- columns from statistics --------------------------------------------------

namespace {

constexpr auto group_rows = 3;

/// An Arrow array from its JSON representation.
auto from_json(std::shared_ptr<arrow::DataType> type, std::string const& text)
  -> std::shared_ptr<arrow::Array> {
  auto result = arrow::json::ArrayFromJSONString(std::move(type), text);
  REQUIRE(result.ok());
  return result.MoveValueUnsafe();
}

/// The JSON of a column that holds each of `values` in one row group.
auto each(std::initializer_list<std::string_view> values) -> std::string {
  auto result = std::string{"["};
  for (auto value : values) {
    for (auto i = 0; i < group_rows; ++i) {
      if (result.size() > 1) {
        result += ',';
      }
      result += value;
    }
  }
  return result + ']';
}

/// The JSON of a column of the numbers `0` to `rows - 1`.
auto iota(int64_t rows) -> std::string {
  auto result = std::string{"["};
  for (auto i = int64_t{0}; i < rows; ++i) {
    result += fmt::format("{}{}", i == 0 ? "" : ",", i);
  }
  return result + ']';
}

/// Writes a table in row groups of `group_rows` rows, embedding the Arrow
/// schema, with or without statistics. Without statistics, the reader must
/// decode every column, so reading the other file tells what decoding yields.
auto write_groups(arrow::FieldVector fields, arrow::ArrayVector columns,
                  bool statistics = true) -> std::shared_ptr<arrow::Buffer> {
  auto table = arrow::Table::Make(arrow::schema(std::move(fields)), columns);
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto builder = ::parquet::WriterProperties::Builder{};
  builder.compression(::parquet::Compression::UNCOMPRESSED);
  if (not statistics) {
    builder.disable_statistics();
  }
  REQUIRE(::parquet::arrow::WriteTable(
            *table, arrow::default_memory_pool(), sink, group_rows,
            builder.build(),
            ::parquet::ArrowWriterProperties::Builder{}.store_schema()->build())
            .ok());
  return sink->Finish().ValueOrDie();
}

/// Every event of a read, in order.
auto rows(Run const& result) -> std::vector<data> {
  auto rows = std::vector<data>{};
  for (auto const& events : result.events) {
    for (auto i : nova::storage::true_bits(events.mask)) {
      rows.push_back(nova::materialize_legacy(events.data.get(i)));
    }
  }
  return rows;
}

/// The diagnostics of a read, one line each.
auto messages(Run const& result) -> std::vector<std::string> {
  auto messages = std::vector<std::string>{};
  for (auto const& diagnostic : result.diagnostics) {
    auto message
      = fmt::format("{}: {}", diagnostic.severity, diagnostic.message);
    for (auto const& note : diagnostic.notes) {
      message += fmt::format(" ({})", note.message);
    }
    messages.push_back(std::move(message));
  }
  return messages;
}

/// Reads a table with and without statistics, and checks that both yield the
/// same events and diagnostics. Returns the read with statistics.
auto read_both(arrow::FieldVector const& fields,
               arrow::ArrayVector const& columns,
               ir::OptimizeRequest const& request
               = {.filter = {}, .order = EventOrder::ordered},
               RunOptions const& options = {}) -> std::pair<Run, Run> {
  auto buffer = write_groups(fields, columns);
  auto result = run(buffer, request, options);
  auto decoded = run(write_groups(fields, columns, false), request, options);
  CHECK_EQUAL(rows(result), rows(decoded));
  CHECK_EQUAL(messages(result), messages(decoded));
  return {std::move(result), std::move(decoded)};
}

/// The values of a field, which may be nested, such as `a.b`.
auto values(nova::Events const& events, std::string_view path)
  -> nova::Array<nova::Data> {
  auto result = nova::Array<nova::Data>{events.data};
  while (not path.empty()) {
    auto name = path.substr(0, path.find('.'));
    path.remove_prefix(std::min(path.size(), name.size() + 1));
    auto records = result.get_alternative<nova::Record>();
    REQUIRE(records);
    auto field = records->data.field(name);
    REQUIRE(field);
    result = std::move(field->data);
  }
  return result;
}

/// Whether an array repeats a single value, or is null throughout, instead of
/// storing a value for every row.
auto repeats(nova::Array<nova::Data> const& array) -> bool {
  return match(
    array,
    [](nova::UnionArray const&) {
      return false;
    },
    []<class Tag>(nova::Array<Tag> const& values) {
      return match(values.storage(), []<class Storage>(Storage const& x) {
        // Booleans are bits, which a bitmap of a single value does not store.
        if constexpr (std::same_as<Storage, nova::storage::BitMap>) {
          return x.data().empty();
        } else {
          TENZIR_UNUSED(x);
          return std::same_as<Storage, nova::storage::NullStorage>
                 or concepts::instantiation_of<Storage,
                                               nova::storage::ConstantStorage>;
        }
      });
    });
}

/// Whether a field repeats a value in every batch of a read.
auto repeats(Run const& result, std::string_view path) -> bool {
  REQUIRE(not result.events.empty());
  return std::ranges::all_of(result.events, [&](nova::Events const& events) {
    return repeats(values(events, path));
  });
}

/// The leaf columns of a file by name.
auto column_index(std::shared_ptr<arrow::Buffer> const& buffer,
                  std::string_view path) -> int {
  auto metadata = ::parquet::ReadMetaData(
    std::make_shared<arrow::io::BufferReader>(buffer));
  for (auto i = 0; i < metadata->num_columns(); ++i) {
    if (metadata->schema()->Column(i)->path()->ToDotString() == path) {
      return i;
    }
  }
  FAIL("no column {}", path);
  return -1;
}

/// The column chunks that a read fetched, as pairs of row group and column.
auto fetched(Run const& result, std::shared_ptr<arrow::Buffer> const& buffer)
  -> std::vector<std::pair<int, int>> {
  auto metadata = ::parquet::ReadMetaData(
    std::make_shared<arrow::io::BufferReader>(buffer));
  auto reads = data_reads(result.file->reads());
  auto chunks = std::vector<std::pair<int, int>>{};
  for (auto group = 0; group < metadata->num_row_groups(); ++group) {
    for (auto column = 0; column < metadata->num_columns(); ++column) {
      if (touches(reads, column_chunk(*metadata, group, column))) {
        chunks.emplace_back(group, column);
      }
    }
  }
  return chunks;
}

/// The column chunks of `columns` in all row groups.
auto chunks_of(std::initializer_list<int> columns, int groups)
  -> std::vector<std::pair<int, int>> {
  auto chunks = std::vector<std::pair<int, int>>{};
  for (auto group = 0; group < groups; ++group) {
    for (auto column : columns) {
      chunks.emplace_back(group, column);
    }
  }
  return chunks;
}

/// Replaces bytes in the footer of a file, where it keeps the statistics.
auto patch_footer(std::shared_ptr<arrow::Buffer> const& buffer,
                  std::string_view from, std::string_view to)
  -> std::shared_ptr<arrow::Buffer> {
  REQUIRE_EQUAL(from.size(), to.size());
  auto bytes = buffer->ToString();
  // The footer precedes its length and the magic bytes.
  auto length = uint32_t{};
  std::memcpy(&length, bytes.data() + bytes.size() - 8, sizeof(length));
  auto offset = bytes.find(from, bytes.size() - 8 - length);
  REQUIRE(offset != std::string::npos);
  REQUIRE_EQUAL(bytes.find(from, offset + 1), std::string::npos);
  bytes.replace(offset, from.size(), to);
  return arrow::Buffer::FromString(std::move(bytes));
}

} // namespace

TEST("statistics provide single-valued columns of every type") {
  // Every column but `id` holds one value per row group, and a different one
  // in the next.
  auto fields = arrow::FieldVector{arrow::field("id", arrow::int64())};
  auto columns = arrow::ArrayVector{from_json(arrow::int64(), iota(9))};
  auto add = [&](std::string name, std::shared_ptr<arrow::DataType> type,
                 std::initializer_list<std::string_view> values) {
    fields.push_back(arrow::field(std::move(name), type));
    columns.push_back(from_json(std::move(type), each(values)));
  };
  add("bool", arrow::boolean(), {"true", "false", "true"});
  add("int8", arrow::int8(), {"-128", "127", "0"});
  add("int16", arrow::int16(), {"-32768", "32767", "1"});
  add("int32", arrow::int32(), {"-2147483648", "2147483647", "2"});
  add("int64", arrow::int64(),
      {"-9223372036854775808", "9223372036854775807", "3"});
  add("uint8", arrow::uint8(), {"0", "255", "4"});
  add("uint16", arrow::uint16(), {"0", "65535", "5"});
  // Parquet stores the larger ones in the bits of negative numbers.
  add("uint32", arrow::uint32(), {"0", "4294967295", "6"});
  add("uint64", arrow::uint64(), {"0", "18446744073709551615", "7"});
  add("string", arrow::utf8(), {R"("first")", R"("")", R"("third")"});
  add("blob", arrow::binary(), {R"("\u0000")", R"("b")", R"("c")"});
  auto units = std::array{
    std::pair{arrow::TimeUnit::SECOND, "s"},
    std::pair{arrow::TimeUnit::MILLI, "ms"},
    std::pair{arrow::TimeUnit::MICRO, "us"},
    std::pair{arrow::TimeUnit::NANO, "ns"},
  };
  // Parquet has no timestamps of seconds, which Arrow writes as milliseconds.
  // The reader restores milliseconds, which the statistics hold, too.
  for (auto [unit, name] : units) {
    add(fmt::format("time_{}", name), arrow::timestamp(unit, "UTC"),
        {"-1", "0", "1767225600"});
  }
  // Arrow stores durations as integers and restores them from its schema.
  for (auto [unit, name] : units) {
    add(fmt::format("duration_{}", name), arrow::duration(unit),
        {"-1", "0", "86400"});
  }
  add("decimal", arrow::decimal128(10, 2),
      {R"("1.50")", R"("-2.25")", R"("0.00")"});
  auto buffer = write_groups(fields, columns);
  for (auto pipeline :
       {"read_parquet", R"(read_parquet decimal_format="float")"}) {
    auto [result, decoded]
      = read_both(fields, columns, {.filter = {}, .order = EventOrder::ordered},
                  {.pipeline = pipeline});
    REQUIRE_EQUAL(result.events.size(), size_t{3});
    auto events = rows(result);
    CHECK_EQUAL(as<record>(events[0]).at("time_s"),
                data{tenzir::time{std::chrono::seconds{-1}}});
    CHECK_EQUAL(as<record>(events[6]).at("time_s"),
                data{tenzir::time{std::chrono::seconds{1'767'225'600}}});
    CHECK(not repeats(result, "id"));
    for (auto const& field : fields | std::views::drop(1)) {
      CHECK(repeats(result, field->name()));
      CHECK(not repeats(decoded, field->name()));
    }
    // Only `id` is fetched.
    result = run(buffer, {.filter = {}, .order = EventOrder::ordered},
                 {.pipeline = pipeline});
    CHECK_EQUAL(fetched(result, buffer), chunks_of({0}, 3));
  }
  // A constant outside of the projection is not read at all.
  auto result = run(buffer, {.filter = {},
                             .order = EventOrder::ordered,
                             .projection = projection_of("uint32")});
  CHECK(result.diagnostics.empty());
  CHECK(data_reads(result.file->reads()).empty());
  REQUIRE_EQUAL(rows(result).size(), size_t{9});
  CHECK_EQUAL(rows(result)[3],
              (data{record{{"uint32", uint64_t{4294967295}}}}));
}

TEST("statistics provide chunks that are null throughout") {
  auto fields = arrow::FieldVector{
    arrow::field("id", arrow::int64()),
    // Nulls within a row group, which need decoding.
    arrow::field("sparse", arrow::int64()),
  };
  auto columns = arrow::ArrayVector{
    from_json(arrow::int64(), iota(9)),
    from_json(arrow::int64(), "[null,1,2,3,null,5,6,7,null]"),
  };
  auto types = arrow::DataTypeVector{
    arrow::int64(),
    arrow::float64(),
    arrow::boolean(),
    arrow::utf8(),
    arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"),
    arrow::duration(arrow::TimeUnit::SECOND),
    arrow::decimal128(10, 2),
  };
  for (auto const& type : types) {
    fields.push_back(arrow::field(fmt::format("null_{}", fields.size()), type));
    columns.push_back(from_json(type, each({"null", "null", "null"})));
  }
  // Null in the first and last row group, a value in the one between.
  fields.push_back(arrow::field("gaps", arrow::int64()));
  columns.push_back(from_json(arrow::int64(), each({"null", "1", "null"})));
  auto [result, decoded] = read_both(fields, columns);
  CHECK(result.diagnostics.empty());
  auto buffer = write_groups(fields, columns);
  result = run(buffer, {.filter = {}, .order = EventOrder::ordered});
  CHECK_EQUAL(fetched(result, buffer), chunks_of({0, 1}, 3));
}

TEST("floats with equal bounds are decoded") {
  // Statistics leave out NaNs, so they record 1 as both bounds of the first
  // row group.
  auto nan = std::numeric_limits<double>::quiet_NaN();
  auto doubles = arrow::DoubleBuilder{};
  auto floats = arrow::FloatBuilder{};
  for (auto x : {1.0, nan, 1.0, 2.0, 2.0, 2.0}) {
    REQUIRE(doubles.Append(x).ok());
    REQUIRE(floats.Append(static_cast<float>(x)).ok());
  }
  auto fields = arrow::FieldVector{
    arrow::field("double", arrow::float64()),
    arrow::field("float", arrow::float32()),
    arrow::field("int", arrow::int64()),
  };
  auto columns = arrow::ArrayVector{
    doubles.Finish().ValueOrDie(),
    floats.Finish().ValueOrDie(),
    from_json(arrow::int64(), each({"1", "2"})),
  };
  auto buffer = write_groups(fields, columns);
  auto result = run(buffer, {.filter = {}, .order = EventOrder::ordered});
  CHECK(result.diagnostics.empty());
  CHECK_EQUAL(fetched(result, buffer), chunks_of({0, 1}, 2));
  CHECK(repeats(result, "int"));
  auto events = rows(result);
  REQUIRE_EQUAL(events.size(), size_t{6});
  for (auto name : {"double", "float"}) {
    auto value = as<record>(events[1]).at(name);
    CHECK(std::isnan(as<double>(value)));
    CHECK_EQUAL(as<record>(events[2]).at(name), data{1.0});
  }
}

TEST("inexact bounds are decoded") {
  // In the first row group, flag the bounds as inexact, and make them equal,
  // as a writer that truncates `x-1` and `x-2` to `x-` might.
  auto fields = arrow::FieldVector{
    arrow::field("id", arrow::int64()),
    arrow::field("string", arrow::utf8()),
  };
  auto columns = arrow::ArrayVector{
    from_json(arrow::int64(), iota(6)),
    from_json(arrow::utf8(), R"(["x-1","x-2","x-1","y","y","y"])"),
  };
  // The statistics of a string column hold the maximum, then the minimum, then
  // whether each is exact.
  auto buffer
    = patch_footer(write_groups(fields, columns), "\x03x-2\x18\x03x-1\x11\x11",
                   "\x03x-1\x18\x03x-1\x12\x12");
  auto metadata = ::parquet::ReadMetaData(
    std::make_shared<arrow::io::BufferReader>(buffer));
  auto stats = metadata->RowGroup(0)->ColumnChunk(1)->statistics();
  REQUIRE(stats);
  REQUIRE_EQUAL(stats->EncodeMin(), stats->EncodeMax());
  REQUIRE(stats->is_min_value_exact() == false);
  REQUIRE(stats->is_max_value_exact() == false);
  auto result = run(buffer, {.filter = {}, .order = EventOrder::ordered});
  CHECK(result.diagnostics.empty());
  auto decoded = run(write_groups(fields, columns, false),
                     {.filter = {}, .order = EventOrder::ordered});
  CHECK_EQUAL(rows(result), rows(decoded));
  CHECK_EQUAL(fetched(result, buffer),
              (std::vector<std::pair<int, int>>{{0, 0}, {0, 1}, {1, 0}}));
}

TEST("timestamps that nanoseconds cannot represent are decoded") {
  // The first row group holds 2026-01-01 and the second one the year 2500,
  // which exceeds nanoseconds since the epoch.
  auto fields = arrow::FieldVector{
    arrow::field("id", arrow::int64()),
    arrow::field("time", arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")),
    arrow::field("duration", arrow::duration(arrow::TimeUnit::SECOND)),
  };
  for (auto const& values : {
         std::pair{each({"1767225600000", "16725225600000"}), each({"1", "1"})},
         std::pair{each({"1767225600000", "1767225600000"}),
                   each({"1", "10000000000000"})},
       }) {
    auto columns = arrow::ArrayVector{
      from_json(arrow::int64(), iota(6)),
      from_json(fields[1]->type(), values.first),
      from_json(fields[2]->type(), values.second),
    };
    auto [result, decoded] = read_both(fields, columns);
    // The import reports the second row group, as it did before.
    CHECK_EQUAL(rows(result).size(), size_t{3});
    REQUIRE_EQUAL(result.diagnostics.size(), 1u);
    CHECK_EQUAL(result.diagnostics.front().severity, severity::error);
    CHECK(repeats(result, "time"));
    CHECK(repeats(result, "duration"));
  }
}

TEST("statistics provide the types that the reader restores") {
  auto fields = arrow::FieldVector{arrow::field("id", arrow::int64())};
  auto columns = arrow::ArrayVector{from_json(arrow::int64(), iota(6))};
  auto add = [&](std::string name, std::shared_ptr<arrow::Array> array) {
    fields.push_back(arrow::field(std::move(name), array->type()));
    columns.push_back(std::move(array));
  };
  auto ips = ip_type::make_arrow_builder(arrow::default_memory_pool());
  auto subnets = subnet_type::make_arrow_builder(arrow::default_memory_pool());
  auto enumeration = enumeration_type{{"first"}, {"second"}};
  auto enumerations
    = enumeration.make_arrow_builder(arrow::default_memory_pool());
  auto bytes = std::array<uint8_t, 16>{};
  bytes.back() = 1;
  auto loopback = ip::v6(std::span{bytes});
  for (auto i = 0; i < 2 * group_rows; ++i) {
    auto first = i < group_rows;
    auto address = first ? ip::v4(0x0a000001) : loopback;
    REQUIRE(append_builder(ip_type{}, *ips, address).ok());
    REQUIRE(
      append_builder(subnet_type{}, *subnets,
                     subnet{address, static_cast<uint8_t>(first ? 104 : 128)})
        .ok());
    REQUIRE(enumerations->Append(first ? 0 : 1).ok());
  }
  add("ip", ips->Finish().ValueOrDie());
  add("subnet", subnets->Finish().ValueOrDie());
  add("enumeration", enumerations->Finish().ValueOrDie());
  auto dictionary = arrow::StringDictionaryBuilder{};
  for (auto value : {"a", "a", "a", "b", "b", "b"}) {
    REQUIRE(dictionary.Append(value).ok());
  }
  add("dictionary", dictionary.Finish().ValueOrDie());
  add("zoned",
      from_json(arrow::timestamp(arrow::TimeUnit::NANO, "Europe/Berlin"),
                each({"0", "1"})));
  auto [result, decoded] = read_both(fields, columns);
  CHECK(result.diagnostics.empty());
  for (auto const& field : fields | std::views::drop(1)) {
    CHECK(repeats(result, field->name()));
  }
  auto events = rows(result);
  REQUIRE_EQUAL(events.size(), size_t{6});
  CHECK_EQUAL(as<record>(events[0]).at("ip"), data{ip::v4(0x0a000001)});
  CHECK_EQUAL(as<record>(events[3]).at("subnet"),
              (data{subnet{loopback, 128}}));
  CHECK_EQUAL(as<record>(events[3]).at("enumeration"), data{"second"});
  auto buffer = write_groups(fields, columns);
  result = run(buffer, {.filter = {}, .order = EventOrder::ordered});
  CHECK_EQUAL(fetched(result, buffer), chunks_of({0}, 2));
}

TEST("records mix fields from statistics with decoded ones") {
  // A record whose constants sit between decoded fields, and a record with
  // nulls, whose leaves then have nulls, too, except the one that has nothing
  // but nulls.
  auto inner = arrow::struct_({
    arrow::field("e", arrow::int32()),
    arrow::field("f", arrow::int64()),
  });
  auto outer = arrow::struct_({
    arrow::field("a", arrow::int64()),
    arrow::field("b", arrow::int64()),
    arrow::field("c", arrow::utf8()),
    arrow::field("d", inner),
    arrow::field("g", arrow::uint8()),
  });
  auto nullable = arrow::struct_({
    arrow::field("x", arrow::int64()),
    arrow::field("z", arrow::int64()),
  });
  auto fields = arrow::FieldVector{
    arrow::field("r", outer),
    arrow::field("s", nullable),
  };
  auto columns = arrow::ArrayVector{
    from_json(outer, R"([
      {"a": 0, "b": 1, "c": null, "d": {"e": 2, "f": 0}, "g": 3},
      {"a": 1, "b": 1, "c": null, "d": {"e": 2, "f": 1}, "g": 3},
      {"a": 2, "b": 1, "c": null, "d": {"e": 2, "f": 2}, "g": 3},
      {"a": 3, "b": 4, "c": null, "d": {"e": 5, "f": 3}, "g": 6},
      {"a": 4, "b": 4, "c": null, "d": {"e": 5, "f": 4}, "g": 6},
      {"a": 5, "b": 4, "c": null, "d": {"e": 5, "f": 5}, "g": 6}
    ])"),
    from_json(nullable, R"([
      {"x": 0, "z": null}, null, {"x": 2, "z": null},
      null, {"x": 4, "z": null}, {"x": 5, "z": null}
    ])"),
  };
  auto [result, decoded] = read_both(fields, columns);
  CHECK(result.diagnostics.empty());
  for (auto path : {"r.b", "r.d.e", "r.g"}) {
    CHECK(repeats(result, path));
  }
  for (auto path : {"r.a", "r.d.f"}) {
    CHECK(not repeats(result, path));
  }
  // The fields keep their order, which the comparison with the decoded file
  // checks, too.
  auto events = rows(result);
  REQUIRE_EQUAL(events.size(), size_t{6});
  auto names = std::vector<std::string>{};
  for (auto const& [name, _] : as<record>(as<record>(events[0]).at("r"))) {
    names.push_back(name);
  }
  CHECK_EQUAL(names, (std::vector<std::string>{"a", "b", "c", "d", "g"}));
  CHECK_EQUAL(as<record>(events[1]).at("s"), data{});
  // A restore within a row group slices the records.
  auto buffer = write_groups(fields, columns);
  auto resumed = run(buffer, {.filter = {}, .order = EventOrder::ordered},
                     {.restore = checkpoint_at(buffer, 1)});
  CHECK(resumed.diagnostics.empty());
  auto expected = rows(decoded);
  expected.erase(expected.begin());
  CHECK_EQUAL(rows(resumed), expected);
  CHECK(repeats(resumed, "r.b"));
}

TEST("records whose leaves all come from statistics") {
  // `n` has nulls, and so does its only field in the other rows, which the
  // statistics cannot tell apart. `m` has no nulls, and `p` no nulls anywhere.
  auto leaf = arrow::struct_({arrow::field("z", arrow::int64())});
  auto nested = arrow::struct_({
    arrow::field("q", arrow::int64()),
    arrow::field("r", arrow::struct_({
                        arrow::field("s", arrow::utf8()),
                        arrow::field("t", arrow::int64()),
                      })),
  });
  auto fields = arrow::FieldVector{
    arrow::field("id", arrow::int64()),
    arrow::field("n", leaf),
    arrow::field("p", nested),
    arrow::field("m", leaf, /*nullable=*/false),
  };
  auto columns = arrow::ArrayVector{
    from_json(arrow::int64(), iota(6)),
    from_json(leaf, R"([{"z": null}, null, {"z": null}, null, null, null])"),
    from_json(nested, R"([
      {"q": 1, "r": {"s": "x", "t": null}},
      {"q": 1, "r": {"s": "x", "t": null}},
      {"q": 1, "r": {"s": "x", "t": null}},
      {"q": 2, "r": {"s": "y", "t": null}},
      {"q": 2, "r": {"s": "y", "t": null}},
      {"q": 2, "r": {"s": "y", "t": null}}
    ])"),
    from_json(leaf, each({R"({"z": null})", R"({"z": null})"})),
  };
  auto [result, decoded] = read_both(fields, columns);
  CHECK(result.diagnostics.empty());
  auto events = rows(result);
  REQUIRE_EQUAL(events.size(), size_t{6});
  CHECK_EQUAL(as<record>(events[0]).at("n"), (data{record{{"z", data{}}}}));
  CHECK_EQUAL(as<record>(events[1]).at("n"), data{});
  CHECK_EQUAL(as<record>(events[4]).at("m"), (data{record{{"z", data{}}}}));
  CHECK(repeats(result, "p.q"));
  CHECK(repeats(result, "p.r.s"));
  auto buffer = write_groups(fields, columns);
  result = run(buffer, {.filter = {}, .order = EventOrder::ordered});
  CHECK_EQUAL(fetched(result, buffer),
              chunks_of({0, column_index(buffer, "n.z")}, 2));
}

TEST("filters see the columns from statistics") {
  auto fields = arrow::FieldVector{
    arrow::field("id", arrow::int64()),
    arrow::field("c", arrow::int64()),
  };
  auto columns = arrow::ArrayVector{
    from_json(arrow::int64(), iota(9)),
    from_json(arrow::int64(), each({"1", "2", "3"})),
  };
  auto buffer = write_groups(fields, columns);
  auto request = [](std::string_view filter) {
    return ir::OptimizeRequest{.filter = {id_filter(filter)},
                               .order = EventOrder::ordered};
  };
  // Row-group pruning leaves the second row group.
  auto [result, decoded] = read_both(fields, columns, request("c == 2"));
  CHECK_EQUAL(total_rows(result.events), uint64_t{3});
  result = run(buffer, request("c == 2"));
  CHECK_EQUAL(fetched(result, buffer),
              (std::vector<std::pair<int, int>>{{1, 0}}));
  // Arithmetic keeps every row group, and the filter evaluates the constants.
  std::tie(result, decoded) = read_both(fields, columns, request("c + 0 != 2"));
  CHECK_EQUAL(total_rows(result.events), uint64_t{6});
  result = run(buffer, request("c + 0 != 2"));
  CHECK_EQUAL(fetched(result, buffer), chunks_of({0}, 3));
  // A comparison that warns warns the same way.
  std::tie(result, decoded) = read_both(fields, columns, request("c > \"x\""));
  CHECK(result.events.empty());
  CHECK(not result.diagnostics.empty());
}

TEST("statistics that do not decode leave their row group to the reader") {
  auto fields = arrow::FieldVector{
    arrow::field("id", arrow::int64()),
    arrow::field("c", arrow::int64()),
  };
  auto columns = arrow::ArrayVector{
    from_json(arrow::int64(), iota(6)),
    from_json(arrow::int64(), each({"7", "8"})),
  };
  // Shorten the minimum of `c` in the second row group to three bytes, which
  // do not decode as a 64-bit integer, and lengthen the maximum to keep the
  // size of the footer.
  auto eight = std::string{"\x08\0\0\0\0\0\0\0", 8};
  auto buffer = patch_footer(write_groups(fields, columns),
                             "\x28\x08" + eight + "\x18\x08" + eight,
                             "\x28\x0d" + eight + std::string(5, '\0')
                               + "\x18\x03" + eight.substr(0, 3));
  auto metadata = ::parquet::ReadMetaData(
    std::make_shared<arrow::io::BufferReader>(buffer));
  auto throws = false;
  try {
    std::ignore = metadata->RowGroup(1)->ColumnChunk(1)->statistics();
  } catch (::parquet::ParquetException const&) {
    throws = true;
  }
  REQUIRE(throws);
  auto result = run(buffer, {.filter = {}, .order = EventOrder::ordered});
  // Arrow attaches the statistics to what it decodes, which fails as it did
  // before.
  CHECK_EQUAL(total_rows(result.events), uint64_t{3});
  REQUIRE_EQUAL(result.diagnostics.size(), 1u);
  auto const& error = result.diagnostics.front();
  CHECK_EQUAL(error.severity, severity::error);
  CHECK(error.message.starts_with("IOError"));
  REQUIRE_EQUAL(error.notes.size(), 1u);
  CHECK_EQUAL(error.notes.front().message, "failed to read record batch");
  CHECK_EQUAL(fetched(result, buffer),
              (std::vector<std::pair<int, int>>{{0, 0}, {1, 0}, {1, 1}}));
}
