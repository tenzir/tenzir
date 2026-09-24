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

#include <tenzir/async.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/file_handle.hpp>
#include <tenzir/forwarding_file.hpp>
#include <tenzir/nova_flag.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/session.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/tql2/parser.hpp>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/util/future.h>
#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <folly/coro/BlockingWait.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>

#include <functional>
#include <mutex>

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

auto projection_of(std::string_view field) -> Option<ir::OptimizeProjection> {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto s = session{provider};
  auto expr
    = parse_expression_with_location_override(field, location::unknown, s);
  REQUIRE(expr);
  auto result = Option<ir::OptimizeProjection>{ir::OptimizeProjection{}};
  ir::add_to_projection(result, *ast::field_path::try_from(*expr));
  return result;
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
  auto pipe = compile("read_parquet", std::move(request), base_ctx{dh, reg});
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
  // Matches start in the third row group.
  auto result = run(buffer, {.filter = {id_filter("id >= 250")},
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
