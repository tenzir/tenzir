//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/async/scope.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/data.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/nova/materialize.hpp>
#include <tenzir/nova_flag.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/session.hpp>
#include <tenzir/source.hpp>
#include <tenzir/test/nova.hpp>
#include <tenzir/test/registry.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/parser.hpp>
#include <tenzir/tql2/registry.hpp>
#include <tenzir/tql2/resolve.hpp>
#include <tenzir/uuid.hpp>

#include <arpa/inet.h>
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/filesystem/localfs.h>
#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Sleep.h>
#include <iceberg/arrow/arrow_io_internal.h>
#include <iceberg/arrow/arrow_register.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/catalog/memory/in_memory_catalog.h>
#include <iceberg/data/data_writer.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/parquet/parquet_register.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_identifier.h>
#include <iceberg/table_metadata.h>
#include <iceberg/type.h>
#include <iceberg/update/fast_append.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <mutex>
#include <poll.h>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

using namespace tenzir;

namespace {

auto find_named(Description const& desc, std::string_view name)
  -> _::operator_plugin::Named const& {
  for (auto const& arg : desc.named) {
    for (auto const& alias : arg.names) {
      if (alias == name) {
        return arg;
      }
    }
  }
  FAIL("named argument `{}` not found", name);
}

auto set_catalog_arg(Description const& desc, Any& args, std::string uri)
  -> void {
  auto const& arg = find_named(desc, "catalog");
  auto expr = ast::expression{ast::constant{std::move(uri), location::unknown}};
  as<std::function<void(Any&, ast::expression)>>(arg.setter)(args,
                                                             std::move(expr));
}

auto spawn_from_iceberg(std::function<void(Description const&, Any&)> configure
                        = nullptr) -> Box<Operator<void, nova::Events>> {
  auto const* plugin = plugins::find<OperatorPlugin>("from_iceberg");
  REQUIRE(plugin);
  auto desc = plugin->describe();
  auto args = desc.make_args();
  as<std::function<void(Any&, located<std::string>)>>(
    desc.positional.front().setter)(args, located{std::string{"db.events"},
                                                  location::unknown});
  if (configure) {
    configure(desc, args);
  }
  for (auto& spawn : desc.spawns) {
    if (auto* create = try_as<Spawn<void, nova::Events>>(spawn)) {
      return (*create)(std::move(args));
    }
  }
  FAIL("Nova from_iceberg source is not registered");
}

auto compile_tql_diagnostics(std::string source_text)
  -> std::vector<diagnostic> {
  auto const prev_nova = nova_enabled();
  set_nova_enabled(true);
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, global_registry());
  auto context = base_ctx{dh, provider.as_session().reg()};
  auto sources = SourceMap{};
  auto source = Source::new_source(std::move(source_text), "test.tql", false);
  sources.add_source(source);
  auto root = compile_ctx::make_root(context, sources);
  auto parsed = parse(*source, provider.as_session());
  REQUIRE(parsed);
  if (resolve_entities(*parsed, ExternPrelude::empty(), context)) {
    if (auto compiled = std::move(*parsed).compile(root)) {
      std::ignore = ir::make_plan(std::move(*compiled), tag_v<void>, context);
    }
  }
  set_nova_enabled(prev_nova);
  return std::move(dh).collect();
}

class CollectingPush final : public Push<nova::Events> {
public:
  auto operator()(nova::Events events) -> Task<void> override {
    for (auto i = nova::storage::Index{0}; i < events.length(); ++i) {
      if (not events.mask.get(i)) {
        continue;
      }
      auto row = nova::materialize_legacy(events.data.get(i));
      REQUIRE(is<record>(row));
      ids.push_back(as<int64_t>(as<record>(row).at(field)));
    }
    co_return;
  }

  std::string field = "id";
  std::vector<int64_t> ids;
};

struct TempParquetDir {
  std::filesystem::path dir;
  std::shared_ptr<::iceberg::FileIO> io;

  TempParquetDir() {
    static std::once_flag reg_flag;
    std::call_once(reg_flag, [] {
      ::iceberg::arrow::RegisterAll();
      ::iceberg::parquet::RegisterAll();
      ::iceberg::avro::RegisterAll();
    });
    dir = std::filesystem::temp_directory_path()
          / fmt::format("tenzir-from-iceberg-test-{}", uuid::random());
    std::filesystem::create_directories(dir);
    io = std::make_shared<::iceberg::arrow::ArrowFileSystemFileIO>(
      std::make_shared<arrow::fs::LocalFileSystem>());
  }

  ~TempParquetDir() {
    auto ec = std::error_code{};
    std::filesystem::remove_all(dir, ec);
  }

  TempParquetDir(TempParquetDir const&) = delete;
  auto operator=(TempParquetDir const&) -> TempParquetDir& = delete;

  auto write_ids_file(std::string_view filename, int64_t first_id,
                      int64_t count) const -> std::pair<std::string, int64_t> {
    auto const path = fmt::format("file://{}", (dir / filename).string());
    auto schema = std::make_shared<::iceberg::Schema>(
      std::vector<::iceberg::SchemaField>{
        ::iceberg::SchemaField::MakeRequired(1, "id", ::iceberg::int64()),
      },
      /*schema_id=*/0);
    auto opts = ::iceberg::DataWriterOptions{
      .path = path,
      .schema = schema,
      .spec = ::iceberg::PartitionSpec::Unpartitioned(),
      .partition = {},
      .format = ::iceberg::FileFormatType::kParquet,
      .io = io,
      .sort_order_id = {},
      .properties = {},
    };
    auto writer = ::iceberg::DataWriter::Make(opts);
    REQUIRE(writer.has_value());
    auto builder = arrow::Int64Builder{};
    tenzir::check(builder.Reserve(count));
    for (auto i = int64_t{0}; i < count; ++i) {
      builder.UnsafeAppend(first_id + i);
    }
    auto col = tenzir::check(builder.Finish());
    auto struct_arr = tenzir::check(arrow::StructArray::Make(
      {col}, {arrow::field("id", arrow::int64(), false)}));
    auto c_arr = ArrowArray{};
    tenzir::check(arrow::ExportArray(*struct_arr, &c_arr));
    auto write_status = (*writer)->Write(&c_arr);
    if (c_arr.release != nullptr) {
      c_arr.release(&c_arr);
    }
    REQUIRE(write_status.has_value());
    REQUIRE((*writer)->Close().has_value());
    auto meta = (*writer)->Metadata();
    REQUIRE(meta.has_value());
    REQUIRE_EQUAL(meta->data_files.size(), size_t{1});
    return {path, meta->data_files.front()->file_size_in_bytes};
  }

  auto write_id_file(std::string_view filename, int64_t id_value) const
    -> std::pair<std::string, int64_t> {
    return write_ids_file(filename, id_value, 1);
  }
};

/// Minimal loopback REST catalog serving a single-snapshot table with
/// server-side scan planning (`POST .../plan`) whose responses are scripted
/// per request.
class MockServerPlanRestServer {
public:
  struct PlannedFile {
    std::string path;
    int64_t size_bytes = 0;
    Option<int64_t> start = None{};
    Option<int64_t> length = None{};
    int64_t record_count = 1;
  };

  explicit MockServerPlanRestServer(
    std::vector<std::vector<PlannedFile>> plan_responses,
    Option<std::string> renamed_column = None{})
    : plan_responses_{std::move(plan_responses)},
      renamed_column_{std::move(renamed_column)} {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(listen_fd_ >= 0);
    auto opt = int{1};
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    auto addr = sockaddr_in{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    REQUIRE_EQUAL(
      ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    REQUIRE_EQUAL(::listen(listen_fd_, 8), 0);
    auto len = socklen_t{sizeof(addr)};
    REQUIRE_EQUAL(
      ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len), 0);
    port_ = ntohs(addr.sin_port);
    thread_ = std::thread([this] {
      serve();
    });
  }

  ~MockServerPlanRestServer() {
    stop_.store(true, std::memory_order_release);
    if (listen_fd_ >= 0) {
      ::shutdown(listen_fd_, SHUT_RDWR);
      ::close(listen_fd_);
      listen_fd_ = -1;
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  MockServerPlanRestServer(MockServerPlanRestServer const&) = delete;
  auto operator=(MockServerPlanRestServer const&)
    -> MockServerPlanRestServer& = delete;

  auto uri() const -> std::string {
    return fmt::format("http://127.0.0.1:{}", port_);
  }

  auto plan_requests() -> std::vector<std::string> {
    auto lock = std::lock_guard{mutex_};
    return plan_requests_;
  }

private:
  auto next_plan_json(std::string request_body) -> std::string {
    auto lock = std::lock_guard{mutex_};
    plan_requests_.push_back(std::move(request_body));
    auto const idx = std::min(plan_call_++, plan_responses_.size() - size_t{1});
    auto tasks_json = std::string{};
    for (auto const& f : plan_responses_[idx]) {
      if (not tasks_json.empty()) {
        tasks_json += ',';
      }
      auto range_fields = std::string{};
      if (f.start) {
        range_fields += fmt::format(R"(,"start":{})", *f.start);
      }
      if (f.length) {
        range_fields += fmt::format(R"(,"length":{})", *f.length);
      }
      tasks_json += fmt::format(
        R"({{"data-file":{{"content":"data","file-path":"{}","file-format":"PARQUET","spec-id":0,"partition":[],"file-size-in-bytes":{},"record-count":{}}}{}}})",
        f.path, f.size_bytes, f.record_count, range_fields);
    }
    return fmt::format(
      R"({{"status":"completed","plan-id":"plan-{}","file-scan-tasks":[{}]}})",
      idx + 1, tasks_json);
  }

  auto schemas_json() const -> std::string {
    constexpr auto first
      = R"({"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"long"}]})";
    if (not renamed_column_) {
      return fmt::format(R"("current-schema-id":0,"schemas":[{}],)", first);
    }
    return fmt::format(
      R"("current-schema-id":1,"schemas":[{},{{"type":"struct","schema-id":1,"fields":[{{"id":1,"name":"{}","required":true,"type":"long"}}]}}],)",
      first, *renamed_column_);
  }

  auto serve() -> void {
    auto const config_json = std::string{
      R"({"defaults":{"scan-planning-mode":"server"},"overrides":{},)"
      R"("endpoints":["GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",)"
      R"("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"]})"};
    auto const table_json
      = std::
          string{R"({"metadata-location":"file:///tmp/mock/metadata/v1.metadata.json",)"
                 R"("config":{"scan-planning-mode":"server"},)"
                 R"("metadata":{"format-version":2,"table-uuid":"22222222-3333-4444-5555-666666666666",)"
                 R"("location":"file:///tmp/mock","last-sequence-number":1,"last-updated-ms":1704067200000,)"
                 R"("last-column-id":1,)"}
        + schemas_json()
        + R"("default-spec-id":0,"partition-specs":[{"spec-id":0,"fields":[]}],)"
          R"("last-partition-id":999,"default-sort-order-id":0,"sort-orders":[{"order-id":0,"fields":[]}],)"
          R"("properties":{},"current-snapshot-id":100,)"
          R"("snapshots":[{"snapshot-id":100,"sequence-number":1,"timestamp-ms":1704067200000,)"
          R"("summary":{"operation":"append"},"manifest-list":"file:///tmp/mock/snap-100.avro","schema-id":0}],)"
          R"("snapshot-log":[{"timestamp-ms":1704067200000,"snapshot-id":100}],"metadata-log":[]}})";
    while (not stop_.load(std::memory_order_acquire)) {
      auto pfd = pollfd{.fd = listen_fd_, .events = POLLIN, .revents = 0};
      auto const rc = ::poll(&pfd, 1, 50);
      if (rc <= 0) {
        continue;
      }
      auto client = ::accept(listen_fd_, nullptr, nullptr);
      if (client < 0) {
        continue;
      }
      auto buf = std::string{};
      auto chunk = std::string(4096, '\0');
      auto content_length = size_t{0};
      auto header_end = std::string::npos;
      while (true) {
        auto const n = ::recv(client, chunk.data(), chunk.size(), 0);
        if (n <= 0) {
          break;
        }
        buf.append(chunk.data(), static_cast<size_t>(n));
        if (header_end == std::string::npos) {
          header_end = buf.find("\r\n\r\n");
          if (header_end != std::string::npos) {
            auto const headers = std::string_view{buf}.substr(0, header_end);
            auto const lower_headers = detail::ascii_tolower(headers);
            if (auto const cl = lower_headers.find("content-length:");
                cl != std::string::npos) {
              auto pos = cl + std::string_view{"content-length:"}.size();
              while (pos < lower_headers.size() and lower_headers[pos] == ' ') {
                ++pos;
              }
              auto end = lower_headers.find("\r\n", pos);
              auto const digits = std::string_view{lower_headers}.substr(
                pos, end == std::string::npos ? std::string::npos : end - pos);
              auto val = size_t{0};
              for (auto c : digits) {
                if (c >= '0' and c <= '9') {
                  val = val * 10 + static_cast<size_t>(c - '0');
                }
              }
              content_length = val;
            }
          }
        }
        if (header_end != std::string::npos
            and buf.size() >= header_end + 4 + content_length) {
          break;
        }
      }
      if (not buf.empty()) {
        auto const first_line_end = buf.find("\r\n");
        auto const req_line = buf.substr(0, first_line_end);
        auto body = std::string{};
        auto status = std::string_view{"200 OK"};
        if (req_line.find("/v1/config") != std::string::npos) {
          body = config_json;
        } else if (req_line.find("/v1/namespaces/db/tables/events/plan")
                   != std::string::npos) {
          auto req_body = header_end == std::string::npos
                            ? std::string{}
                            : buf.substr(header_end + 4, content_length);
          body = next_plan_json(std::move(req_body));
        } else if (req_line.find("/v1/namespaces/db/tables/events")
                   != std::string::npos) {
          body = table_json;
        } else {
          status = "404 Not Found";
          body
            = R"({"error":{"message":"not found","type":"NotFoundException","code":404}})";
        }
        auto resp
          = fmt::format("HTTP/1.1 {}\r\nContent-Type: application/json\r\n"
                        "Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                        status, body.size(), body);
        auto sent = size_t{0};
        while (sent < resp.size()) {
          auto const w
            = ::send(client, resp.data() + sent, resp.size() - sent, 0);
          if (w <= 0) {
            break;
          }
          sent += static_cast<size_t>(w);
        }
      }
      ::close(client);
    }
  }

  std::mutex mutex_;
  std::vector<std::vector<PlannedFile>> plan_responses_;
  Option<std::string> renamed_column_;
  std::vector<std::string> plan_requests_;
  size_t plan_call_ = 0;
  int listen_fd_ = -1;
  uint16_t port_ = 0;
  std::atomic<bool> stop_ = false;
  std::thread thread_;
};

class UnusedPush final : public Push<nova::Events> {
public:
  auto operator()(nova::Events) -> Task<void> override {
    FAIL("unexpected output event");
    co_return;
  }
};

class ScopedOpCtx final : public test::NovaOpCtx {
public:
  ScopedOpCtx(diagnostic_handler& dh, registry const& reg, AsyncScope& scope)
    : NovaOpCtx{dh, reg}, scope_{scope} {
  }

  auto spawn_task(Task<void> task) -> AsyncHandle<void> override {
    return scope_.spawn(std::move(task));
  }

  auto make_counter(MetricsLabel, MetricsDirection, MetricsVisibility,
                    MetricsUnit) -> MetricsCounter override {
    return {};
  }

private:
  AsyncScope& scope_;
};

class CountingPush final : public Push<nova::Events> {
public:
  auto operator()(nova::Events events) -> Task<void> override {
    rows += static_cast<uint64_t>(events.active_count());
    co_return;
  }

  uint64_t rows = 0;
};

/// An operator context that records when the tasks it spawned have finished.
class WatchedOpCtx final : public test::NovaOpCtx {
public:
  WatchedOpCtx(diagnostic_handler& dh, registry const& reg, AsyncScope& scope)
    : NovaOpCtx{dh, reg}, scope_{scope} {
  }

  auto spawn_task(Task<void> task) -> AsyncHandle<void> override {
    spawned_ += 1;
    return scope_.spawn(
      [task = std::move(task), ended = ended_]() mutable -> Task<void> {
        auto guard = folly::makeGuard([&] {
          ended->fetch_add(1);
        });
        co_await std::move(task);
      });
  }

  auto make_counter(MetricsLabel, MetricsDirection, MetricsVisibility,
                    MetricsUnit) -> MetricsCounter override {
    return {};
  }

  auto spawned() const -> size_t {
    return spawned_;
  }

  auto all_ended() const -> bool {
    return ended_->load() == spawned_;
  }

private:
  AsyncScope& scope_;
  size_t spawned_ = 0;
  std::shared_ptr<std::atomic<size_t>> ended_
    = std::make_shared<std::atomic<size_t>>(0);
};

/// Minimal loopback HTTP server serving an Iceberg REST `/v1/config` and a
/// scripted client-planned table response at `/v1/namespaces/db/tables/events`.
class MockTableRestServer {
public:
  explicit MockTableRestServer(std::string table_json)
    : table_json_{std::move(table_json)} {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(listen_fd_ >= 0);
    auto opt = int{1};
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    auto addr = sockaddr_in{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    REQUIRE_EQUAL(
      ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    REQUIRE_EQUAL(::listen(listen_fd_, 8), 0);
    auto len = socklen_t{sizeof(addr)};
    REQUIRE_EQUAL(
      ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len), 0);
    port_ = ntohs(addr.sin_port);
    thread_ = std::thread([this] {
      serve();
    });
  }

  ~MockTableRestServer() {
    stop_.store(true, std::memory_order_release);
    if (listen_fd_ >= 0) {
      ::shutdown(listen_fd_, SHUT_RDWR);
      ::close(listen_fd_);
      listen_fd_ = -1;
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  MockTableRestServer(MockTableRestServer const&) = delete;
  auto operator=(MockTableRestServer const&) -> MockTableRestServer& = delete;

  auto uri() const -> std::string {
    return fmt::format("http://127.0.0.1:{}", port_);
  }

private:
  auto serve() -> void {
    while (not stop_.load(std::memory_order_acquire)) {
      auto pfd = pollfd{.fd = listen_fd_, .events = POLLIN, .revents = 0};
      auto const rc = ::poll(&pfd, 1, 50);
      if (rc <= 0) {
        continue;
      }
      auto client = ::accept(listen_fd_, nullptr, nullptr);
      if (client < 0) {
        continue;
      }
      auto buf = std::string(4096, '\0');
      auto const n = ::recv(client, buf.data(), buf.size(), 0);
      if (n > 0) {
        buf.resize(static_cast<size_t>(n));
        auto const first_line_end = buf.find("\r\n");
        auto const req_line = buf.substr(0, first_line_end);
        auto body = std::string{};
        auto status = std::string_view{"200 OK"};
        if (req_line.find("/v1/config") != std::string::npos) {
          body = R"({"defaults":{},"overrides":{}})";
        } else if (req_line.find("/v1/namespaces/db/tables/events")
                   != std::string::npos) {
          body = table_json_;
        } else {
          status = "404 Not Found";
          body
            = R"({"error":{"message":"not found","type":"NotFoundException","code":404}})";
        }
        auto resp
          = fmt::format("HTTP/1.1 {}\r\nContent-Type: application/json\r\n"
                        "Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                        status, body.size(), body);
        auto sent = size_t{0};
        while (sent < resp.size()) {
          auto const w
            = ::send(client, resp.data() + sent, resp.size() - sent, 0);
          if (w <= 0) {
            break;
          }
          sent += static_cast<size_t>(w);
        }
      }
      ::close(client);
    }
  }

  std::string table_json_;
  int listen_fd_ = -1;
  uint16_t port_ = 0;
  std::atomic<bool> stop_ = false;
  std::thread thread_;
};

/// Minimal loopback HTTP server serving an Iceberg REST `/v1/config` and an
/// empty v2 table at `/v1/namespaces/db/tables/events` for graceful-stop tests.
class MockEmptyTableRestServer {
public:
  MockEmptyTableRestServer() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(listen_fd_ >= 0);
    auto opt = int{1};
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    auto addr = sockaddr_in{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    REQUIRE_EQUAL(
      ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    REQUIRE_EQUAL(::listen(listen_fd_, 8), 0);
    auto len = socklen_t{sizeof(addr)};
    REQUIRE_EQUAL(
      ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len), 0);
    port_ = ntohs(addr.sin_port);
    thread_ = std::thread([this] {
      serve();
    });
  }

  ~MockEmptyTableRestServer() {
    stop_.store(true, std::memory_order_release);
    if (listen_fd_ >= 0) {
      ::shutdown(listen_fd_, SHUT_RDWR);
      ::close(listen_fd_);
      listen_fd_ = -1;
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  MockEmptyTableRestServer(MockEmptyTableRestServer const&) = delete;
  auto operator=(MockEmptyTableRestServer const&)
    -> MockEmptyTableRestServer& = delete;

  auto uri() const -> std::string {
    return fmt::format("http://127.0.0.1:{}", port_);
  }

private:
  auto serve() -> void {
    auto const table_json = std::string{
      R"({"metadata-location":"file:///tmp/mock/metadata/v1.metadata.json",)"
      R"("metadata":{"format-version":2,"table-uuid":"11111111-2222-3333-4444-555555555555",)"
      R"("location":"file:///tmp/mock","last-sequence-number":0,"last-updated-ms":1704067200000,)"
      R"("last-column-id":1,"current-schema-id":0,)"
      R"("schemas":[{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"long"}]}],)"
      R"("default-spec-id":0,"partition-specs":[{"spec-id":0,"fields":[]}],)"
      R"("last-partition-id":999,"default-sort-order-id":0,"sort-orders":[{"order-id":0,"fields":[]}],)"
      R"("properties":{},"current-snapshot-id":-1,"snapshots":[],"snapshot-log":[],"metadata-log":[]}})"};
    while (not stop_.load(std::memory_order_acquire)) {
      auto pfd = pollfd{.fd = listen_fd_, .events = POLLIN, .revents = 0};
      auto const rc = ::poll(&pfd, 1, 50);
      if (rc <= 0) {
        continue;
      }
      auto client = ::accept(listen_fd_, nullptr, nullptr);
      if (client < 0) {
        continue;
      }
      auto buf = std::string(4096, '\0');
      auto const n = ::recv(client, buf.data(), buf.size(), 0);
      if (n > 0) {
        buf.resize(static_cast<size_t>(n));
        auto const first_line_end = buf.find("\r\n");
        auto const req_line = buf.substr(0, first_line_end);
        auto body = std::string{};
        auto status = std::string_view{"200 OK"};
        if (req_line.find("/v1/config") != std::string::npos) {
          body = R"({"defaults":{},"overrides":{}})";
        } else if (req_line.find("/v1/namespaces/db/tables/events")
                   != std::string::npos) {
          body = table_json;
        } else {
          status = "404 Not Found";
          body
            = R"({"error":{"message":"not found","type":"NotFoundException","code":404}})";
        }
        auto resp
          = fmt::format("HTTP/1.1 {}\r\nContent-Type: application/json\r\n"
                        "Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                        status, body.size(), body);
        auto sent = size_t{0};
        while (sent < resp.size()) {
          auto const w
            = ::send(client, resp.data() + sent, resp.size() - sent, 0);
          if (w <= 0) {
            break;
          }
          sent += static_cast<size_t>(w);
        }
      }
      ::close(client);
    }
  }

  int listen_fd_ = -1;
  uint16_t port_ = 0;
  std::atomic<bool> stop_ = false;
  std::thread thread_;
};

struct Checkpoint {
  std::string table_uuid;
  bool done = false;
  Option<int64_t> pinned_snapshot_id = None{};
  Option<int64_t> last_snapshot_id = None{};
  Option<int64_t> active_to_snapshot_id = None{};
  uint64_t file_index = 0;
  uint64_t rows_in_file = 0;
  std::string current_file_path;
  int64_t current_file_start = 0;
  int64_t current_file_length = 0;
  std::string completed_files_digest;
  bool counted_from_metadata = false;
  Option<uint64_t> remaining = None{};
  Option<int32_t> plan_schema_id = None{};
};

auto encode_checkpoint(Checkpoint cp) -> caf::byte_buffer {
  auto bytes = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{bytes};
  auto saving = Serde{serializer};
  saving("table_uuid", cp.table_uuid);
  saving("done", cp.done);
  saving("pinned_snapshot_id", cp.pinned_snapshot_id);
  saving("last_snapshot_id", cp.last_snapshot_id);
  saving("active_to_snapshot_id", cp.active_to_snapshot_id);
  saving("file_index", cp.file_index);
  saving("rows_in_file", cp.rows_in_file);
  saving("current_file_path", cp.current_file_path);
  saving("current_file_start", cp.current_file_start);
  saving("current_file_length", cp.current_file_length);
  saving("completed_files_digest", cp.completed_files_digest);
  saving("counted_from_metadata", cp.counted_from_metadata);
  saving("remaining", cp.remaining);
  saving("plan_schema_id", cp.plan_schema_id);
  return bytes;
}

auto decode_checkpoint(caf::byte_buffer const& bytes) -> Checkpoint {
  auto cp = Checkpoint{};
  auto deserializer = caf::binary_deserializer{bytes};
  auto loading = Serde{deserializer};
  loading("table_uuid", cp.table_uuid);
  loading("done", cp.done);
  loading("pinned_snapshot_id", cp.pinned_snapshot_id);
  loading("last_snapshot_id", cp.last_snapshot_id);
  loading("active_to_snapshot_id", cp.active_to_snapshot_id);
  loading("file_index", cp.file_index);
  loading("rows_in_file", cp.rows_in_file);
  loading("current_file_path", cp.current_file_path);
  loading("current_file_start", cp.current_file_start);
  loading("current_file_length", cp.current_file_length);
  loading("completed_files_digest", cp.completed_files_digest);
  loading("counted_from_metadata", cp.counted_from_metadata);
  loading("remaining", cp.remaining);
  loading("plan_schema_id", cp.plan_schema_id);
  return cp;
}

auto restore_from(Operator<void, nova::Events>& source,
                  caf::byte_buffer const& bytes) -> void {
  auto deserializer = caf::binary_deserializer{bytes};
  auto loading = Serde{deserializer};
  source.snapshot(loading);
}

auto planned_file(std::pair<std::string, int64_t> written, int64_t record_count
                                                           = 1)
  -> MockServerPlanRestServer::PlannedFile {
  return MockServerPlanRestServer::PlannedFile{
    .path = std::move(written.first),
    .size_bytes = written.second,
    .start = None{},
    .length = None{},
    .record_count = record_count,
  };
}

auto set_filter_arg(Description const& desc, Any& args, std::string_view source)
  -> void {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
  REQUIRE(expr);
  REQUIRE(resolve_entities(*expr, ExternPrelude::empty(), ctx));
  REQUIRE(desc.set_filter.has_value());
  (*desc.set_filter)(args, ir::OptimizeFilter{std::move(*expr)});
}

auto ids_between(int64_t first, int64_t last) -> std::vector<int64_t> {
  auto ids = std::vector<int64_t>{};
  for (auto id = first; id < last; ++id) {
    ids.push_back(id);
  }
  return ids;
}

auto run_to_end(Operator<void, nova::Events>& source, diagnostic_handler& dh,
                Push<nova::Events>& push) -> void {
  auto reg = registry{};
  folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
    auto ctx = ScopedOpCtx{dh, reg, scope};
    co_await source.start(ctx);
    while (source.state() != OperatorState::done) {
      auto msg = co_await source.await_task(dh);
      co_await source.process_task(std::move(msg), push, ctx);
    }
  }));
}

auto run_to_checkpoint(Operator<void, nova::Events>& source,
                       diagnostic_handler& dh, CollectingPush& push,
                       std::function<bool()> should_checkpoint)
  -> std::pair<caf::byte_buffer, std::vector<int64_t>> {
  auto checkpoint = caf::byte_buffer{};
  auto emitted = std::vector<int64_t>{};
  auto reg = registry{};
  folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
    auto ctx = ScopedOpCtx{dh, reg, scope};
    co_await source.start(ctx);
    while (source.state() != OperatorState::done and not should_checkpoint()) {
      auto msg = co_await source.await_task(dh);
      co_await source.process_task(std::move(msg), push, ctx);
    }
    auto serializer = caf::binary_serializer{checkpoint};
    auto saving = Serde{serializer};
    source.snapshot(saving);
    emitted = push.ids;
    co_await source.stop(ctx);
    auto drain = CollectingPush{};
    drain.field = push.field;
    while (source.state() != OperatorState::done) {
      auto msg = co_await source.await_task(dh);
      co_await source.process_task(std::move(msg), drain, ctx);
    }
  }));
  return {std::move(checkpoint), std::move(emitted)};
}

} // namespace

TEST("from_iceberg requires from_snapshot_id when to_snapshot_id is set") {
  auto missing_from = compile_tql_diagnostics(
    R"(from_iceberg "db.events", catalog="https://example.invalid/iceberg", to_snapshot_id=2)");
  REQUIRE_EQUAL(missing_from.size(), size_t{1});
  CHECK_EQUAL(missing_from[0].severity, severity::error);
  CHECK_EQUAL(missing_from[0].message,
              "`to_snapshot_id` requires `from_snapshot_id`");
  auto with_from = compile_tql_diagnostics(
    R"(from_iceberg "db.events", catalog="https://example.invalid/iceberg", from_snapshot_id=1, to_snapshot_id=2)");
  CHECK(with_from.empty());
}

TEST("from_iceberg live checkpoint preserves from_snapshot_id before first "
     "window") {
  auto configure = [](Description const& desc, Any& args) {
    as<std::function<void(Any&, located<bool>)>>(
      find_named(desc, "live").setter)(args, located{true, location::unknown});
    as<std::function<void(Any&, located<int64_t>)>>(
      find_named(desc, "from_snapshot_id").setter)(
      args, located{int64_t{42}, location::unknown});
  };
  auto source = spawn_from_iceberg(configure);
  auto bytes = caf::byte_buffer{};
  {
    auto serializer = caf::binary_serializer{bytes};
    auto saving = Serde{serializer};
    source->snapshot(saving);
  }
  // Inspect serialized checkpoint fields directly to confirm
  // `last_snapshot_id` is initialized to 42 prior to any WindowDoneMessage.
  auto saved = decode_checkpoint(bytes);
  CHECK_EQUAL(saved.last_snapshot_id, Option<int64_t>{42});
  CHECK(not saved.counted_from_metadata);
  CHECK_EQUAL(saved.plan_schema_id, Option<int32_t>{None{}});
  // Restoring into a fresh operator preserves the exact checkpoint bytes.
  auto restored = spawn_from_iceberg(configure);
  {
    auto deserializer = caf::binary_deserializer{bytes};
    auto loading = Serde{deserializer};
    restored->snapshot(loading);
  }
  auto resaved = caf::byte_buffer{};
  {
    auto serializer = caf::binary_serializer{resaved};
    auto saving = Serde{serializer};
    restored->snapshot(saving);
  }
  CHECK_EQUAL(resaved, bytes);
}

TEST("from_iceberg restores completed and mid-file checkpoints") {
  // 1. Completed checkpoint transitions directly to done without touching any
  //    catalog or network endpoint.
  auto completed_bytes = encode_checkpoint(Checkpoint{
    .table_uuid = "uuid-completed",
    .done = true,
    .pinned_snapshot_id = 10,
    .last_snapshot_id = None{},
    .active_to_snapshot_id = None{},
    .file_index = 2,
    .rows_in_file = 0,
    .current_file_path = {},
    .current_file_start = 0,
    .current_file_length = 0,
    .completed_files_digest = "digest-2",
    .counted_from_metadata = false,
    .remaining = None{},
    .plan_schema_id = None{},
  });
  auto source = spawn_from_iceberg();
  restore_from(*source, completed_bytes);
  CHECK_EQUAL(source->state(), OperatorState::done);
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto ctx = test::NovaOpCtx{dh, reg};
  folly::coro::blockingWait(source->start(ctx));
  CHECK(dh.empty());
  // 2. Mid-file checkpoint preserves `current_file_path`,
  //    `counted_from_metadata`, `remaining`, and `plan_schema_id`.
  auto mid_bytes = encode_checkpoint(Checkpoint{
    .table_uuid = "uuid-mid",
    .done = false,
    .pinned_snapshot_id = 7,
    .last_snapshot_id = None{},
    .active_to_snapshot_id = None{},
    .file_index = 3,
    .rows_in_file = 50,
    .current_file_path = "file:///warehouse/data/part-3.parquet",
    .current_file_start = 0,
    .current_file_length = 1024,
    .completed_files_digest = "digest-3",
    .counted_from_metadata = true,
    .remaining = 10,
    .plan_schema_id = 2,
  });
  auto mid_source = spawn_from_iceberg();
  restore_from(*mid_source, mid_bytes);
  CHECK_EQUAL(mid_source->state(), OperatorState::normal);
  auto resaved = caf::byte_buffer{};
  {
    auto serializer = caf::binary_serializer{resaved};
    auto saving = Serde{serializer};
    mid_source->snapshot(saving);
  }
  CHECK_EQUAL(resaved, mid_bytes);
}

TEST("from_iceberg graceful stop delivers DoneMessage without deadlock") {
  auto server = MockEmptyTableRestServer{};
  auto source = spawn_from_iceberg([&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
    as<std::function<void(Any&, located<bool>)>>(
      find_named(desc, "live").setter)(args, located{true, location::unknown});
    as<std::function<void(Any&, located<duration>)>>(
      find_named(desc, "poll_interval").setter)(
      args, located{duration{std::chrono::hours{1}}, location::unknown});
  });
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto push = UnusedPush{};
  folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
    auto ctx = ScopedOpCtx{dh, reg, scope};
    co_await source->start(ctx);
    CHECK_EQUAL(source->state(), OperatorState::normal);
    // Simulate executor GracefulStop: call stop() WITHOUT cancelling `scope`,
    // then drive await_task() -> process_task() until OperatorState::done.
    co_await source->stop(ctx);
    while (source->state() != OperatorState::done) {
      auto msg = co_await source->await_task(dh);
      co_await source->process_task(std::move(msg), push, ctx);
    }
    CHECK_EQUAL(source->state(), OperatorState::done);
  }));
  CHECK(dh.empty());
}

TEST("from_iceberg resumes at file boundary cleanly across reordered server "
     "plans") {
  auto tmp = TempParquetDir{};
  auto file_a = planned_file(tmp.write_id_file("a.parquet", 101));
  auto file_b = planned_file(tmp.write_id_file("b.parquet", 202));
  // Call 1 returns [b, a]; Call 2 returns [a, b]. Sorting server-side plans
  // ensures both runs process [a, b] stably.
  auto server = MockServerPlanRestServer{{{file_b, file_a}, {file_a, file_b}}};
  auto configure = [&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
  };
  auto checkpoint = caf::byte_buffer{};
  {
    auto source = spawn_from_iceberg(configure);
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto push = CollectingPush{};
    folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
      auto ctx = ScopedOpCtx{dh, reg, scope};
      co_await source->start(ctx);
      // Process SnapshotPinnedMessage, BatchMessage(a), and FileDoneMessage(a)
      // so the checkpoint lands right on the file boundary (`file_index == 1`,
      // `rows_in_file == 0`, `current_file_path == ""`).
      for (auto step = 0; step < 3; ++step) {
        auto msg = co_await source->await_task(dh);
        co_await source->process_task(std::move(msg), push, ctx);
      }
      auto serializer = caf::binary_serializer{checkpoint};
      auto saving = Serde{serializer};
      source->snapshot(saving);
      REQUIRE_EQUAL(push.ids.size(), size_t{1});
      CHECK_EQUAL(push.ids.front(), int64_t{101});
      co_await source->stop(ctx);
      auto drain = CollectingPush{};
      while (source->state() != OperatorState::done) {
        auto msg = co_await source->await_task(dh);
        co_await source->process_task(std::move(msg), drain, ctx);
      }
    }));
    CHECK(dh.empty());
  }
  // Confirm the saved checkpoint is on a clean file boundary.
  {
    auto saved = decode_checkpoint(checkpoint);
    CHECK_EQUAL(saved.file_index, uint64_t{1});
    CHECK_EQUAL(saved.rows_in_file, uint64_t{0});
    CHECK(saved.current_file_path.empty());
    CHECK(not saved.completed_files_digest.empty());
    CHECK(not saved.counted_from_metadata);
    CHECK_EQUAL(saved.plan_schema_id, Option<int32_t>{0});
  }
  // Restore into a fresh operator and run against the reordered /plan response.
  {
    auto restored = spawn_from_iceberg(configure);
    restore_from(*restored, checkpoint);
    auto dh = collecting_diagnostic_handler{};
    auto push = CollectingPush{};
    run_to_end(*restored, dh, push);
    CHECK(dh.empty());
    REQUIRE_EQUAL(push.ids.size(), size_t{1});
    CHECK_EQUAL(push.ids.front(), int64_t{202});
  }
}

TEST("from_iceberg rejects file-boundary resume when skipped files differ or "
     "plan shrinks") {
  auto tmp = TempParquetDir{};
  auto file_a = planned_file(tmp.write_id_file("a.parquet", 1));
  auto file_b = planned_file(tmp.write_id_file("b.parquet", 2));
  auto file_c = planned_file(tmp.write_id_file("c.parquet", 3));
  auto file_a_shorter = file_a;
  file_a_shorter.size_bytes -= 1;
  // Call 1 returns [a, b]; Call 2 replaces file 0 with [c, b] (mismatched
  // path); Call 3 returns file_a with the same (path, start=0) but a shorter
  // task length (`size_a - 1`); Call 4 returns [] (shrunk below file_index).
  auto server = MockServerPlanRestServer{
    {{file_a, file_b}, {file_c, file_b}, {file_a_shorter, file_b}, {}}};
  auto configure = [&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
  };
  auto checkpoint = caf::byte_buffer{};
  {
    auto source = spawn_from_iceberg(configure);
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto push = CollectingPush{};
    folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
      auto ctx = ScopedOpCtx{dh, reg, scope};
      co_await source->start(ctx);
      for (auto step = 0; step < 3; ++step) {
        auto msg = co_await source->await_task(dh);
        co_await source->process_task(std::move(msg), push, ctx);
      }
      auto serializer = caf::binary_serializer{checkpoint};
      auto saving = Serde{serializer};
      source->snapshot(saving);
      REQUIRE_EQUAL(push.ids.size(), size_t{1});
      CHECK_EQUAL(push.ids.front(), int64_t{1});
      co_await source->stop(ctx);
      auto drain = CollectingPush{};
      while (source->state() != OperatorState::done) {
        auto msg = co_await source->await_task(dh);
        co_await source->process_task(std::move(msg), drain, ctx);
      }
    }));
    CHECK(dh.empty());
  }
  // Restore against [c, b]: file 0 is `b` (sorted [b, c]) instead of `a`.
  {
    auto restored = spawn_from_iceberg(configure);
    {
      auto deserializer = caf::binary_deserializer{checkpoint};
      auto loading = Serde{deserializer};
      restored->snapshot(loading);
    }
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto push = CollectingPush{};
    folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
      auto ctx = ScopedOpCtx{dh, reg, scope};
      co_await restored->start(ctx);
      while (restored->state() != OperatorState::done) {
        auto msg = co_await restored->await_task(dh);
        co_await restored->process_task(std::move(msg), push, ctx);
      }
    }));
    CHECK(push.ids.empty());
    auto diags = std::move(dh).collect();
    REQUIRE_EQUAL(diags.size(), size_t{1});
    CHECK_EQUAL(diags.front().severity, severity::error);
    CHECK(diags.front().message.contains("do not match checkpointed files"));
  }
  // Restore against [file_a(len = size_a - 1), file_b]: task 0 has the same
  // (path, start) as the completed task but a different length, so the digest
  // check must reject it instead of skipping a different byte range.
  {
    auto restored = spawn_from_iceberg(configure);
    {
      auto deserializer = caf::binary_deserializer{checkpoint};
      auto loading = Serde{deserializer};
      restored->snapshot(loading);
    }
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto push = CollectingPush{};
    folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
      auto ctx = ScopedOpCtx{dh, reg, scope};
      co_await restored->start(ctx);
      while (restored->state() != OperatorState::done) {
        auto msg = co_await restored->await_task(dh);
        co_await restored->process_task(std::move(msg), push, ctx);
      }
    }));
    CHECK(push.ids.empty());
    auto diags = std::move(dh).collect();
    REQUIRE_EQUAL(diags.size(), size_t{1});
    CHECK_EQUAL(diags.front().severity, severity::error);
    CHECK(diags.front().message.contains("do not match checkpointed files"));
  }
  // Restore against []: fewer files than checkpointed file_index (1).
  {
    auto restored = spawn_from_iceberg(configure);
    {
      auto deserializer = caf::binary_deserializer{checkpoint};
      auto loading = Serde{deserializer};
      restored->snapshot(loading);
    }
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto push = CollectingPush{};
    folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
      auto ctx = ScopedOpCtx{dh, reg, scope};
      co_await restored->start(ctx);
      while (restored->state() != OperatorState::done) {
        auto msg = co_await restored->await_task(dh);
        co_await restored->process_task(std::move(msg), push, ctx);
      }
    }));
    CHECK(push.ids.empty());
    auto diags = std::move(dh).collect();
    REQUIRE_EQUAL(diags.size(), size_t{1});
    CHECK_EQUAL(diags.front().severity, severity::error);
    CHECK(diags.front().message.contains("fewer than checkpointed file index"));
  }
}

TEST("from_iceberg enforces limit locally without sending min-rows-requested") {
  auto tmp = TempParquetDir{};
  auto file_a = planned_file(tmp.write_id_file("a.parquet", 10));
  auto file_b = planned_file(tmp.write_id_file("b.parquet", 20));
  auto server = MockServerPlanRestServer{{{file_a, file_b}}};
  auto source = spawn_from_iceberg([&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
    REQUIRE(desc.set_limit.has_value());
    (*desc.set_limit)(args, Option<uint64_t>{1});
  });
  auto dh = collecting_diagnostic_handler{};
  auto push = CollectingPush{};
  run_to_end(*source, dh, push);
  CHECK(dh.empty());
  REQUIRE_EQUAL(push.ids.size(), size_t{1});
  CHECK_EQUAL(push.ids.front(), int64_t{10});
  auto requests = server.plan_requests();
  REQUIRE_EQUAL(requests.size(), size_t{1});
  CHECK(not requests.front().empty());
  CHECK(requests.front().contains("\"select\""));
  CHECK(not requests.front().contains("min-rows-requested"));
}

TEST("from_iceberg reads 20 files concurrently in exact deterministic order "
     "with checkpoint resume") {
  auto tmp = TempParquetDir{};
  auto files = std::vector<MockServerPlanRestServer::PlannedFile>{};
  auto expected_all = std::vector<int64_t>{};
  for (auto i = int64_t{0}; i < 20; ++i) {
    files.push_back(
      planned_file(tmp.write_id_file(fmt::format("f{:02}.parquet", i), i)));
    expected_all.push_back(i);
  }
  auto reversed_files = files;
  std::ranges::reverse(reversed_files);
  // Call 1 returns reversed order (sorted to f00..f19); Call 2 returns
  // f00..f19; Call 3 is a full uninterrupted read.
  auto server
    = MockServerPlanRestServer{{reversed_files, files, reversed_files}};
  auto configure = [&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
  };
  // 1. Run until file boundary `file_index == 5`, checkpoint, and stop.
  auto checkpoint = caf::byte_buffer{};
  {
    auto source = spawn_from_iceberg(configure);
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto push = CollectingPush{};
    folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
      auto ctx = ScopedOpCtx{dh, reg, scope};
      co_await source->start(ctx);
      // 1 SnapshotPinnedMessage + 5 * (BatchMessage + FileDoneMessage) = 11.
      for (auto step = 0; step < 11; ++step) {
        auto msg = co_await source->await_task(dh);
        co_await source->process_task(std::move(msg), push, ctx);
      }
      auto serializer = caf::binary_serializer{checkpoint};
      auto saving = Serde{serializer};
      source->snapshot(saving);
      // Must arrive in exact sorted order 0..4 without sorting `push.ids`.
      CHECK_EQUAL(push.ids, (std::vector<int64_t>{0, 1, 2, 3, 4}));
      co_await source->stop(ctx);
      auto drain = CollectingPush{};
      while (source->state() != OperatorState::done) {
        auto msg = co_await source->await_task(dh);
        co_await source->process_task(std::move(msg), drain, ctx);
      }
    }));
    CHECK(dh.empty());
  }
  // Verify checkpoint recorded `file_index == 5`.
  {
    auto saved = decode_checkpoint(checkpoint);
    CHECK(not saved.done);
    CHECK_EQUAL(saved.file_index, uint64_t{5});
    CHECK_EQUAL(saved.rows_in_file, uint64_t{0});
    CHECK(saved.current_file_path.empty());
    CHECK(not saved.completed_files_digest.empty());
    CHECK(not saved.counted_from_metadata);
    CHECK_EQUAL(saved.plan_schema_id, Option<int32_t>{0});
  }
  // 2. Restore into a fresh operator and verify exact remaining order 5..19.
  {
    auto restored = spawn_from_iceberg(configure);
    {
      auto deserializer = caf::binary_deserializer{checkpoint};
      auto loading = Serde{deserializer};
      restored->snapshot(loading);
    }
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto push = CollectingPush{};
    folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
      auto ctx = ScopedOpCtx{dh, reg, scope};
      co_await restored->start(ctx);
      while (restored->state() != OperatorState::done) {
        auto msg = co_await restored->await_task(dh);
        co_await restored->process_task(std::move(msg), push, ctx);
      }
    }));
    CHECK(dh.empty());
    auto expected_rest = std::vector<int64_t>{};
    for (auto i = int64_t{5}; i < 20; ++i) {
      expected_rest.push_back(i);
    }
    CHECK_EQUAL(push.ids, expected_rest);
  }
  // 3. Full uninterrupted run also emits exact order 0..19.
  {
    auto full = spawn_from_iceberg(configure);
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto push = CollectingPush{};
    folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
      auto ctx = ScopedOpCtx{dh, reg, scope};
      co_await full->start(ctx);
      while (full->state() != OperatorState::done) {
        auto msg = co_await full->await_task(dh);
        co_await full->process_task(std::move(msg), push, ctx);
      }
    }));
    CHECK(dh.empty());
    CHECK_EQUAL(push.ids, expected_all);
  }
}

TEST("from_iceberg stops promptly while concurrent file readers wait on a "
     "full queue") {
  auto tmp = TempParquetDir{};
  auto files = std::vector<MockServerPlanRestServer::PlannedFile>{};
  // 32 files produce 64 messages (batch + file-done), filling the bounded
  // operator queue (8) and per-file prefetch queues while nobody consumes.
  for (auto i = int64_t{0}; i < 32; ++i) {
    files.push_back(
      planned_file(tmp.write_id_file(fmt::format("f{:02}.parquet", i), i)));
  }
  auto server = MockServerPlanRestServer{{files}};
  auto source = spawn_from_iceberg([&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
  });
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto push = CollectingPush{};
  folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
    auto ctx = WatchedOpCtx{dh, reg, scope};
    co_await source->start(ctx);
    REQUIRE_EQUAL(ctx.spawned(), size_t{1});
    // Allow the coordinator and prefetch workers to fill the queues before
    // requesting graceful stop without dequeuing anything.
    co_await folly::coro::sleep(std::chrono::milliseconds{500});
    CHECK(not ctx.all_ended());
    co_await source->stop(ctx);
    auto const deadline
      = std::chrono::steady_clock::now() + std::chrono::seconds{10};
    while (not ctx.all_ended()) {
      if (std::chrono::steady_clock::now() > deadline) {
        FAIL("the reader did not end within 10s of `stop()` while its output "
             "queue was full");
      }
      co_await folly::coro::sleep(std::chrono::milliseconds{5});
    }
    auto messages = size_t{0};
    while (source->state() != OperatorState::done) {
      auto msg = co_await source->await_task(dh);
      messages += 1;
      co_await source->process_task(std::move(msg), push, ctx);
    }
    // At most 8 regular messages plus 1 forced DoneMessage.
    CHECK(messages <= size_t{9});
  }));
  CHECK(dh.empty());
  CHECK(push.ids.size() < size_t{32});
}

TEST("from_iceberg fails once deterministically when a prefetched data file "
     "fails") {
  auto tmp = TempParquetDir{};
  auto files = std::vector<MockServerPlanRestServer::PlannedFile>{};
  for (auto i = int64_t{0}; i < 6; ++i) {
    files.push_back(
      planned_file(tmp.write_id_file(fmt::format("f{}.parquet", i), i)));
  }
  // Missing file sorted between f3.parquet and f4.parquet ('_' > '.').
  files.push_back(MockServerPlanRestServer::PlannedFile{
    .path = fmt::format("file://{}", (tmp.dir / "f3_missing.parquet").string()),
    .size_bytes = files.front().size_bytes,
    .start = None{},
    .length = None{},
    .record_count = 1,
  });
  auto server = MockServerPlanRestServer{{files}};
  auto source = spawn_from_iceberg([&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
  });
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto push = CollectingPush{};
  folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
    auto ctx = ScopedOpCtx{dh, reg, scope};
    co_await source->start(ctx);
    while (source->state() != OperatorState::done) {
      auto msg = co_await source->await_task(dh);
      co_await source->process_task(std::move(msg), push, ctx);
    }
  }));
  // Because slots drain strictly in planning order, files 0..3 arrive before
  // the error on f3-missing.parquet, and files 4..5 are never emitted.
  CHECK_EQUAL(push.ids, (std::vector<int64_t>{0, 1, 2, 3}));
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags.front().severity, severity::error);
}

TEST("from_iceberg resumes metadata-counted scan checkpoint consistently") {
  auto tmp = TempParquetDir{};
  auto const table_dir = tmp.dir / "events";
  std::filesystem::create_directories(table_dir / "metadata");
  std::filesystem::create_directories(table_dir / "data");
  auto catalog_res = ::iceberg::InMemoryCatalog::Make(
    "test", tmp.io, fmt::format("file://{}", tmp.dir.string()), {});
  REQUIRE(catalog_res.has_value());
  auto catalog = std::move(*catalog_res);
  REQUIRE(catalog->CreateNamespace(::iceberg::Namespace{.levels = {"db"}}, {})
            .has_value());
  auto schema = std::make_shared<::iceberg::Schema>(
    std::vector<::iceberg::SchemaField>{
      ::iceberg::SchemaField::MakeRequired(1, "id", ::iceberg::int64()),
    },
    /*schema_id=*/0);
  auto ident
    = ::iceberg::TableIdentifier{.ns = {.levels = {"db"}}, .name = "events"};
  auto table_res
    = catalog->CreateTable(ident, schema,
                           ::iceberg::PartitionSpec::Unpartitioned(),
                           ::iceberg::SortOrder::Unsorted(),
                           fmt::format("file://{}", table_dir.string()), {});
  REQUIRE(table_res.has_value());
  auto table = std::move(*table_res);
  // Write `table_slice_size + 5` rows so the metadata-only fast path emits two
  // batches and allows a mid-task checkpoint between batch 1 and batch 2.
  auto const first_batch_rows = uint64_t{defaults::import::table_slice_size};
  auto const tail_rows = uint64_t{5};
  auto const total_rows = first_batch_rows + tail_rows;
  auto const data_path = fmt::format(
    "file://{}", (table_dir / "data" / "part-0.parquet").string());
  auto writer_opts = ::iceberg::DataWriterOptions{
    .path = data_path,
    .schema = schema,
    .spec = ::iceberg::PartitionSpec::Unpartitioned(),
    .partition = {},
    .format = ::iceberg::FileFormatType::kParquet,
    .io = tmp.io,
    .sort_order_id = {},
    .properties = {},
  };
  auto writer = ::iceberg::DataWriter::Make(writer_opts);
  REQUIRE(writer.has_value());
  auto builder = arrow::Int64Builder{};
  tenzir::check(builder.Reserve(static_cast<int64_t>(total_rows)));
  for (auto i = uint64_t{0}; i < total_rows; ++i) {
    builder.UnsafeAppend(static_cast<int64_t>(i));
  }
  auto col = tenzir::check(builder.Finish());
  auto struct_arr = tenzir::check(arrow::StructArray::Make(
    {col}, {arrow::field("id", arrow::int64(), false)}));
  auto c_arr = ArrowArray{};
  tenzir::check(arrow::ExportArray(*struct_arr, &c_arr));
  auto write_status = (*writer)->Write(&c_arr);
  if (c_arr.release != nullptr) {
    c_arr.release(&c_arr);
  }
  REQUIRE(write_status.has_value());
  REQUIRE((*writer)->Close().has_value());
  auto meta = (*writer)->Metadata();
  REQUIRE(meta.has_value());
  auto append = table->NewFastAppend();
  REQUIRE(append.has_value());
  (*append)->AppendFile(meta->data_files.front());
  REQUIRE((*append)->Commit().has_value());
  REQUIRE(table->Refresh().has_value());
  auto const meta_loc = std::string{table->metadata_file_location()};
  constexpr auto file_scheme = std::string_view{"file://"};
  auto const local_meta_path = meta_loc.starts_with(file_scheme)
                                 ? meta_loc.substr(file_scheme.size())
                                 : meta_loc;
  auto meta_stream = std::ifstream{local_meta_path};
  REQUIRE(meta_stream.good());
  auto meta_buf = std::ostringstream{};
  meta_buf << meta_stream.rdbuf();
  auto const table_json = fmt::format(
    R"({{"metadata-location":"{}","metadata":{}}})", meta_loc, meta_buf.str());
  auto server = MockTableRestServer{table_json};
  auto configure = [&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
    REQUIRE(desc.set_projection.has_value());
    (*desc.set_projection)(
      args, Option<ir::OptimizeProjection>{ir::OptimizeProjection{}});
  };
  // Run 1: process SnapshotPinnedMessage + first BatchMessage, checkpoint, stop.
  auto checkpoint = caf::byte_buffer{};
  {
    auto source = spawn_from_iceberg(configure);
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto push = CountingPush{};
    folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
      auto ctx = ScopedOpCtx{dh, reg, scope};
      co_await source->start(ctx);
      for (auto step = 0; step < 2; ++step) {
        auto msg = co_await source->await_task(dh);
        co_await source->process_task(std::move(msg), push, ctx);
      }
      auto serializer = caf::binary_serializer{checkpoint};
      auto saving = Serde{serializer};
      source->snapshot(saving);
      CHECK_EQUAL(push.rows, first_batch_rows);
      co_await source->stop(ctx);
      auto drain = CountingPush{};
      while (source->state() != OperatorState::done) {
        auto msg = co_await source->await_task(dh);
        co_await source->process_task(std::move(msg), drain, ctx);
      }
    }));
    CHECK(dh.empty());
  }
  // Confirm checkpoint persisted `counted_from_metadata == true` and mid-task
  // row offset.
  {
    auto saved = decode_checkpoint(checkpoint);
    CHECK(not saved.done);
    CHECK(saved.counted_from_metadata);
    CHECK_EQUAL(saved.file_index, uint64_t{0});
    CHECK_EQUAL(saved.rows_in_file, first_batch_rows);
    CHECK(not saved.current_file_path.empty());
    CHECK_EQUAL(saved.plan_schema_id, Option<int32_t>{0});
  }
  // Run 2: restore checkpoint and verify the exact remaining 5 rows are emitted.
  {
    auto restored = spawn_from_iceberg(configure);
    {
      auto deserializer = caf::binary_deserializer{checkpoint};
      auto loading = Serde{deserializer};
      restored->snapshot(loading);
    }
    auto dh = collecting_diagnostic_handler{};
    auto reg = registry{};
    auto push = CountingPush{};
    folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
      auto ctx = ScopedOpCtx{dh, reg, scope};
      co_await restored->start(ctx);
      while (restored->state() != OperatorState::done) {
        auto msg = co_await restored->await_task(dh);
        co_await restored->process_task(std::move(msg), push, ctx);
      }
    }));
    CHECK(dh.empty());
    CHECK_EQUAL(push.rows, tail_rows);
  }
}

TEST("from_iceberg emits restart hint when live history is cut") {
  // Table whose only snapshot (200) points to a missing parent (199),
  // simulating snapshot expiration or a truncated catalog history.
  auto const table_json = std::string{
    R"({"metadata-location":"file:///tmp/mock/metadata/v2.metadata.json",)"
    R"("metadata":{"format-version":2,"table-uuid":"33333333-4444-5555-6666-777777777777",)"
    R"("location":"file:///tmp/mock","last-sequence-number":2,"last-updated-ms":1704067200000,)"
    R"("last-column-id":1,"current-schema-id":0,)"
    R"("schemas":[{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"long"}]}],)"
    R"("default-spec-id":0,"partition-specs":[{"spec-id":0,"fields":[]}],)"
    R"("last-partition-id":999,"default-sort-order-id":0,"sort-orders":[{"order-id":0,"fields":[]}],)"
    R"("properties":{},"current-snapshot-id":200,)"
    R"("snapshots":[{"snapshot-id":200,"parent-snapshot-id":199,"sequence-number":2,"timestamp-ms":1704067200000,)"
    R"("summary":{"operation":"append"},"manifest-list":"file:///tmp/mock/snap-200.avro","schema-id":0}],)"
    R"("snapshot-log":[{"timestamp-ms":1704067200000,"snapshot-id":200}],"metadata-log":[]}})"};
  auto server = MockTableRestServer{table_json};
  auto source = spawn_from_iceberg([&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
    as<std::function<void(Any&, located<bool>)>>(
      find_named(desc, "live").setter)(args, located{true, location::unknown});
    as<std::function<void(Any&, located<int64_t>)>>(
      find_named(desc, "from_snapshot_id").setter)(
      args, located{int64_t{100}, location::unknown});
  });
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto push = UnusedPush{};
  folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
    auto ctx = ScopedOpCtx{dh, reg, scope};
    co_await source->start(ctx);
    while (source->state() != OperatorState::done) {
      auto msg = co_await source->await_task(dh);
      co_await source->process_task(std::move(msg), push, ctx);
    }
  }));
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags.front().severity, severity::error);
  CHECK(diags.front().message.contains("cannot verify"));
  REQUIRE_EQUAL(diags.front().notes.size(), size_t{1});
  CHECK_EQUAL(diags.front().notes.front().kind, diagnostic_note_kind::hint);
  CHECK(diags.front().notes.front().message.contains("from_snapshot_id"));
}

TEST("from_iceberg rejects a resume whose in-progress file left the plan") {
  auto tmp = TempParquetDir{};
  auto file_a = planned_file(tmp.write_id_file("a.parquet", 0));
  auto file_b
    = planned_file(tmp.write_ids_file("b.parquet", 1, 20'000), 20'000);
  // The second plan still has completed file `a`, but not `b`, in which the
  // checkpoint lies.
  auto server = MockServerPlanRestServer{{{file_a, file_b}, {file_a}}};
  auto configure = [&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
  };
  auto source = spawn_from_iceberg(configure);
  auto dh = collecting_diagnostic_handler{};
  auto push = CollectingPush{};
  auto [checkpoint, emitted] = run_to_checkpoint(*source, dh, push, [&] {
    return push.ids.size() > size_t{1};
  });
  CHECK(dh.empty());
  auto saved = decode_checkpoint(checkpoint);
  REQUIRE_EQUAL(saved.file_index, uint64_t{1});
  REQUIRE(saved.rows_in_file > uint64_t{0});
  auto restored = spawn_from_iceberg(configure);
  restore_from(*restored, checkpoint);
  auto rest = CollectingPush{};
  run_to_end(*restored, dh, rest);
  CHECK(rest.ids.empty());
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags.front().severity, severity::error);
  CHECK(diags.front().message.contains("is missing from the planned scan"));
}

TEST("from_iceberg rejects a mid-file resume past the end of the file") {
  auto tmp = TempParquetDir{};
  auto long_b
    = planned_file(tmp.write_ids_file("b.parquet", 0, 20'000), 20'000);
  auto server = MockServerPlanRestServer{{{long_b}}};
  auto configure = [&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
  };
  auto source = spawn_from_iceberg(configure);
  auto dh = collecting_diagnostic_handler{};
  auto push = CollectingPush{};
  auto [checkpoint, emitted] = run_to_checkpoint(*source, dh, push, [&] {
    return not push.ids.empty();
  });
  CHECK(dh.empty());
  // Move the checkpointed offset past the end of the same file, which is what
  // a file with fewer rows under the same path and range looks like.
  auto saved = decode_checkpoint(checkpoint);
  REQUIRE(saved.rows_in_file > uint64_t{0});
  REQUIRE_EQUAL(saved.current_file_path, long_b.path);
  saved.rows_in_file = 50'000;
  auto restored = spawn_from_iceberg(configure);
  restore_from(*restored, encode_checkpoint(std::move(saved)));
  auto rest = CollectingPush{};
  run_to_end(*restored, dh, rest);
  CHECK(rest.ids.empty());
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags.front().severity, severity::error);
  CHECK(diags.front().message.contains("before the checkpointed offset"));
}

TEST("from_iceberg resumes mid-file with the rest of a pushed limit") {
  auto tmp = TempParquetDir{};
  auto file_a
    = planned_file(tmp.write_ids_file("a.parquet", 0, 20'000), 20'000);
  auto file_b
    = planned_file(tmp.write_ids_file("b.parquet", 20'000, 20'000), 20'000);
  auto server = MockServerPlanRestServer{{{file_a, file_b}}};
  auto const limit = uint64_t{10'000};
  auto configure = [&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
    REQUIRE(desc.set_limit.has_value());
    (*desc.set_limit)(args, Option<uint64_t>{limit});
  };
  auto source = spawn_from_iceberg(configure);
  auto dh = collecting_diagnostic_handler{};
  auto push = CollectingPush{};
  auto [checkpoint, emitted] = run_to_checkpoint(*source, dh, push, [&] {
    return not push.ids.empty();
  });
  CHECK(dh.empty());
  REQUIRE(emitted.size() < limit);
  auto saved = decode_checkpoint(checkpoint);
  CHECK_EQUAL(saved.remaining, Option<uint64_t>{limit - emitted.size()});
  auto restored = spawn_from_iceberg(configure);
  restore_from(*restored, checkpoint);
  auto rest = CollectingPush{};
  run_to_end(*restored, dh, rest);
  CHECK(dh.empty());
  emitted.insert(emitted.end(), rest.ids.begin(), rest.ids.end());
  CHECK_EQUAL(emitted, ids_between(0, static_cast<int64_t>(limit)));
}

TEST("from_iceberg resumes a filtered read with the rest of its limit") {
  auto tmp = TempParquetDir{};
  auto file = planned_file(tmp.write_ids_file("a.parquet", 0, 20'000), 20'000);
  auto server = MockServerPlanRestServer{{{file}}};
  auto const limit = uint64_t{10'000};
  auto configure = [&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
    // Drops the first 100 rows of the first batch.
    set_filter_arg(desc, args, "id >= 100");
    REQUIRE(desc.set_limit.has_value());
    (*desc.set_limit)(args, Option<uint64_t>{limit});
  };
  auto source = spawn_from_iceberg(configure);
  auto dh = collecting_diagnostic_handler{};
  auto push = CollectingPush{};
  auto [checkpoint, emitted] = run_to_checkpoint(*source, dh, push, [&] {
    return not push.ids.empty();
  });
  CHECK(dh.empty());
  REQUIRE(not emitted.empty());
  REQUIRE(emitted.size() < limit);
  auto saved = decode_checkpoint(checkpoint);
  // The position counts raw rows; the limit counts emitted ones.
  CHECK_EQUAL(saved.rows_in_file, uint64_t{emitted.size()} + 100);
  CHECK_EQUAL(saved.remaining, Option<uint64_t>{limit - emitted.size()});
  auto restored = spawn_from_iceberg(configure);
  restore_from(*restored, checkpoint);
  auto rest = CollectingPush{};
  run_to_end(*restored, dh, rest);
  CHECK(dh.empty());
  emitted.insert(emitted.end(), rest.ids.begin(), rest.ids.end());
  CHECK_EQUAL(emitted, ids_between(100, 100 + static_cast<int64_t>(limit)));
}

TEST("from_iceberg fails restore when metadata-only count is unavailable") {
  auto tmp = TempParquetDir{};
  auto file = planned_file(tmp.write_id_file("a.parquet", 1));
  // Server-side `/plan` never uses the local metadata-count shortcut, so
  // restoring a checkpoint with `counted_from_metadata == true` must fail
  // instead of silently falling back to a per-file task layout.
  auto server = MockServerPlanRestServer{{{file}}};
  auto configure = [&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
    REQUIRE(desc.set_projection.has_value());
    (*desc.set_projection)(
      args, Option<ir::OptimizeProjection>{ir::OptimizeProjection{}});
  };
  auto cp = encode_checkpoint(Checkpoint{
    .table_uuid = "22222222-3333-4444-5555-666666666666",
    .done = false,
    .pinned_snapshot_id = 100,
    .last_snapshot_id = None{},
    .active_to_snapshot_id = None{},
    .file_index = 0,
    .rows_in_file = 1,
    .current_file_path = "file:///tmp/mock/snap-100.avro",
    .current_file_start = 0,
    .current_file_length = 0,
    .completed_files_digest = {},
    .counted_from_metadata = true,
    .remaining = None{},
    .plan_schema_id = 0,
  });
  auto restored = spawn_from_iceberg(configure);
  restore_from(*restored, cp);
  auto dh = collecting_diagnostic_handler{};
  auto push = CountingPush{};
  run_to_end(*restored, dh, push);
  CHECK_EQUAL(push.rows, uint64_t{0});
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags.front().severity, severity::error);
  CHECK(diags.front().message.contains("checkpointed metadata-only row count "
                                       "is unavailable on restore"));
}

TEST("from_iceberg resumes with the schema of the original plan") {
  auto tmp = TempParquetDir{};
  auto file = planned_file(tmp.write_ids_file("a.parquet", 0, 20'000), 20'000);
  // The current schema renamed `id` to `ident` after snapshot 100 was written.
  // A fresh read uses schema 1 (`ident`); a restore must keep schema 1 even
  // though it pins snapshot 100 (whose historical schema 0 still says `id`).
  auto server = MockServerPlanRestServer{{{file}}, std::string{"ident"}};
  auto configure = [&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
    set_filter_arg(desc, args, "ident >= 5");
  };
  auto source = spawn_from_iceberg(configure);
  auto dh = collecting_diagnostic_handler{};
  auto push = CollectingPush{};
  push.field = "ident";
  auto [checkpoint, emitted] = run_to_checkpoint(*source, dh, push, [&] {
    return not push.ids.empty();
  });
  CHECK(dh.empty());
  REQUIRE(not emitted.empty());
  auto saved = decode_checkpoint(checkpoint);
  CHECK_EQUAL(saved.plan_schema_id, Option<int32_t>{1});
  REQUIRE(saved.rows_in_file > uint64_t{0});
  auto restored = spawn_from_iceberg(configure);
  restore_from(*restored, checkpoint);
  auto rest = CollectingPush{};
  rest.field = "ident";
  run_to_end(*restored, dh, rest);
  CHECK(dh.empty());
  emitted.insert(emitted.end(), rest.ids.begin(), rest.ids.end());
  CHECK_EQUAL(emitted, ids_between(5, 20'000));
  // A checkpoint whose schema the table no longer has cannot resume.
  saved.plan_schema_id = 7;
  auto orphaned = spawn_from_iceberg(configure);
  restore_from(*orphaned, encode_checkpoint(std::move(saved)));
  auto none = CollectingPush{};
  none.field = "ident";
  run_to_end(*orphaned, dh, none);
  CHECK(none.ids.empty());
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags.front().severity, severity::error);
  CHECK(diags.front().message.contains("is no longer in the table metadata"));
}

TEST("from_iceberg pins the snapshot and its plan schema together") {
  auto tmp = TempParquetDir{};
  auto file = planned_file(tmp.write_ids_file("a.parquet", 0, 20'000), 20'000);
  auto server = MockServerPlanRestServer{{{file}}, std::string{"ident"}};
  auto configure = [&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
    set_filter_arg(desc, args, "ident >= 5");
  };
  auto source = spawn_from_iceberg(configure);
  auto dh = collecting_diagnostic_handler{};
  auto push = CollectingPush{};
  push.field = "ident";
  // Checkpoint right after the very first message (SnapshotPinnedMessage),
  // before any row is emitted.
  auto checkpoint = caf::byte_buffer{};
  auto reg = registry{};
  folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
    auto ctx = ScopedOpCtx{dh, reg, scope};
    co_await source->start(ctx);
    auto msg = co_await source->await_task(dh);
    co_await source->process_task(std::move(msg), push, ctx);
    {
      auto serializer = caf::binary_serializer{checkpoint};
      auto saving = Serde{serializer};
      source->snapshot(saving);
    }
    co_await source->stop(ctx);
    while (source->state() != OperatorState::done) {
      auto next = co_await source->await_task(dh);
      co_await source->process_task(std::move(next), push, ctx);
    }
  }));
  CHECK(dh.empty());
  auto saved = decode_checkpoint(checkpoint);
  CHECK_EQUAL(saved.pinned_snapshot_id, Option<int64_t>{100});
  CHECK_EQUAL(saved.plan_schema_id, Option<int32_t>{1});
  CHECK_EQUAL(saved.file_index, uint64_t{0});
  CHECK_EQUAL(saved.rows_in_file, uint64_t{0});
  auto restored = spawn_from_iceberg(configure);
  restore_from(*restored, checkpoint);
  auto rest = CollectingPush{};
  rest.field = "ident";
  run_to_end(*restored, dh, rest);
  CHECK(dh.empty());
  CHECK_EQUAL(rest.ids, ids_between(5, 20'000));
}

TEST("from_iceberg defers planning errors until earlier prefetched files "
     "drain") {
  auto tmp = TempParquetDir{};
  auto const table_dir = tmp.dir / "events";
  std::filesystem::create_directories(table_dir / "metadata");
  std::filesystem::create_directories(table_dir / "data");
  auto catalog_res = ::iceberg::InMemoryCatalog::Make(
    "test", tmp.io, fmt::format("file://{}", tmp.dir.string()), {});
  REQUIRE(catalog_res.has_value());
  auto catalog = std::move(*catalog_res);
  REQUIRE(catalog->CreateNamespace(::iceberg::Namespace{.levels = {"db"}}, {})
            .has_value());
  auto schema = std::make_shared<::iceberg::Schema>(
    std::vector<::iceberg::SchemaField>{
      ::iceberg::SchemaField::MakeRequired(1, "id", ::iceberg::int64()),
    },
    /*schema_id=*/0);
  auto ident
    = ::iceberg::TableIdentifier{.ns = {.levels = {"db"}}, .name = "events"};
  auto table_res
    = catalog->CreateTable(ident, schema,
                           ::iceberg::PartitionSpec::Unpartitioned(),
                           ::iceberg::SortOrder::Unsorted(),
                           fmt::format("file://{}", table_dir.string()), {});
  REQUIRE(table_res.has_value());
  auto table = std::move(*table_res);
  auto write_and_append = [&](std::string_view filename, int64_t first_id) {
    auto const data_path
      = fmt::format("file://{}", (table_dir / "data" / filename).string());
    auto writer_opts = ::iceberg::DataWriterOptions{
      .path = data_path,
      .schema = schema,
      .spec = ::iceberg::PartitionSpec::Unpartitioned(),
      .partition = {},
      .format = ::iceberg::FileFormatType::kParquet,
      .io = tmp.io,
      .sort_order_id = {},
      .properties = {},
    };
    auto writer = ::iceberg::DataWriter::Make(writer_opts);
    REQUIRE(writer.has_value());
    auto builder = arrow::Int64Builder{};
    tenzir::check(builder.Append(first_id));
    tenzir::check(builder.Append(first_id + 1));
    auto col = tenzir::check(builder.Finish());
    auto struct_arr = tenzir::check(arrow::StructArray::Make(
      {col}, {arrow::field("id", arrow::int64(), false)}));
    auto c_arr = ArrowArray{};
    tenzir::check(arrow::ExportArray(*struct_arr, &c_arr));
    auto write_status = (*writer)->Write(&c_arr);
    if (c_arr.release != nullptr) {
      c_arr.release(&c_arr);
    }
    REQUIRE(write_status.has_value());
    REQUIRE((*writer)->Close().has_value());
    auto meta = (*writer)->Metadata();
    REQUIRE(meta.has_value());
    // Understate `record_count` as 1 (while the file holds 2 rows) so that a
    // `head 2` scan still calls `plan->next_task()` a second time and hits the
    // broken second manifest before draining file 0.
    meta->data_files.front()->record_count = 1;
    auto append = table->NewFastAppend();
    REQUIRE(append.has_value());
    (*append)->AppendFile(meta->data_files.front());
    REQUIRE((*append)->Commit().has_value());
    REQUIRE(table->Refresh().has_value());
    return table->metadata()->current_snapshot_id;
  };
  // Iceberg's `PlanFilesStream` opens manifests in batches of 32. Commit 33
  // single-file manifests so batch 1 (manifests 0..31) plans cleanly and batch
  // 2 (manifest 32) fails mid-scan while earlier files are still draining.
  constexpr auto good_manifests = size_t{32};
  auto rows_by_snapshot = std::unordered_map<int64_t, int64_t>{};
  for (auto i = size_t{0}; i < good_manifests + 1; ++i) {
    auto const first_id = static_cast<int64_t>(i * 2);
    auto const snap_id
      = write_and_append(fmt::format("part-{:02}.parquet", i), first_id);
    rows_by_snapshot.emplace(snap_id, first_id);
  }
  auto snap_res
    = table->metadata()->SnapshotById(table->metadata()->current_snapshot_id);
  REQUIRE(snap_res.has_value());
  auto snap_reader = ::iceberg::SnapshotReader{snap_res->get()};
  auto manifests = snap_reader.Manifests(tmp.io);
  REQUIRE(manifests.has_value());
  REQUIRE_EQUAL(manifests->size(), good_manifests + 1);
  auto expected_rows = std::vector<int64_t>{};
  expected_rows.reserve(good_manifests * 2);
  for (auto i = size_t{0}; i < good_manifests; ++i) {
    REQUIRE((*manifests)[i].added_snapshot_id.has_value());
    auto const first_id
      = rows_by_snapshot.at(*(*manifests)[i].added_snapshot_id);
    expected_rows.push_back(first_id);
    expected_rows.push_back(first_id + 1);
  }
  // Delete the 33rd manifest in manifest-list order so batch 1 yields all 32
  // data files and batch 2 fails on the next `plan->next_task()` call.
  REQUIRE(
    tmp.io->DeleteFile((*manifests)[good_manifests].manifest_path).has_value());
  constexpr auto file_scheme = std::string_view{"file://"};
  auto const meta_loc = std::string{table->metadata_file_location()};
  auto const local_meta_path = meta_loc.starts_with(file_scheme)
                                 ? meta_loc.substr(file_scheme.size())
                                 : meta_loc;
  auto meta_stream = std::ifstream{local_meta_path};
  REQUIRE(meta_stream.good());
  auto meta_buf = std::ostringstream{};
  meta_buf << meta_stream.rdbuf();
  auto const table_json = fmt::format(
    R"({{"metadata-location":"{}","metadata":{}}})", meta_loc, meta_buf.str());
  auto server = MockTableRestServer{table_json};
  // 1. Without a limit, all 64 rows from manifests 0..31 emit in exact order
  //    before the deferred planning error on manifest 32 is reported once.
  {
    auto source = spawn_from_iceberg([&](Description const& desc, Any& args) {
      set_catalog_arg(desc, args, server.uri());
    });
    auto dh = collecting_diagnostic_handler{};
    auto push = CollectingPush{};
    run_to_end(*source, dh, push);
    CHECK_EQUAL(push.ids, expected_rows);
    auto diags = std::move(dh).collect();
    REQUIRE_EQUAL(diags.size(), size_t{1});
    CHECK_EQUAL(diags.front().severity, severity::error);
    CHECK(diags.front().message.contains("failed to plan Iceberg scan"));
  }
  // 2. With `head 64` satisfied by manifests 0..31 (whose understated
  //    `record_count = 1` still forces `refill()` to probe manifest 32), the
  //    deferred planning error is suppressed and all 64 rows emit cleanly.
  {
    auto source = spawn_from_iceberg([&](Description const& desc, Any& args) {
      set_catalog_arg(desc, args, server.uri());
      REQUIRE(desc.set_limit.has_value());
      (*desc.set_limit)(args, Option<uint64_t>{good_manifests * 2});
    });
    auto dh = collecting_diagnostic_handler{};
    auto push = CollectingPush{};
    run_to_end(*source, dh, push);
    CHECK(dh.empty());
    CHECK_EQUAL(push.ids, expected_rows);
  }
}

TEST("from_iceberg stops promptly while prefetch workers wait on full "
     "per-file queues") {
  auto tmp = TempParquetDir{};
  auto files = std::vector<MockServerPlanRestServer::PlannedFile>{};
  // Each 50,000-row file produces 7 batches (> per_file_queue_capacity == 4),
  // filling both the operator queue (8) and every prefetched file's queue.
  for (auto i = int64_t{0}; i < 6; ++i) {
    files.push_back(planned_file(
      tmp.write_ids_file(fmt::format("f{:02}.parquet", i), i * 50'000, 50'000),
      50'000));
  }
  auto server = MockServerPlanRestServer{{files}};
  auto source = spawn_from_iceberg([&](Description const& desc, Any& args) {
    set_catalog_arg(desc, args, server.uri());
  });
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto push = CollectingPush{};
  folly::coro::blockingWait(async_scope([&](AsyncScope& scope) -> Task<void> {
    auto ctx = WatchedOpCtx{dh, reg, scope};
    co_await source->start(ctx);
    REQUIRE_EQUAL(ctx.spawned(), size_t{1});
    co_await folly::coro::sleep(std::chrono::milliseconds{500});
    CHECK(not ctx.all_ended());
    co_await source->stop(ctx);
    auto const deadline
      = std::chrono::steady_clock::now() + std::chrono::seconds{10};
    while (not ctx.all_ended()) {
      if (std::chrono::steady_clock::now() > deadline) {
        FAIL("the reader did not end within 10s of `stop()` while prefetch "
             "workers waited on full per-file queues");
      }
      co_await folly::coro::sleep(std::chrono::milliseconds{5});
    }
    auto messages = size_t{0};
    while (source->state() != OperatorState::done) {
      auto msg = co_await source->await_task(dh);
      messages += 1;
      co_await source->process_task(std::move(msg), push, ctx);
    }
    CHECK(messages <= size_t{9});
  }));
  CHECK(dh.empty());
  CHECK(push.ids.size() < size_t{6 * 50'000});
}
