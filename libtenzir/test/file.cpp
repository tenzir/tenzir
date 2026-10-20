//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/as_bytes.hpp>
#include <tenzir/config.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/file.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/uuid.hpp>

#include <fmt/format.h>
#include <folly/File.h>
#include <folly/FileUtil.h>
#include <sys/stat.h>

#include <algorithm>
#include <array>
#include <barrier>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <latch>
#include <limits>
#include <set>
#include <thread>

namespace tenzir {
namespace {

struct FileFixture {
  FileFixture() {
    std::filesystem::create_directory(directory);
  }

  ~FileFixture() {
    auto error = std::error_code{};
    std::filesystem::remove_all(directory, error);
  }

  FileFixture(FileFixture const&) = delete;
  FileFixture(FileFixture&&) = delete;
  auto operator=(FileFixture const&) -> FileFixture& = delete;
  auto operator=(FileFixture&&) -> FileFixture& = delete;

  auto entries() const -> std::set<std::filesystem::path> {
    auto result = std::set<std::filesystem::path>{};
    for (auto const& entry : std::filesystem::directory_iterator{directory}) {
      result.insert(entry.path().filename());
    }
    return result;
  }

  std::filesystem::path directory
    = std::filesystem::temp_directory_path()
      / fmt::format("tenzir-file-{}", uuid::random());
};

TEST("whole and exact-prefix reads preserve bytes") {
  auto fixture = FileFixture{};
  auto path = fixture.directory / "nested" / "data";
  auto content = std::string(150'000, 'x');
  content[7] = '\0';
  content[100'000] = '\n';
  REQUIRE(write_file(path, as_bytes(content)).is_ok());
  auto whole = read_file(path);
  REQUIRE(whole.is_ok());
  CHECK(std::ranges::equal(whole.unwrap(), as_bytes(content)));
  auto text = read_file(path, FileReadOptions{});
  REQUIRE(text.is_ok());
  CHECK(std::ranges::equal(text.unwrap(), as_bytes(content)));
  auto streamed
    = read_file(path, FileReadOptions{.source = FileSource::stream});
  REQUIRE(streamed.is_ok());
  CHECK(std::ranges::equal(streamed.unwrap(), as_bytes(content)));
  // Catalog probes read eight bytes, not the whole file, and allow a suffix.
  auto prefix = std::array<std::byte, 8>{};
  REQUIRE(read_file(path, prefix).is_ok());
  CHECK(std::ranges::equal(prefix, as_bytes(content).first(8)));
  REQUIRE(read_file(path, std::span<std::byte>{}).is_ok());
  auto oversized = std::vector<std::byte>(content.size() + 1);
  CHECK(read_file(path, oversized).is_err());
  // Direct writes truncate existing files; subsequent reads see the new EOF.
  REQUIRE(write_file(path, as_bytes(std::string_view{"short"})).is_ok());
  text = read_file(path);
  REQUIRE(text.is_ok());
  CHECK(std::ranges::equal(text.unwrap(), as_bytes(std::string_view{"short"})));
  auto empty = fixture.directory / "empty";
  REQUIRE(write_file(empty, {}).is_ok());
  auto empty_bytes = read_file(empty);
  REQUIRE(empty_bytes.is_ok());
  CHECK(empty_bytes.unwrap().empty());
  auto empty_text
    = read_file(empty, {.source = FileSource::stream, .max_bytes = 0});
  REQUIRE(empty_text.is_ok());
  CHECK(empty_text.unwrap().empty());
  CHECK(read_file(empty, std::span<std::byte>{}).is_ok());
  CHECK(read_file(empty, prefix).is_err());
  auto missing = fixture.directory / "missing";
  CHECK(read_file(missing).is_err());
  CHECK(read_file(missing, {.source = FileSource::stream}).is_err());
  CHECK(read_file(missing, std::span<std::byte>{}).is_err());
}

TEST("owning read limits accept exact sizes and reject excess bytes") {
  auto fixture = FileFixture{};
  auto path = fixture.directory / "data";
  for (auto size :
       std::array<size_t, 6>{0, 1, 65'535, 65'536, 65'537, 150'000}) {
    auto expected = std::vector<std::byte>(size, std::byte{0xff});
    REQUIRE(write_file(path, expected).is_ok());
    for (auto source : {FileSource::regular, FileSource::stream}) {
      for (auto limit :
           {size_t{0}, size, size + 1, std::numeric_limits<size_t>::max()}) {
        auto result = read_file(path, {.source = source, .max_bytes = limit});
        if (size > limit) {
          REQUIRE(result.is_err());
          CHECK(result.unwrap_err().contains("exceeds read limit"));
        } else {
          REQUIRE(result.is_ok());
          CHECK(result.unwrap() == expected);
        }
      }
      if (size > 0) {
        CHECK(
          read_file(path, {.source = source, .max_bytes = size - 1}).is_err());
      }
    }
  }
  // A large stat size must not cause a stat-sized allocation for a tiny limit.
  std::filesystem::resize_file(path, 64 * 1024 * 1024);
  CHECK(read_file(path, {.max_bytes = 1}).is_err());
  CHECK(read_file(fixture.directory / "missing", {.max_bytes = 0}).is_err());
}

TEST("non-regular input files are rejected without blocking") {
  auto fixture = FileFixture{};
  auto fifo = fixture.directory / "fifo";
  REQUIRE_EQUAL(::mkfifo(fifo.c_str(), 0600), 0);
  auto link = fixture.directory / "link";
  std::filesystem::create_symlink(fifo, link);
  // No writer is opened: a blocking read-only open would hang here.
  for (auto const& path :
       {fifo, link, fixture.directory, std::filesystem::path{"/dev/null"}}) {
    CHECK(read_file(path).is_err());
    CHECK(read_file(path, {.max_bytes = 0}).is_err());
    CHECK(read_file(path, std::span<std::byte>{}).is_err());
  }
  CHECK(write_file(fixture.directory, {}).is_err());
}

TEST("explicit streams consume FIFO producers to EOF and accept devices") {
  auto fixture = FileFixture{};
  auto fifo = fixture.directory / "fifo";
  REQUIRE_EQUAL(::mkfifo(fifo.c_str(), 0600), 0);
  auto link = fixture.directory / "link";
  std::filesystem::create_symlink(fifo, link);
  auto content = std::string(150'000, 'x');
  content[7] = '\0';
  content[100'000] = '\n';
  auto missing = fixture.directory / "missing";
  for (auto const& path : {fifo, link, missing}) {
    for (auto limit : {Option<size_t>{None{}}, Option<size_t>{content.size()},
                       Option<size_t>{content.size() - 1}}) {
      auto written = false;
      // The FIFO itself coordinates the blocking opens. The reader consumes
      // multiple chunks and waits for the producer to close, without sleeps.
      // Use an independent peer so a write_file failure cannot strand the read
      // helper in open.
      auto producer = std::jthread{[&] {
        auto output = std::ofstream{fifo};
        // Send the final byte separately: the limit covers the entire stream,
        // not just the initial producer write.
        output.write(content.data(),
                     static_cast<std::streamsize>(content.size() - 1));
        output.flush();
        output.put(content.back());
        output.close();
        written = static_cast<bool>(output);
      }};
      auto text
        = read_file(path, {.source = FileSource::stream, .max_bytes = limit});
      if (not text or path == missing) {
        // A failed open leaves the producer waiting for a reader. Keep both
        // ends open during rescue, then drain until the producer has finished.
        // The missing-path iteration exercises this cleanup deterministically.
        auto fd = folly::openNoInt(fifo.c_str(), O_RDWR | O_CLOEXEC);
        REQUIRE(fd >= 0);
        auto keepalive = folly::File{fd, true};
        auto opened = std::latch{1};
        auto drain = std::jthread{[&] {
          auto input = std::ifstream{fifo};
          opened.count_down();
          input.ignore(std::numeric_limits<std::streamsize>::max());
        }};
        opened.wait();
        producer.join();
        CHECK(keepalive.closeNoThrow());
        drain.join();
      } else {
        producer.join();
      }
      REQUIRE(written);
      if (path == missing or limit.value_or(content.size()) < content.size()) {
        CHECK(text.is_err());
      } else {
        REQUIRE(text.is_ok());
        CHECK(std::ranges::equal(text.unwrap(), as_bytes(content)));
      }
    }
  }
  auto empty
    = read_file("/dev/null", {.source = FileSource::stream, .max_bytes = 0});
  REQUIRE(empty.is_ok());
  CHECK(empty.unwrap().empty());
}

TEST("stream prefixes need not wait for EOF and still detect short input") {
  auto fixture = FileFixture{};
  auto fifo = fixture.directory / "fifo";
  REQUIRE_EQUAL(::mkfifo(fifo.c_str(), 0600), 0);
  auto content = std::string{"prefix\0xy", 9};
  for (auto size : {size_t{8}, size_t{9}, size_t{10}}) {
    auto release = std::latch{1};
    auto written = false;
    auto producer = std::jthread{[&] {
      auto output = std::ofstream{fifo};
      output.write(content.data(),
                   static_cast<std::streamsize>(content.size()));
      output.flush();
      // A full prefix must return while the producer is still open. A short
      // prefix instead needs producer EOF to report the incomplete read.
      if (size <= content.size()) {
        release.wait();
      }
      output.close();
      written = static_cast<bool>(output);
    }};
    auto destination = std::vector<std::byte>(size);
    auto result = read_file(fifo, destination, FileSource::stream);
    release.count_down();
    if (not result) {
      // Rescue a peer if the helper regresses to rejecting before open. The
      // entire tiny payload fits in the FIFO without a drain thread.
      auto fd = folly::openNoInt(fifo.c_str(), O_RDWR | O_CLOEXEC);
      REQUIRE(fd >= 0);
      auto keepalive = folly::File{fd, true};
      producer.join();
    } else {
      producer.join();
    }
    REQUIRE(written);
    if (size > content.size()) {
      CHECK(result.is_err());
    } else {
      REQUIRE(result.is_ok());
      CHECK(std::ranges::equal(destination, as_bytes(content).first(size)));
    }
  }
  CHECK(
    read_file("/dev/null", std::span<std::byte>{}, FileSource::stream).is_ok());
}

TEST("stream read errors are not successful empty input") {
  auto fixture = FileFixture{};
  auto text = read_file(fixture.directory,
                        FileReadOptions{.source = FileSource::stream});
  REQUIRE(text.is_err());
  CHECK(text.unwrap_err().starts_with("failed to read "));
}

TEST("direct writes preserve synchronous FIFO output") {
  auto fixture = FileFixture{};
  auto fifo = fixture.directory / "fifo";
  REQUIRE_EQUAL(::mkfifo(fifo.c_str(), 0600), 0);
  for (auto const& path : {fifo, fixture.directory}) {
    auto observed = std::string{};
    auto opened = std::latch{1};
    // The FIFO coordinates normal reader/writer opens without sleeps.
    auto reader = std::jthread{[&] {
      auto input = std::ifstream{fifo};
      opened.count_down();
      observed.assign(std::istreambuf_iterator<char>{input}, {});
    }};
    auto result = write_file(path, as_bytes(std::string_view{"endpoint"}));
    if (not result or path == fixture.directory) {
      // Release a reader whose writer failed before opening the FIFO. The
      // directory iteration tests this path without altering the helper API.
      auto fd = folly::openNoInt(fifo.c_str(), O_RDWR | O_CLOEXEC);
      REQUIRE(fd >= 0);
      auto keepalive = folly::File{fd, true};
      opened.wait();
      CHECK(keepalive.closeNoThrow());
    }
    reader.join();
    if (path == fixture.directory) {
      CHECK(result.is_err());
      CHECK(observed.empty());
    } else {
      REQUIRE(result.is_ok());
      CHECK_EQUAL(observed, "endpoint");
    }
  }
}

TEST("direct writes follow symlinks and atomic writes replace them") {
  auto fixture = FileFixture{};
  auto target = fixture.directory / "target";
  auto link = fixture.directory / "link";
  REQUIRE(write_file(target, as_bytes(std::string_view{"initial"})).is_ok());
  std::filesystem::create_symlink(target, link);
  REQUIRE(write_file(link, as_bytes(std::string_view{"direct"})).is_ok());
  CHECK(std::filesystem::is_symlink(link));
  auto text = read_file(target);
  REQUIRE(text.is_ok());
  CHECK(
    std::ranges::equal(text.unwrap(), as_bytes(std::string_view{"direct"})));
  auto linked = read_file(link);
  REQUIRE(linked.is_ok());
  CHECK(std::ranges::equal(linked.unwrap(), as_bytes(text.unwrap())));
  REQUIRE(replace_file(link, as_bytes(std::string_view{"atomic"})).is_ok());
  CHECK(not std::filesystem::is_symlink(link));
  text = read_file(link);
  REQUIRE(text.is_ok());
  CHECK(
    std::ranges::equal(text.unwrap(), as_bytes(std::string_view{"atomic"})));
  text = read_file(target);
  REQUIRE(text.is_ok());
  CHECK(
    std::ranges::equal(text.unwrap(), as_bytes(std::string_view{"direct"})));
}

TEST("atomic writes create parents and preserve umask without fixed "
     "temporaries") {
  auto fixture = FileFixture{};
  auto path = fixture.directory / "nested" / "data";
  REQUIRE(replace_file(path, as_bytes(std::string_view{"old"})).is_ok());
  auto stale = path;
  stale += ".tmp";
  REQUIRE(write_file(stale, as_bytes(std::string_view{"leave me"})).is_ok());
  // Both writers must let the kernel apply the same inherited umask. Run this
  // suite under umask 077 as well; do not change process-wide umask in a test.
  CHECK(std::filesystem::status(path).permissions()
        == std::filesystem::status(stale).permissions());
  REQUIRE(replace_file(path, as_bytes(std::string_view{"new"})).is_ok());
  auto text = read_file(path);
  REQUIRE(text.is_ok());
  CHECK(std::ranges::equal(text.unwrap(), as_bytes(std::string_view{"new"})));
  text = read_file(stale);
  REQUIRE(text.is_ok());
  CHECK(
    std::ranges::equal(text.unwrap(), as_bytes(std::string_view{"leave me"})));
  REQUIRE(replace_file(path, {}).is_ok());
  text = read_file(path);
  REQUIRE(text.is_ok());
  CHECK(text.unwrap().empty());
  CHECK_EQUAL(
    std::distance(std::filesystem::directory_iterator{path.parent_path()},
                  std::filesystem::directory_iterator{}),
    2);
}

TEST("failed publication cleans up its temporary and preserves the "
     "destination") {
  auto fixture = FileFixture{};
  auto destination = fixture.directory / "destination";
  auto sentinel = destination / "sentinel";
  REQUIRE(write_file(sentinel, as_bytes(std::string_view{"keep"})).is_ok());
  auto before = fixture.entries();
  CHECK(replace_file(destination, as_bytes(std::string_view{"new"})).is_err());
  CHECK(fixture.entries() == before);
  auto text = read_file(sentinel);
  REQUIRE(text.is_ok());
  CHECK(std::ranges::equal(text.unwrap(), as_bytes(std::string_view{"keep"})));
  // A regular file in the parent path must not be overwritten either.
  CHECK(replace_file(sentinel / "child", {}).is_err());
  CHECK(write_file(sentinel / "child", {}).is_err());
  CHECK(fixture.entries() == before);
#if TENZIR_LINUX
  if (std::filesystem::exists("/dev/full")) {
    CHECK(write_file("/dev/full", as_bytes(std::string_view{"x"})).is_err());
  }
#endif
}

TEST("dash is a literal filename for generic file helpers") {
  auto fixture = FileFixture{};
  auto previous = std::filesystem::current_path();
  auto restore = detail::scope_guard{[&]() noexcept {
    auto error = std::error_code{};
    std::filesystem::current_path(previous, error);
  }};
  std::filesystem::current_path(fixture.directory);
  REQUIRE(write_file("-", as_bytes(std::string_view{"literal"})).is_ok());
  auto text = read_file("-");
  REQUIRE(text.is_ok());
  CHECK(
    std::ranges::equal(text.unwrap(), as_bytes(std::string_view{"literal"})));
  auto bytes = read_file("-");
  REQUIRE(bytes.is_ok());
  CHECK(std::ranges::equal(bytes.unwrap(), as_bytes(text.unwrap())));
  auto prefix = std::array<std::byte, 3>{};
  REQUIRE(read_file("-", prefix).is_ok());
  CHECK(std::ranges::equal(prefix, as_bytes(text.unwrap()).first(3)));
  REQUIRE(replace_file("-", as_bytes(std::string_view{"replacement"})).is_ok());
  text = read_file("-");
  REQUIRE(text.is_ok());
  CHECK(std::ranges::equal(text.unwrap(),
                           as_bytes(std::string_view{"replacement"})));
}

TEST("whole-file reads stay complete across concurrent atomic publications") {
  auto fixture = FileFixture{};
  auto target = fixture.directory / "current";
  auto small = std::vector<std::byte>(17, std::byte{0x11});
  auto large = std::vector<std::byte>(4096, std::byte{0x22});
  REQUIRE(replace_file(target, small).is_ok());
  // Stress coverage: either complete revision is valid, regardless of which
  // writer wins. This makes no scheduling assumption or deterministic claim
  // about reproducing the old pathname-size/open race.
  auto rendezvous = std::barrier{3};
  constexpr auto iterations = 500;
  auto first_ok = true;
  auto second_ok = true;
  auto publish = [&](std::span<std::byte const> bytes, bool& ok) {
    for (auto i = 0; i < iterations; ++i) {
      rendezvous.arrive_and_wait();
      auto result = replace_file(target, bytes);
      ok = ok and result.is_ok();
      rendezvous.arrive_and_wait();
    }
  };
  auto first = std::jthread{[&] {
    publish(small, first_ok);
  }};
  auto second = std::jthread{[&] {
    publish(large, second_ok);
  }};
  auto complete = true;
  for (auto i = 0; i < iterations; ++i) {
    rendezvous.arrive_and_wait();
    auto bytes = read_file(target);
    complete = complete and bytes.is_ok()
               and (bytes.unwrap() == small or bytes.unwrap() == large);
    rendezvous.arrive_and_wait();
  }
  first.join();
  second.join();
  CHECK(first_ok);
  CHECK(second_ok);
  CHECK(complete);
  CHECK(fixture.entries() == std::set<std::filesystem::path>{"current"});
}

#if TENZIR_LINUX
TEST("owning reads consume proc files with zero reported size") {
  auto path = std::filesystem::path{"/proc/self/status"};
  if (not std::filesystem::exists(path)) {
    return;
  }
  CHECK_EQUAL(std::filesystem::file_size(path), 0u);
  CHECK(read_file(path, {.max_bytes = 0}).is_err());
  CHECK(read_file(path, std::span<std::byte>{}).is_ok());
  auto prefix = std::array<std::byte, 8>{};
  CHECK(read_file(path, prefix).is_ok());
  auto text = read_file(path);
  REQUIRE(text.is_ok());
  REQUIRE(not text.unwrap().empty());
  auto view = std::string_view{
    reinterpret_cast<char const*>(text.unwrap().data()), text.unwrap().size()};
  CHECK(view.contains("Name:"));
  CHECK(view.contains("Pid:"));
  auto streamed
    = read_file(path, FileReadOptions{.source = FileSource::stream});
  REQUIRE(streamed.is_ok());
  REQUIRE(not streamed.unwrap().empty());
  view
    = std::string_view{reinterpret_cast<char const*>(streamed.unwrap().data()),
                       streamed.unwrap().size()};
  CHECK(view.contains("Name:"));
  CHECK(view.contains("Pid:"));
}
#endif

} // namespace
} // namespace tenzir
