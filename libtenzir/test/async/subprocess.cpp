//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/subprocess.hpp"

#include <folly/File.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/io/IOBufQueue.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <string>
#include <unistd.h>

// Intentially after the other includes to avoid leaking the CHECK macro
// into upstream headers.
#include "tenzir/test/test.hpp"

namespace tenzir {

namespace {

auto to_string(chunk_ptr const& chunk) -> std::string {
  return {reinterpret_cast<char const*>(chunk->data()), chunk->size()};
}

} // namespace

TEST("input pipe writes data and output pipe reads eof") {
  auto fds = std::array<int, 2>{};
  auto result = pipe(fds.data());
  check_eq(result, 0);
  folly::coro::blockingWait([&]() -> Task<void> {
    auto input = WritePipe{folly::File{fds[1], true}, 0};
    auto output = ReadPipe{folly::File{fds[0], true}, 1};
    co_await folly::coro::collectAll(
      [&]() -> Task<void> {
        co_await input.write(chunk::copy("hello", 5));
        co_await input.close();
      }(),
      [&]() -> Task<void> {
        auto chunk = co_await output.read_chunk();
        check(chunk.is_some());
        check_eq(to_string(*chunk), "hello");
        auto none = co_await output.read_chunk();
        check(none.is_none());
        check(output.is_eof());
      }());
  }());
}

TEST("output pipe read_some appends to iobuf queue") {
  auto fds = std::array<int, 2>{};
  auto result = pipe(fds.data());
  check_eq(result, 0);
  folly::coro::blockingWait([&]() -> Task<void> {
    auto input = WritePipe{folly::File{fds[1], true}, 0};
    auto output = ReadPipe{folly::File{fds[0], true}, 1};
    auto queue = folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()};
    co_await input.write(chunk::copy("world", 5));
    co_await input.close();
    auto bytes_read = co_await output.read_some(queue, 1, 8);
    check_eq(bytes_read, 5u);
    MESSAGE("read_some: bytes_read={} empty={} chainLength={}", bytes_read,
            queue.empty(), queue.chainLength());
    auto* front = queue.front();
    MESSAGE("read_some: front={} front_length={}",
            fmt::ptr(static_cast<void const*>(front)),
            front ? front->length() : 0);
    auto moved = queue.move();
    MESSAGE("read_some: moved={} moved_length={} moved_chain_length={}",
            fmt::ptr(static_cast<void const*>(moved.get())),
            moved ? moved->length() : 0,
            moved ? moved->computeChainDataLength() : 0);
    auto data = moved ? moved->coalesce() : folly::ByteRange{};
    MESSAGE("read_some: coalesced_size={} coalesced='{}'", data.size(),
            std::string{reinterpret_cast<char const*>(data.data()),
                        data.size()});
    auto str = std::string{
      reinterpret_cast<char const*>(data.data()),
      data.size(),
    };
    check_eq(str, "world");
  }());
}

TEST("subprocess spawns cat with pipe wrappers") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto spec = SubprocessSpec{};
    spec.argv = {"cat"};
    spec.stdin_mode = PipeMode::pipe;
    spec.stdout_mode = PipeMode::pipe;
    spec.use_path = true;
    spec.kill_child_on_destruction = true;
    auto subprocess = co_await Subprocess::spawn(std::move(spec));
    auto stdin_pipe = subprocess.stdin_pipe();
    auto stdout_pipe = subprocess.stdout_pipe();
    check(stdin_pipe.is_some());
    check(stdout_pipe.is_some());
    co_await (*stdin_pipe).write(chunk::copy("hello", 5));
    co_await (*stdin_pipe).close();
    auto chunk = co_await (*stdout_pipe).read_chunk();
    check(chunk.is_some());
    check_eq(to_string(*chunk), "hello");
    auto none = co_await (*stdout_pipe).read_chunk();
    check(none.is_none());
    auto return_code = co_await subprocess.wait();
    check(return_code.succeeded());
  }());
}

TEST("subprocess terminate_or_kill stops a running child") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto spec = SubprocessSpec{};
    spec.argv = {"sleep", "30"};
    spec.use_path = true;
    spec.kill_child_on_destruction = true;
    auto subprocess = co_await Subprocess::spawn(std::move(spec));
    auto poll = co_await subprocess.wait_timeout(std::chrono::milliseconds{0});
    check(poll.running());
    auto return_code
      = co_await subprocess.terminate_or_kill(std::chrono::milliseconds{100});
    check(not return_code.running());
    check(return_code.killed() or return_code.exited());
    check(not return_code.succeeded());
  }());
}

TEST("subprocess signals cached process group after wait") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto spec = SubprocessSpec{};
    spec.argv = {"sh", "-c", "sleep 30 & echo $!; exit 0"};
    spec.stdout_mode = PipeMode::pipe;
    spec.use_path = true;
    spec.process_group_leader = true;
    spec.kill_child_on_destruction = true;
    auto subprocess = co_await Subprocess::spawn(std::move(spec));
    auto stdout_pipe = subprocess.stdout_pipe();
    check(stdout_pipe.is_some());
    auto chunk = co_await (*stdout_pipe).read_chunk();
    check(chunk.is_some());
    auto child_pid = std::strtol(to_string(*chunk).c_str(), nullptr, 10);
    check(child_pid > 0);
    auto return_code = co_await subprocess.wait();
    check(return_code.succeeded());
    co_await subprocess.send_signal_to_process_group(SIGKILL);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (std::chrono::steady_clock::now() < deadline) {
      if (::kill(child_pid, 0) != 0 and errno == ESRCH) {
        co_return;
      }
      co_await sleep(std::chrono::milliseconds{10});
    }
    FAIL("background child {} still existed after process-group kill",
         child_pid);
  }());
}

} // namespace tenzir
