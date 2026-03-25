//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/subprocess.hpp"

#include "tenzir/as_bytes.hpp"
#include "tenzir/async/blocking_executor.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"

#include <folly/FileUtil.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

namespace tenzir {

namespace {

auto throw_last_system_error(char const* operation) -> void {
  throw std::system_error{errno, std::generic_category(), operation};
}

auto has_fd(std::vector<int> const& fds, int fd) -> bool {
  return std::find(fds.begin(), fds.end(), fd) != fds.end();
}

auto configure_pipe_mode(folly::Subprocess::Options& options, int child_fd,
                         PipeMode mode) -> void {
  switch (mode) {
    case PipeMode::inherit:
      return;
    case PipeMode::pipe:
      options.fd(child_fd, folly::Subprocess::PIPE);
      return;
    case PipeMode::dev_null:
      options.fd(child_fd, folly::Subprocess::DEV_NULL);
      return;
  }
  TENZIR_UNREACHABLE();
}

struct SpawnResult {
  folly::Subprocess subprocess;
  std::vector<folly::Subprocess::ChildPipe> pipes;
};

} // namespace

WritePipe::WritePipe(folly::File pipe, int child_fd)
  : pipe_{std::move(pipe)}, child_fd_{child_fd} {
  TENZIR_ASSERT(pipe_);
  TENZIR_ASSERT(child_fd_ >= 0);
}

WritePipe::~WritePipe() = default;

auto WritePipe::write(std::span<std::byte const> bytes) -> Task<void> {
  if (bytes.empty()) {
    co_return;
  }
  TENZIR_ASSERT(pipe_);
  TENZIR_ASSERT(not closed_);
  auto buffer = std::vector<std::byte>{bytes.begin(), bytes.end()};
  auto fd = pipe_.fd();
  co_await spawn_blocking([fd, buffer = std::move(buffer)]() {
    auto written = folly::writeFull(fd, buffer.data(), buffer.size());
    if (written == -1) {
      throw_last_system_error("writeFull");
    }
    if (detail::narrow<size_t>(written) != buffer.size()) {
      throw std::system_error{EIO, std::generic_category(),
                              "writeFull returned a short write"};
    }
  });
}

auto WritePipe::write(chunk_ptr chunk) -> Task<void> {
  if (not chunk or chunk->size() == 0) {
    co_return;
  }
  co_await write(as_bytes(chunk));
}

auto WritePipe::close() -> Task<void> {
  if (closed_) {
    co_return;
  }
  closed_ = true;
  auto pipe = std::move(pipe_);
  co_await spawn_blocking([pipe = std::move(pipe)]() mutable {
    if (pipe) {
      pipe.close();
    }
  });
}

auto WritePipe::is_closed() const noexcept -> bool {
  return closed_ or not pipe_;
}

auto WritePipe::child_fd() const noexcept -> int {
  return child_fd_;
}

auto WritePipe::native_fd() const noexcept -> int {
  return pipe_.fd();
}

ReadPipe::ReadPipe(folly::File pipe, int child_fd)
  : pipe_{std::move(pipe)}, child_fd_{child_fd} {
  TENZIR_ASSERT(pipe_);
  TENZIR_ASSERT(child_fd_ >= 0);
}

ReadPipe::~ReadPipe() = default;

auto ReadPipe::read_chunk(size_t max_bytes) -> Task<Option<chunk_ptr>> {
  if (closed_ or eof_) {
    co_return None{};
  }
  TENZIR_ASSERT(pipe_);
  TENZIR_ASSERT(not closed_);
  TENZIR_ASSERT(max_bytes > 0);
  auto fd = pipe_.fd();
  auto result = co_await spawn_blocking([fd, max_bytes]() -> Option<chunk_ptr> {
    auto buffer = std::vector<std::byte>{};
    buffer.resize(max_bytes);
    auto bytes_read = folly::readNoInt(fd, buffer.data(), buffer.size());
    if (bytes_read == -1) {
      throw_last_system_error("readNoInt");
    }
    if (bytes_read == 0) {
      return None{};
    }
    buffer.resize(detail::narrow<size_t>(bytes_read));
    return chunk::make(std::move(buffer));
  });
  if (result.is_none()) {
    eof_ = true;
  }
  co_return result;
}

auto ReadPipe::read_some(folly::IOBufQueue& out, size_t min_read,
                         size_t new_alloc) -> Task<size_t> {
  auto max_bytes = std::max(min_read, new_alloc);
  if (max_bytes == 0) {
    co_return 0;
  }
  auto next = co_await read_chunk(max_bytes);
  if (next.is_none()) {
    co_return 0;
  }
  auto bytes = as_bytes(*next);
  out.append(folly::IOBuf::copyBuffer(bytes.data(), bytes.size()));
  co_return bytes.size();
}

auto ReadPipe::close() -> Task<void> {
  if (closed_) {
    co_return;
  }
  closed_ = true;
  auto pipe = std::move(pipe_);
  co_await spawn_blocking([pipe = std::move(pipe)]() mutable {
    if (pipe) {
      pipe.close();
    }
  });
}

auto ReadPipe::is_closed() const noexcept -> bool {
  return closed_ or not pipe_;
}

auto ReadPipe::is_eof() const noexcept -> bool {
  return eof_;
}

auto ReadPipe::child_fd() const noexcept -> int {
  return child_fd_;
}

auto ReadPipe::native_fd() const noexcept -> int {
  return pipe_.fd();
}

Subprocess::Subprocess(folly::Subprocess subprocess,
                       Option<WritePipe> stdin_pipe,
                       Option<ReadPipe> stdout_pipe,
                       Option<ReadPipe> stderr_pipe,
                       std::vector<WritePipe> input_pipes,
                       std::vector<ReadPipe> output_pipes)
  : subprocess_{std::move(subprocess)},
    stdin_pipe_{std::move(stdin_pipe)},
    stdout_pipe_{std::move(stdout_pipe)},
    stderr_pipe_{std::move(stderr_pipe)},
    input_pipes_{std::move(input_pipes)},
    output_pipes_{std::move(output_pipes)} {
}

Subprocess::~Subprocess() = default;

auto Subprocess::spawn(SubprocessSpec spec) -> Task<Subprocess> {
  if (spec.argv.empty()) {
    throw std::invalid_argument{"SubprocessSpec.argv must not be empty"};
  }
  auto pipe_input_fds = spec.pipe_input_fds;
  auto pipe_output_fds = spec.pipe_output_fds;
  auto result = co_await spawn_blocking([spec = std::move(spec)]() mutable {
    auto options = folly::Subprocess::Options{};
    configure_pipe_mode(options, STDIN_FILENO, spec.stdin_mode);
    configure_pipe_mode(options, STDOUT_FILENO, spec.stdout_mode);
    configure_pipe_mode(options, STDERR_FILENO, spec.stderr_mode);
    for (auto child_fd : spec.pipe_input_fds) {
      options.fd(child_fd, folly::Subprocess::PIPE_IN);
    }
    for (auto child_fd : spec.pipe_output_fds) {
      options.fd(child_fd, folly::Subprocess::PIPE_OUT);
    }
    if (spec.use_path) {
      options.usePath();
    }
    if (spec.cwd) {
      options.chdir(*spec.cwd);
    }
    if (spec.process_group_leader) {
      options.processGroupLeader();
    }
    if (spec.kill_child_on_destruction) {
      options.killChildOnDestruction();
    }
    auto* env = spec.env ? &*spec.env : nullptr;
    auto subprocess = folly::Subprocess{spec.argv, options, nullptr, env};
    auto pipes = subprocess.takeOwnershipOfPipes();
    return SpawnResult{
      .subprocess = std::move(subprocess),
      .pipes = std::move(pipes),
    };
  });
  auto stdin_pipe = Option<WritePipe>{None{}};
  auto stdout_pipe = Option<ReadPipe>{None{}};
  auto stderr_pipe = Option<ReadPipe>{None{}};
  auto input_pipes = std::vector<WritePipe>{};
  auto output_pipes = std::vector<ReadPipe>{};
  for (auto& child_pipe : result.pipes) {
    auto child_fd = child_pipe.childFd;
    if (child_fd == STDIN_FILENO) {
      stdin_pipe = WritePipe{std::move(child_pipe.pipe), child_fd};
      continue;
    }
    if (child_fd == STDOUT_FILENO) {
      stdout_pipe = ReadPipe{std::move(child_pipe.pipe), child_fd};
      continue;
    }
    if (child_fd == STDERR_FILENO) {
      stderr_pipe = ReadPipe{std::move(child_pipe.pipe), child_fd};
      continue;
    }
    if (has_fd(pipe_input_fds, child_fd)) {
      input_pipes.emplace_back(std::move(child_pipe.pipe), child_fd);
      continue;
    }
    if (has_fd(pipe_output_fds, child_fd)) {
      output_pipes.emplace_back(std::move(child_pipe.pipe), child_fd);
      continue;
    }
    TENZIR_UNREACHABLE();
  }
  co_return Subprocess{
    std::move(result.subprocess), std::move(stdin_pipe),
    std::move(stdout_pipe),       std::move(stderr_pipe),
    std::move(input_pipes),       std::move(output_pipes),
  };
}

auto Subprocess::stdin_pipe() -> Option<WritePipe&> {
  if (stdin_pipe_.is_none()) {
    return None{};
  }
  return *stdin_pipe_;
}

auto Subprocess::stdout_pipe() -> Option<ReadPipe&> {
  if (stdout_pipe_.is_none()) {
    return None{};
  }
  return *stdout_pipe_;
}

auto Subprocess::stderr_pipe() -> Option<ReadPipe&> {
  if (stderr_pipe_.is_none()) {
    return None{};
  }
  return *stderr_pipe_;
}

auto Subprocess::input_pipe(int child_fd) -> Option<WritePipe&> {
  if (stdin_pipe_.is_some() and stdin_pipe_->child_fd() == child_fd) {
    return *stdin_pipe_;
  }
  for (auto& pipe : input_pipes_) {
    if (pipe.child_fd() == child_fd) {
      return pipe;
    }
  }
  return None{};
}

auto Subprocess::output_pipe(int child_fd) -> Option<ReadPipe&> {
  if (stdout_pipe_.is_some() and stdout_pipe_->child_fd() == child_fd) {
    return *stdout_pipe_;
  }
  if (stderr_pipe_.is_some() and stderr_pipe_->child_fd() == child_fd) {
    return *stderr_pipe_;
  }
  for (auto& pipe : output_pipes_) {
    if (pipe.child_fd() == child_fd) {
      return pipe;
    }
  }
  return None{};
}

auto Subprocess::wait() -> Task<folly::ProcessReturnCode> {
  co_return co_await spawn_blocking([this]() {
    return subprocess_.wait();
  });
}

auto Subprocess::wait_timeout(std::chrono::milliseconds timeout)
  -> Task<folly::ProcessReturnCode> {
  co_return co_await spawn_blocking([this, timeout]() {
    return subprocess_.waitTimeout(timeout);
  });
}

auto Subprocess::terminate_or_kill(std::chrono::milliseconds timeout)
  -> Task<folly::ProcessReturnCode> {
  co_return co_await spawn_blocking([this, timeout]() {
    return subprocess_.terminateOrKill(timeout);
  });
}

auto Subprocess::terminate_or_kill_process_group(
  std::chrono::milliseconds timeout) -> Task<folly::ProcessReturnCode> {
  co_return co_await spawn_blocking([this, timeout]() {
    auto pid = subprocess_.pid();
    if (pid <= 0) {
      throw std::logic_error{"cannot signal a finished subprocess group"};
    }
    if (timeout.count() > 0) {
      auto result = ::kill(-pid, SIGTERM);
      if (result != 0 and errno != ESRCH) {
        throw_last_system_error("kill");
      }
      auto return_code = subprocess_.waitTimeout(timeout);
      if (not return_code.running()) {
        return return_code;
      }
    }
    auto result = ::kill(-pid, SIGKILL);
    if (result != 0 and errno != ESRCH) {
      throw_last_system_error("kill");
    }
    return subprocess_.wait();
  });
}

auto Subprocess::send_signal(int signal) -> Task<void> {
  co_await spawn_blocking([this, signal]() {
    subprocess_.sendSignal(signal);
  });
}

auto Subprocess::send_signal_to_process_group(int signal) -> Task<void> {
  co_await spawn_blocking([this, signal]() {
    auto pid = subprocess_.pid();
    if (pid <= 0) {
      throw std::logic_error{"cannot signal a finished subprocess group"};
    }
    auto result = ::kill(-pid, signal);
    if (result != 0) {
      throw_last_system_error("kill");
    }
  });
}

auto Subprocess::pid() const noexcept -> pid_t {
  return subprocess_.pid();
}

auto Subprocess::return_code() const noexcept -> folly::ProcessReturnCode {
  return subprocess_.returnCode();
}

} // namespace tenzir
