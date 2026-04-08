//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/option.hpp"

#include <folly/File.h>
#include <folly/Subprocess.h>

#include <chrono>
#include <cstddef>
#include <span>
#include <string>
#include <vector>

namespace folly {
class IOBufQueue;
} // namespace folly

namespace tenzir {

enum class PipeMode {
  inherit,
  pipe,
  dev_null,
};

struct SubprocessSpec {
  std::vector<std::string> argv;
  Option<std::vector<std::string>> env = None{};
  Option<std::string> cwd = None{};
  PipeMode stdin_mode = PipeMode::inherit;
  PipeMode stdout_mode = PipeMode::inherit;
  PipeMode stderr_mode = PipeMode::inherit;
  std::vector<int> pipe_input_fds;
  std::vector<int> pipe_output_fds;
  bool use_path = false;
  bool process_group_leader = false;
  bool kill_child_on_destruction = true;
};

class WritePipe {
public:
  explicit WritePipe(folly::File pipe, int child_fd);
  WritePipe(WritePipe&&) noexcept = default;
  auto operator=(WritePipe&&) noexcept -> WritePipe& = default;
  WritePipe(WritePipe const&) = delete;
  auto operator=(WritePipe const&) -> WritePipe& = delete;
  ~WritePipe();

  auto write(std::span<std::byte const> bytes) -> Task<void>;
  auto write(chunk_ptr chunk) -> Task<void>;
  auto close() -> Task<void>;

  auto is_closed() const noexcept -> bool;
  auto child_fd() const noexcept -> int;
  auto native_fd() const noexcept -> int;

private:
  folly::File pipe_;
  int child_fd_ = -1;
  bool closed_ = false;
};

class ReadPipe {
public:
  explicit ReadPipe(folly::File pipe, int child_fd);
  ReadPipe(ReadPipe&&) noexcept = default;
  auto operator=(ReadPipe&&) noexcept -> ReadPipe& = default;
  ReadPipe(ReadPipe const&) = delete;
  auto operator=(ReadPipe const&) -> ReadPipe& = delete;
  ~ReadPipe();

  auto read_chunk(size_t max_bytes = 64z * 1024) -> Task<Option<chunk_ptr>>;
  auto read_some(folly::IOBufQueue& out, size_t min_read = 1,
                 size_t new_alloc = 64z * 1024) -> Task<size_t>;
  auto close() -> Task<void>;

  auto is_closed() const noexcept -> bool;
  auto is_eof() const noexcept -> bool;
  auto child_fd() const noexcept -> int;
  auto native_fd() const noexcept -> int;

private:
  folly::File pipe_;
  int child_fd_ = -1;
  bool eof_ = false;
  bool closed_ = false;
};

class Subprocess {
public:
  Subprocess(Subprocess&&) noexcept = default;
  auto operator=(Subprocess&&) noexcept -> Subprocess& = default;
  Subprocess(Subprocess const&) = delete;
  auto operator=(Subprocess const&) -> Subprocess& = delete;
  ~Subprocess();

  static auto spawn(SubprocessSpec spec) -> Task<Subprocess>;

  auto stdin_pipe() -> Option<WritePipe&>;
  auto stdout_pipe() -> Option<ReadPipe&>;
  auto stderr_pipe() -> Option<ReadPipe&>;
  auto input_pipe(int child_fd) -> Option<WritePipe&>;
  auto output_pipe(int child_fd) -> Option<ReadPipe&>;

  auto wait() -> Task<folly::ProcessReturnCode>;
  auto wait_timeout(std::chrono::milliseconds timeout)
    -> Task<folly::ProcessReturnCode>;
  auto terminate_or_kill(std::chrono::milliseconds timeout)
    -> Task<folly::ProcessReturnCode>;
  auto terminate_or_kill_process_group(std::chrono::milliseconds timeout)
    -> Task<folly::ProcessReturnCode>;
  auto send_signal(int signal) -> Task<void>;
  auto send_signal_to_process_group(int signal) -> Task<void>;

  auto pid() const noexcept -> pid_t;
  auto return_code() const noexcept -> folly::ProcessReturnCode;

private:
  explicit Subprocess(folly::Subprocess subprocess, pid_t process_group_id,
                      Option<WritePipe> stdin_pipe,
                      Option<ReadPipe> stdout_pipe,
                      Option<ReadPipe> stderr_pipe,
                      std::vector<WritePipe> input_pipes,
                      std::vector<ReadPipe> output_pipes);

  folly::Subprocess subprocess_;
  pid_t process_group_id_ = -1;
  Option<WritePipe> stdin_pipe_ = None{};
  Option<ReadPipe> stdout_pipe_ = None{};
  Option<ReadPipe> stderr_pipe_ = None{};
  std::vector<WritePipe> input_pipes_;
  std::vector<ReadPipe> output_pipes_;
};

} // namespace tenzir
