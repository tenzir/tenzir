//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/detail/env.hpp>
#include <vast/detail/fdinbuf.hpp>
#include <vast/detail/fdoutbuf.hpp>
#include <vast/detail/file_path_to_parser.hpp>
#include <vast/detail/posix.hpp>
#include <vast/detail/string.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>

#include <caf/detail/scope_guard.hpp>
#include <caf/error.hpp>

#include <chrono>
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <memory>
#include <string_view>
#include <unistd.h>
#include <variant>

namespace vast::plugins::file {
namespace {

const auto std_io_path = std::string{"-"};

using file_description_wrapper = std::shared_ptr<int>;

/// Tries to expand paths that start with a `~`. Returns the original input
/// string if no expansion occurs.
auto expand_path(std::string path) -> std::string {
  if (path.empty() || path[0] != '~') {
    return path;
  }
  if (path.size() == 1 || path[1] == '/') {
    auto home = detail::getenv("HOME");
    if (home) {
      path.replace(0, 1, *home);
    }
  }
  return path;
}

class writer {
public:
  virtual ~writer() = default;

  virtual auto flush() -> caf::error = 0;

  virtual auto write(std::span<const std::byte> buffer) -> caf::error = 0;

  virtual auto close() -> caf::error = 0;
};

class fd_writer final : public writer {
public:
  fd_writer(int fd, bool close) : fd_{fd}, close_{close} {
  }

  ~fd_writer() override {
    if (auto error = close()) {
      VAST_WARN("closing failed in destructor: {}", error);
    }
  }

  auto flush() -> caf::error override {
    return {};
  }

  auto write(std::span<const std::byte> buffer) -> caf::error override {
    while (!buffer.empty()) {
      auto written = ::write(fd_, buffer.data(), buffer.size());
      if (written == -1) {
        if (errno != EINTR) {
          return caf::make_error(ec::filesystem_error,
                                 fmt::format("file could not be written to: {}",
                                             detail::describe_errno()));
        }
        continue;
      }
      VAST_ASSERT(written > 0);
      buffer = buffer.subspan(written);
    }
    return {};
  }

  auto close() -> caf::error override {
    if (close_ && fd_ != -1) {
      auto failed = ::close(fd_) != 0;
      fd_ = -1;
      if (failed) {
        return caf::make_error(ec::filesystem_error,
                               fmt::format("file could not be closed: {}",
                                           detail::describe_errno()));
      }
    }
    return {};
  }

private:
  int fd_;
  bool close_;
};

class file_writer final : public writer {
public:
  explicit file_writer(std::FILE* file) : file_{file} {
  }

  ~file_writer() override {
    if (auto error = close()) {
      VAST_WARN("closing failed in destructor: {}", error);
    }
  }

  auto flush() -> caf::error override {
    if (std::fflush(file_) != 0) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("file could not be flushed: {}",
                                         detail::describe_errno()));
    }
    return {};
  }

  auto write(std::span<const std::byte> buffer) -> caf::error override {
    auto written = std::fwrite(buffer.data(), 1, buffer.size(), file_);
    if (written != buffer.size()) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("file could not be written to: {}",
                                         detail::describe_errno()));
    }
    return {};
  }

  auto close() -> caf::error override {
    if (file_) {
      auto failed = std::fclose(file_) != 0;
      file_ = nullptr;
      if (failed) {
        return caf::make_error(ec::filesystem_error,
                               fmt::format("file could not be closed: {}",
                                           detail::describe_errno()));
      }
    }
    return {};
  }

private:
  std::FILE* file_;
};

class plugin : public virtual loader_plugin, public virtual saver_plugin {
public:
  static constexpr auto max_chunk_size = size_t{16384};

  auto
  make_loader(std::span<std::string const> args, operator_control_plane&) const
    -> caf::expected<generator<chunk_ptr>> override {
    auto read_timeout = read_timeout_;
    auto path = std::string{};
    auto following = false;
    auto mmap = false;
    auto is_socket = false;
    for (auto i = size_t{0}; i < args.size(); ++i) {
      const auto& arg = args[i];
      if (arg == "--timeout") {
        if (i + 1 == args.size()) {
          return caf::make_error(ec::syntax_error,
                                 fmt::format("missing duration value"));
        }
        if (auto parsed_duration = to<vast::duration>(args[i + 1])) {
          read_timeout = std::chrono::duration_cast<std::chrono::milliseconds>(
            *parsed_duration);
          ++i;
        } else {
          return caf::make_error(ec::syntax_error,
                                 fmt::format("could not parse duration: {}",
                                             args[i + 1]));
        }
      } else if (arg == "-") {
        path = std_io_path;
      } else if (arg == "--mmap") {
        mmap = true;
      } else if (arg == "-f" || arg == "--follow") {
        following = true;
      } else if (not arg.starts_with("-")) {
        std::error_code err{};
        auto expanded = expand_path(arg);
        auto status = std::filesystem::status(expanded, err);
        if (err) {
          return caf::make_error(ec::parse_error,
                                 fmt::format("could not access file {}: {}",
                                             expanded, err));
        }
        is_socket = (status.type() == std::filesystem::file_type::socket);
        if (path == std_io_path) {
          return caf::make_error(ec::parse_error,
                                 fmt::format("file argument {} can not be "
                                             "combined with "
                                             "stdin file argument",
                                             expanded));
        }
        path = std::move(expanded);
      } else {
        return caf::make_error(
          ec::invalid_argument,
          fmt::format("unexpected argument for 'file' connector: {}", arg));
      }
    }
    if (path.empty()) {
      return caf::make_error(ec::syntax_error,
                             fmt::format("no file specified"));
    }
    if (mmap) {
      if (path == std_io_path) {
        return caf::make_error(ec::filesystem_error, "cannot mmap STDIN");
      }
      if (following) {
        return caf::make_error(ec::filesystem_error,
                               "cannot use `--follow` with `--mmap`");
      }
      auto chunk = chunk::mmap(path);
      if (not chunk)
        return std::move(chunk.error());
      return std::invoke(
        [](chunk_ptr chunk) mutable -> generator<chunk_ptr> {
          co_yield std::move(chunk);
        },
        std::move(*chunk));
    }
    auto fd = file_description_wrapper(new int(STDIN_FILENO), [](auto* fd) {
      std::default_delete<int>()(fd);
    });
    if (is_socket) {
      if (path == std_io_path) {
        return caf::make_error(ec::filesystem_error, "cannot use STDIN as UNIX "
                                                     "domain socket");
      }
      auto uds = detail::unix_domain_socket::connect(path);
      if (!uds) {
        return caf::make_error(ec::filesystem_error,
                               fmt::format("unable to connect to UNIX domain "
                                           "socket at {}",
                                           path));
      }
      fd = file_description_wrapper(new int(uds.fd), [](auto fd) {
        if (*fd != -1) {
          ::close(*fd);
        }
        std::default_delete<int>()(fd);
      });
    } else {
      if (path != std_io_path) {
        fd = file_description_wrapper(new int(::open(path.c_str(), O_RDONLY)),
                                      [](auto fd) {
                                        if (*fd != -1) {
                                          ::close(*fd);
                                        }
                                        std::default_delete<int>()(fd);
                                      });
        if (*fd == -1) {
          return caf::make_error(ec::filesystem_error,
                                 fmt::format("open(2) for file {} failed {}:",
                                             path, detail::describe_errno()));
        }
      }
    }
    return std::invoke(
      [](auto timeout, auto fd, auto following) -> generator<chunk_ptr> {
        auto in_buf = detail::fdinbuf(*fd, max_chunk_size);
        in_buf.read_timeout() = timeout;
        auto current_data = std::vector<std::byte>{};
        current_data.reserve(max_chunk_size);
        auto eof_reached = false;
        while (following or not eof_reached) {
          auto current_char = in_buf.sbumpc();
          if (current_char != detail::fdinbuf::traits_type::eof()) {
            current_data.emplace_back(static_cast<std::byte>(current_char));
          }
          if (current_char == detail::fdinbuf::traits_type::eof()
              or current_data.size() == max_chunk_size) {
            eof_reached = (current_char == detail::fdinbuf::traits_type::eof()
                           and not in_buf.timed_out());
            if (eof_reached and current_data.empty() and not following) {
              break;
            }
            auto chunk = chunk::make(std::exchange(current_data, {}));
            co_yield std::move(chunk);
            if (eof_reached and not following) {
              break;
            }
            current_data.reserve(max_chunk_size);
          }
        }
        co_return;
      },
      read_timeout, std::move(fd), following);
  }

  auto default_parser(std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    for (auto i = size_t{0}; i < args.size(); ++i) {
      const auto& arg = args[i];
      if (arg == "-") {
        break;
      }
      if (arg == "--timeout") {
        ++i;
      } else if (!arg.starts_with("-")) {
        return {detail::file_path_to_parser(arg), {}};
      }
    }
    return {"json", {}};
  }

  auto initialize(const record&, const record& global_config)
    -> caf::error override {
    auto timeout
      = try_get<vast::duration>(global_config, "vast.import.read-timeout");
    if (!timeout.engaged()) {
      return std::move(timeout.error());
    }
    if (timeout->has_value()) {
      read_timeout_
        = std::chrono::duration_cast<std::chrono::milliseconds>(**timeout);
    }
    return caf::none;
  }

  auto make_saver(std::span<std::string const> args,
                  [[maybe_unused]] printer_info info,
                  operator_control_plane& ctrl) const
    -> caf::expected<saver> override {
    auto appending = false;
    auto real_time = false;
    auto is_socket = false;
    auto path = std::string{};
    for (auto i = size_t{0}; i < args.size(); ++i) {
      const auto& arg = args[i];
      if (arg == "-") {
        path = std_io_path;
      } else if (arg == "-a" || arg == "--append") {
        appending = true;
      } else if (arg == "--real-time") {
        real_time = true;
      } else if (arg == "--uds") {
        is_socket = true;
      } else if (not arg.starts_with("-")) {
        if (path == std_io_path) {
          return caf::make_error(ec::parse_error,
                                 fmt::format("file argument {} can not be "
                                             "combined with "
                                             "stdout file argument",
                                             arg));
        }
        path = expand_path(arg);
      } else {
        return caf::make_error(
          ec::invalid_argument,
          fmt::format("unexpected argument for 'file' sink: {}", arg));
      }
    }
    // This should not be a `shared_ptr`, but we have to capture `stream` in
    // the returned `std::function`. Also, we capture it in a guard that calls
    // `.close()` to not silently discard errors that occur during destruction.
    auto stream = std::shared_ptr<writer>{};
    if (is_socket) {
      if (path == std_io_path) {
        return caf::make_error(ec::filesystem_error,
                               "cannot use STDOUT as UNIX domain socket");
      }
      auto uds = detail::unix_domain_socket::connect(path);
      if (!uds) {
        return caf::make_error(ec::filesystem_error,
                               fmt::format("unable to connect to UNIX "
                                           "domain "
                                           "socket at {}",
                                           path));
      }
      // TODO: This won't do any additional buffering. Is this what we want?
      stream = std::make_shared<fd_writer>(uds.fd, true);
    } else if (path == std_io_path) {
      stream = std::make_shared<fd_writer>(STDOUT_FILENO, false);
    } else {
      auto directory = std::filesystem::path{path}.parent_path();
      if (!directory.empty()) {
        try {
          std::filesystem::create_directories(directory);
        } catch (const std::exception& exc) {
          return caf::make_error(ec::filesystem_error,
                                 fmt::format("could not create directory {}: "
                                             "{}",
                                             directory, exc.what()));
        }
      }
      // We use `fopen` because we want buffered writes.
      auto handle = std::fopen(path.c_str(), appending ? "ab" : "wb");
      if (handle == nullptr) {
        return caf::make_error(ec::filesystem_error,
                               fmt::format("failed to open {}: {}", path,
                                           detail::describe_errno()));
      }
      stream = std::make_shared<file_writer>(handle);
    }
    VAST_ASSERT(stream);
    auto guard = caf::detail::make_scope_guard([&ctrl, stream] {
      if (auto error = stream->close()) {
        ctrl.abort(std::move(error));
      }
    });
    return [&ctrl, real_time, stream = std::move(stream),
            guard = std::make_shared<decltype(guard)>(std::move(guard))](
             chunk_ptr chunk) {
      if (!chunk || chunk->size() == 0) {
        return;
      }
      if (auto error = stream->write(std::span{chunk->data(), chunk->size()})) {
        ctrl.abort(std::move(error));
        return;
      }
      if (real_time) {
        if (auto error = stream->flush()) {
          ctrl.abort(std::move(error));
          return;
        }
      }
    };
  }

  auto default_printer([[maybe_unused]] std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"json", {}};
  }

  auto saver_does_joining() const -> bool override {
    return true;
  }

  auto name() const -> std::string override {
    return "file";
  }

private:
  std::chrono::milliseconds read_timeout_{vast::defaults::import::read_timeout};
};

} // namespace
} // namespace vast::plugins::file

namespace vast::plugins::stdin_ {
namespace {

class plugin : public virtual file::plugin {
public:
  auto make_loader(std::span<std::string const> args,
                   operator_control_plane& ctrl) const
    -> caf::expected<generator<chunk_ptr>> override {
    std::vector<std::string> new_args = {"-"};
    new_args.insert(new_args.end(), args.begin(), args.end());
    return file::plugin::make_loader(new_args, ctrl);
  }

  auto default_parser([[maybe_unused]] std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"json", {}};
  }

  auto name() const -> std::string override {
    return "stdin";
  }
};

} // namespace
} // namespace vast::plugins::stdin_

namespace vast::plugins::stdout_ {
namespace {

class plugin : public virtual file::plugin {
public:
  auto make_saver(std::span<std::string const> args, printer_info info,
                  operator_control_plane& ctrl) const
    -> caf::expected<saver> override {
    std::vector<std::string> new_args = {"-"};
    new_args.insert(new_args.end(), args.begin(), args.end());
    return file::plugin::make_saver(new_args, std::move(info), ctrl);
  }

  auto default_printer([[maybe_unused]] std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"json", {}};
  }

  auto name() const -> std::string override {
    return "stdout";
  }
};

} // namespace
} // namespace vast::plugins::stdout_

VAST_REGISTER_PLUGIN(vast::plugins::file::plugin)
VAST_REGISTER_PLUGIN(vast::plugins::stdin_::plugin)
VAST_REGISTER_PLUGIN(vast::plugins::stdout_::plugin)
