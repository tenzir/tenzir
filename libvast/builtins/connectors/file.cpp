//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/argument_parser.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/defaults.hpp>
#include <vast/detail/env.hpp>
#include <vast/detail/fdinbuf.hpp>
#include <vast/detail/fdoutbuf.hpp>
#include <vast/detail/file_path_to_parser.hpp>
#include <vast/detail/posix.hpp>
#include <vast/detail/string.hpp>
#include <vast/diagnostics.hpp>
#include <vast/fwd.hpp>
#include <vast/logger.hpp>
#include <vast/parser_interface.hpp>
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

struct loader_args {
  located<std::string> path;
  std::optional<located<std::chrono::milliseconds>> timeout;
  std::optional<location> follow;
  std::optional<location> mmap;

  template <class Inspector>
  friend auto inspect(Inspector& f, loader_args& x) -> bool {
    return f.object(x)
      .pretty_name("loader_args")
      .fields(f.field("path", x.path), f.field("timeout", x.timeout),
              f.field("follow", x.follow), f.field("mmap", x.mmap));
  }
};

struct saver_args {
  located<std::string> path;
  std::optional<location> appending;
  std::optional<location> real_time;
  std::optional<location> uds;

  template <class Inspector>
  friend auto inspect(Inspector& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("saver_args")
      .fields(f.field("path", x.path), f.field("appending", x.appending),
              f.field("real_time", x.real_time), f.field("uds", x.uds));
  }
};

class fd_wrapper {
public:
  fd_wrapper() : fd_{-1}, close_{false} {
  }
  fd_wrapper(int fd, bool close) : fd_{fd}, close_{close} {
  }
  fd_wrapper(const fd_wrapper&) = delete;
  auto operator=(const fd_wrapper&) -> fd_wrapper& = delete;
  fd_wrapper(fd_wrapper&& other) noexcept
    : fd_{other.fd_}, close_{other.close_} {
    other.close_ = false;
  }
  auto operator=(fd_wrapper&& other) noexcept -> fd_wrapper& {
    fd_ = other.fd_;
    close_ = other.close_;
    other.close_ = false;
    return *this;
  }

  ~fd_wrapper() {
    if (close_) {
      if (::close(fd_) != 0) {
        VAST_WARN("failed to close file in destructor: {}",
                  detail::describe_errno());
      }
    }
  }

  operator int() const {
    return fd_;
  }

private:
  int fd_;
  bool close_;
};

class file_loader final : public plugin_loader {
public:
  static constexpr auto max_chunk_size = size_t{16384};

  file_loader() = default;

  explicit file_loader(loader_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    auto make = [](std::chrono::milliseconds timeout, fd_wrapper fd,
                   bool following) -> generator<chunk_ptr> {
      auto in_buf = detail::fdinbuf(fd, max_chunk_size);
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
    };
    // FIXME: This default does not respect config values.
    auto timeout
      = args_.timeout ? args_.timeout->inner : defaults::import::read_timeout;
    if (args_.mmap) {
      auto chunk = chunk::mmap(args_.path.inner);
      if (not chunk) {
        ctrl.diagnostics().emit(
          diagnostic::error("could not mmap file: {}", chunk.error())
            .primary(args_.path.source)
            .done());
        return {};
      }
      return std::invoke(
        [](chunk_ptr chunk) mutable -> generator<chunk_ptr> {
          co_yield std::move(chunk);
        },
        std::move(*chunk));
    }
    if (args_.path.inner == "-") {
      return make(timeout, fd_wrapper{STDIN_FILENO, false}, false);
    }
    auto err = std::error_code{};
    auto status = std::filesystem::status(args_.path.inner, err);
    if (err == std::make_error_code(std::errc::no_such_file_or_directory)) {
      // TODO: Unify and improve error descriptions.
      diagnostic::error("the file `{}` does not exist", args_.path.inner, err)
        .primary(args_.path.source)
        .emit(ctrl.diagnostics());
      return {};
    }
    if (err) {
      diagnostic::error("could not access file `{}`", args_.path.inner, err)
        .primary(args_.path.source)
        .note("{}", err)
        .emit(ctrl.diagnostics());
      return {};
    }
    if (status.type() == std::filesystem::file_type::socket) {
      auto uds = detail::unix_domain_socket::connect(args_.path.inner);
      if (!uds) {
        diagnostic::error("could not connect to UNIX domain socket at {}",
                          args_.path.inner)
          .primary(args_.path.source)
          .emit(ctrl.diagnostics());
        return {};
      }
      return make(timeout, fd_wrapper{uds.fd, true}, args_.follow.has_value());
    }
    // TODO: Switch to something else or make this more robust (for example,
    // check that we do not attempt to `::open` a directory).
    auto fd = ::open(args_.path.inner.c_str(), O_RDONLY);
    if (fd == -1) {
      diagnostic::error("could not open `{}`: {}", args_.path.inner,
                        detail::describe_errno())
        .primary(args_.path.source)
        .throw_();
    }
    return make(timeout, fd_wrapper{fd, true}, args_.follow.has_value());
  }

  auto to_string() const -> std::string override {
    auto result = std::string{"file"};
    result += escape_operator_arg(args_.path.inner);
    if (args_.follow) {
      result += " --follow";
    }
    if (args_.mmap) {
      result += " --mmap";
    }
    if (args_.timeout) {
      result += fmt::format(" --timeout {}", *args_.timeout);
    }
    return result;
  }

  auto name() const -> std::string override {
    return "file";
  }

  auto default_parser() const -> std::string override {
    const parser_parser_plugin* default_parser = nullptr;
    for (const auto* plugin : plugins::get<parser_parser_plugin>()) {
      if (plugin->accepts_file_path(args_.path.inner)) {
        if (default_parser) {
          diagnostic::error("could not determine default parser for file path "
                            "`{}`: parsers `{}` and `{}` both accept file path",
                            args_.path.inner, plugin->name(),
                            default_parser->name())
            .throw_();
        }
        auto test = plugin->name();
        default_parser = plugin;
      }
    }
    if (default_parser) {
      return default_parser->name();
    }
    for (const auto* plugin : plugins::get<parser_parser_plugin>()) {
      if (plugin->accepts_file_extension(args_.path.inner)) {
        if (default_parser) {
          diagnostic::error("could not determine default parser for file path "
                            "`{}`: parsers `{}` and `{}` both accept file "
                            "extension",
                            args_.path.inner, plugin->name(),
                            default_parser->name())
            .throw_();
        }
        auto test = plugin->name();
        default_parser = plugin;
      }
    }
    return default_parser ? default_parser->name() : "json";
  }

  friend auto inspect(auto& f, file_loader& x) -> bool {
    return f.apply(x.args_);
  }

private:
  loader_args args_;
};

class file_saver final : public plugin_saver {
public:
  file_saver() = default;

  explicit file_saver(saver_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "file";
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    // This should not be a `shared_ptr`, but we have to capture `stream` in
    // the returned `std::function`. Also, we capture it in a guard that calls
    // `.close()` to not silently discard errors that occur during destruction.
    auto stream = std::shared_ptr<writer>{};
    auto path = args_.path.inner;
    if (args_.uds) {
      auto uds = detail::unix_domain_socket::connect(path);
      if (!uds) {
        return caf::make_error(ec::filesystem_error,
                               fmt::format("unable to connect to UNIX "
                                           "domain socket at {}",
                                           path));
      }
      // TODO: This won't do any additional buffering. Is this what we want?
      stream = std::make_shared<fd_writer>(uds.fd, true);
    } else if (path == "-") {
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
      auto handle = std::fopen(path.c_str(), args_.appending ? "ab" : "wb");
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
    return [&ctrl, real_time = args_.real_time, stream = std::move(stream),
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

  auto is_joining() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, file_saver& x) -> bool {
    return f.apply(x.args_);
  }

private:
  saver_args args_;
};

class plugin : public virtual loader_plugin<file_loader>,
               public virtual saver_plugin<file_saver> {
public:
  auto name() const -> std::string override {
    return "file";
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

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto args = loader_args{};
    auto parser = argument_parser{"file", "https://vast.io/docs/next/"
                                          "connectors/file"};
    parser.add(args.path, "<path>");
    parser.add("-f,--follow", args.follow);
    parser.add("-m,--mmap", args.mmap);
    parser.add("-t,--timeout", args.timeout, "<duration>");
    parser.parse(p);
    args.path.inner = expand_path(args.path.inner);
    if (args.mmap) {
      if (args.follow) {
        diagnostic::error("cannot have both `--follow` and `--mmap`")
          .primary(*args.follow)
          .primary(*args.mmap)
          .throw_();
      }
      if (args.path.inner == "-") {
        diagnostic::error("cannot have `--mmap` with stdin")
          .primary(*args.mmap)
          .primary(args.path.source)
          .throw_();
      }
      if (args.timeout) {
        // TODO: Ideally, this diagnostic should point to `--timeout` instead of
        // the timeout value.
        diagnostic::error("cannot have both `--timeout` and `--mmap`")
          .primary(args.timeout->source)
          .primary(*args.mmap)
          .throw_();
      }
    }
    return std::make_unique<file_loader>(std::move(args));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto args = saver_args{};
    auto parser = argument_parser{"file", "https://vast.io/docs/next/"
                                          "connectors/file"};
    parser.add(args.path, "<path>");
    parser.add("-a,--appending", args.appending);
    parser.add("-r,--real-time", args.real_time);
    parser.add("--uds", args.uds);
    parser.parse(p);
    // TODO: Better argument validation
    if (args.path.inner == "-") {
      for (auto& other : {args.appending, args.real_time, args.uds}) {
        if (other) {
          diagnostic::error("flags are mutually exclusive")
            .primary(*other)
            .primary(args.path.source)
            .throw_();
        }
      }
    }
    args.path.inner = expand_path(args.path.inner);
    return std::make_unique<file_saver>(std::move(args));
  }

private:
  std::chrono::milliseconds read_timeout_{vast::defaults::import::read_timeout};
};

} // namespace
} // namespace vast::plugins::file

namespace vast::plugins::stdin_ {
namespace {

class plugin : public virtual loader_parser_plugin {
public:
  auto name() const -> std::string override {
    return "stdin";
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto args = file::loader_args{};
    args.path.inner = "-";
    auto parser = argument_parser{"stdin", "https://vast.io/docs/next/"
                                           "connectors/stdin"};
    parser.add("-t,--timeout", args.timeout, "<duration>");
    parser.parse(p);
    return std::make_unique<vast::plugins::file::file_loader>(std::move(args));
  }
};

} // namespace
} // namespace vast::plugins::stdin_

namespace vast::plugins::stdout_ {
namespace {

class plugin : public virtual saver_parser_plugin {
public:
  virtual auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto args = file::saver_args{};
    args.path.inner = "-";
    auto parser = argument_parser{name(), "https://vast.io/docs/next/"
                                          "connectors/stdout"};
    parser.parse(p);
    return std::make_unique<file::file_saver>(std::move(args));
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
