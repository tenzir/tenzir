//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/detail/fdinbuf.hpp>
#include <tenzir/detail/fdoutbuf.hpp>
#include <tenzir/detail/file_path_to_plugin_name.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

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

namespace tenzir::plugins::file {
namespace {

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
      TENZIR_WARN("closing failed in destructor: {}", error);
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
      TENZIR_ASSERT(written > 0);
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
      TENZIR_WARN("closing failed in destructor: {}", error);
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
  std::optional<location> append;
  std::optional<location> real_time;
  std::optional<location> uds;

  template <class Inspector>
  friend auto inspect(Inspector& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("saver_args")
      .fields(f.field("path", x.path), f.field("append", x.append),
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
        TENZIR_WARN("failed to close file in destructor: {}",
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
  // We use 2^20 for the upper bound of a chunk size, which exactly matches the
  // upper limit defined by execution nodes for transporting events.
  // TODO: Get the backpressure-adjusted value at runtime from the execution node.
  static constexpr size_t max_chunk_size = 1 << 20;

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
          // Note that we copy and manually clear here rather than moving the
          // buffer into the chunk and reserving again to avoid excess memory
          // usage from unused capacity.
          co_yield chunk::copy(current_data);
          if (eof_reached and not following) {
            break;
          }
          current_data.clear();
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

  auto name() const -> std::string override {
    return "file";
  }

  auto default_parser() const -> std::string override {
    auto name
      = detail::file_path_to_plugin_name(args_.path.inner).value_or("json");
    if (!plugins::find<parser_parser_plugin>(name))
      return "json";
    return name;
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
      auto handle = std::fopen(path.c_str(), args_.append ? "ab" : "wb");
      if (handle == nullptr) {
        return caf::make_error(ec::filesystem_error,
                               fmt::format("failed to open {}: {}", path,
                                           detail::describe_errno()));
      }
      stream = std::make_shared<file_writer>(handle);
    }
    TENZIR_ASSERT(stream);
    auto guard = caf::detail::make_scope_guard([&ctrl, stream] {
      if (auto error = stream->close()) {
        diagnostic::error(error)
          .note("failed to close stream")
          .emit(ctrl.diagnostics());
      }
    });
    return [&ctrl, real_time = args_.real_time, stream = std::move(stream),
            guard = std::make_shared<decltype(guard)>(std::move(guard))](
             chunk_ptr chunk) {
      if (!chunk || chunk->size() == 0) {
        return;
      }
      if (auto error = stream->write(std::span{chunk->data(), chunk->size()})) {
        diagnostic::error(error)
          .note("failed to write to stream")
          .emit(ctrl.diagnostics());
        return;
      }
      if (real_time) {
        if (auto error = stream->flush()) {
          diagnostic::error(error)
            .note("failed to flush stream")
            .emit(ctrl.diagnostics());
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
      = try_get<tenzir::duration>(global_config, "tenzir.import.read-timeout");
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
    auto parser = argument_parser{"file", "https://docs.tenzir.com/"
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
    auto parser = argument_parser{"file", "https://docs.tenzir.com/"
                                          "connectors/file"};
    parser.add(args.path, "<path>");
    parser.add("-a,--append", args.append);
    parser.add("-r,--real-time", args.real_time);
    parser.add("--uds", args.uds);
    parser.parse(p);
    // TODO: Better argument validation
    if (args.path.inner == "-") {
      for (auto& other : {args.append, args.real_time, args.uds}) {
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
  std::chrono::milliseconds read_timeout_{
    tenzir::defaults::import::read_timeout};
};

class load_file_operator final : public crtp_operator<load_file_operator> {
public:
  load_file_operator() = default;

  explicit load_file_operator(loader_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto loader = file_loader{args_};
    auto instance = loader.instantiate(ctrl);
    if (not instance) {
      co_return;
    }
    for (auto&& chunk : *instance) {
      co_yield std::move(chunk);
    }
  }

  auto name() const -> std::string override {
    return "tql2.load_file";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, load_file_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  loader_args args_;
};

class load_file_plugin final : public operator_plugin2<load_file_operator> {
public:
  auto make(invocation inv, session ctx) const -> operator_ptr override {
    auto args = loader_args{};
    auto timeout = std::optional<located<duration>>{};
    argument_parser2::operator_("load_file")
      .add(args.path, "<path>")
      .add("follow", args.follow)
      .add("mmap", args.mmap)
      .add("timeout", timeout)
      .parse(inv, ctx);
    if (timeout) {
      args.timeout = located{
        std::chrono::duration_cast<std::chrono::milliseconds>(timeout->inner),
        timeout->source};
    }
    return std::make_unique<load_file_operator>(std::move(args));
  }
};

class save_file_operator final : public crtp_operator<save_file_operator> {
public:
  save_file_operator() = default;

  explicit save_file_operator(saver_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    auto loader = file_saver{args_};
    auto instance = loader.instantiate(ctrl, {});
    if (not instance) {
      co_return;
    }
    for (auto&& chunk : input) {
      (*instance)(std::move(chunk));
      co_yield {};
    }
  }

  auto name() const -> std::string override {
    return "tql2.save_file";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, save_file_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  saver_args args_;
};

class save_file_plugin final : public operator_plugin2<save_file_operator> {
public:
  auto make(invocation inv, session ctx) const -> operator_ptr override {
    auto args = saver_args{};
    argument_parser2::operator_("save_file")
      .add(args.path, "<path>")
      .add("append", args.append)
      .add("real_time", args.real_time)
      .add("uds", args.uds)
      .parse(inv, ctx);
    return std::make_unique<save_file_operator>(std::move(args));
  }
};

} // namespace
} // namespace tenzir::plugins::file

namespace tenzir::plugins::stdin_ {
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
    auto parser = argument_parser{"stdin", "https://docs.tenzir.com/"
                                           "connectors/stdin"};
    parser.add("-t,--timeout", args.timeout, "<duration>");
    parser.parse(p);
    return std::make_unique<tenzir::plugins::file::file_loader>(
      std::move(args));
  }
};

} // namespace
} // namespace tenzir::plugins::stdin_

namespace tenzir::plugins::stdout_ {
namespace {

class plugin : public virtual saver_parser_plugin {
public:
  virtual auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto args = file::saver_args{};
    args.path.inner = "-";
    auto parser = argument_parser{name(), "https://docs.tenzir.com/"
                                          "connectors/stdout"};
    parser.parse(p);
    return std::make_unique<file::file_saver>(std::move(args));
  }

  auto name() const -> std::string override {
    return "stdout";
  }
};

} // namespace
} // namespace tenzir::plugins::stdout_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::file::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::file::load_file_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::file::save_file_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::stdin_::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::stdout_::plugin)
