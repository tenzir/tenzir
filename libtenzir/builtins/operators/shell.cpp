//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/string/quoted_string.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>

#include <boost/asio.hpp>
// #include <boost/process.hpp>
#include <boost/process/v2.hpp>
#include <boost/process/v2/shell.hpp>
#include <caf/detail/scope_guard.hpp>

#include <mutex>
#include <queue>
#include <thread>

namespace bp = boost::process::v2;

namespace tenzir::plugins::shell {
namespace {

using namespace tenzir::binary_byte_literals;

/// The block size when reading from the child's stdin.
constexpr auto block_size = 16_KiB;

using child_process_actor = caf::typed_actor<
  auto(atom::read, uint64_t buffer_size)->caf::result<chunk_ptr>,
  auto(atom::write, chunk_ptr chunk)->caf::result<void>,
  auto(atom::status)->caf::result<void>>;

struct child_process_state {
  static constexpr auto name = "child-process";

  std::shared_ptr<boost::asio::io_context> io_ctx = {};

  std::shared_ptr<bp::process> child = {};

  std::optional<boost::asio::readable_pipe> read_pipe = {};
  std::optional<boost::asio::writable_pipe> write_pipe = {};

  caf::typed_response_promise<chunk_ptr> read_rp = {};
  caf::typed_response_promise<void> write_rp = {};

  std::vector<char> read_buffer = {};
};

auto make_child_process(
  child_process_actor::stateful_pointer<child_process_state> self,
  std::string command) -> child_process_actor::behavior_type {
  self->state.io_ctx = std::make_shared<boost::asio::io_context>();
  auto cmd = bp::shell(command);
  self->state.read_pipe.emplace(*self->state.io_ctx);
  self->state.write_pipe.emplace(*self->state.io_ctx);
  self->state.child = std::make_shared<bp::process>(
    *self->state.io_ctx, cmd.exe(), cmd.args(),
    bp::process_stdio{*self->state.write_pipe, *self->state.read_pipe, {}});
  auto worker = std::thread([io_ctx = self->state.io_ctx] {
    auto guard = boost::asio::make_work_guard(*io_ctx);
    io_ctx->run();
  });
  self->attach_functor([worker = std::move(worker), io_ctx = self->state.io_ctx,
                        child = self->state.child]() mutable {
    {
      bp::error_code ec{};
      child->terminate(ec);
    }
    io_ctx->stop();
    worker.join();
  });
  return {
    [self](atom::read, uint64_t buffer_size) -> caf::result<chunk_ptr> {
      TENZIR_ASSERT(buffer_size != 0);
      if (self->state.read_rp.pending()) {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} cannot read while a connect "
                                           "request is pending",
                                           *self));
      }
      self->state.read_buffer.resize(buffer_size);
      self->state.read_rp = self->make_response_promise<chunk_ptr>();
      auto on_read = [self, weak_hdl
                            = caf::actor_cast<caf::weak_actor_ptr>(self)](
                       boost::system::error_code ec, size_t length) {
        if (auto hdl = weak_hdl.lock()) {
          caf::anon_send(
            caf::actor_cast<caf::actor>(hdl),
            caf::make_action([self, ec, length] {
              if (ec) {
                return self->state.read_rp.deliver(caf::make_error(
                  ec::system_error,
                  fmt::format("failed to read from pipe: {}", ec.message())));
              }
              self->state.read_buffer.resize(length);
              self->state.read_buffer.shrink_to_fit();
              return self->state.read_rp.deliver(
                chunk::make(std::exchange(self->state.read_buffer, {})));
            }));
        }
      };
      auto asio_buffer
        = boost::asio::buffer(self->state.read_buffer, buffer_size);
      self->state.read_pipe->async_read_some(asio_buffer, on_read);
      return self->state.read_rp;
    },
    [self](atom::write, chunk_ptr chunk) -> caf::result<void> {
      TENZIR_ASSERT(chunk);
      if (self->state.write_rp.pending()) {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} cannot write while a write "
                                           "request is pending",
                                           *self));
      }
      self->state.write_rp = self->make_response_promise<void>();
      auto on_write
        = [self, chunk, weak_hdl = caf::actor_cast<caf::weak_actor_ptr>(self)](
            boost::system::error_code ec, size_t length) {
            if (auto hdl = weak_hdl.lock()) {
              caf::anon_send(caf::actor_cast<caf::actor>(hdl),
                             caf::make_action([self, chunk, ec, length] {
                               if (ec) {
                                 self->state.write_rp.deliver(caf::make_error(
                                   ec::system_error,
                                   fmt::format("failed to write to pipe: {}",
                                               ec.message())));
                                 return;
                               }
                               if (length < chunk->size()) {
                                 auto remainder = chunk->slice(length);
                                 self->state.write_rp.delegate(
                                   static_cast<child_process_actor>(self),
                                   atom::write_v, std::move(remainder));
                                 return;
                               }
                               TENZIR_ASSERT(length == chunk->size());
                               self->state.write_rp.deliver();
                             }));
            }
          };
      auto asio_buffer = boost::asio::buffer(chunk->data(), chunk->size());
      self->state.write_pipe->async_write_some(asio_buffer, on_write);
      return self->state.write_rp;
    },
    [self](atom::status) -> caf::result<void> {
      if (self->state.child->running()) {
        return {};
      }
      return caf::make_error(ec::silent, "");
    }};
}

class shell_operator final : public crtp_operator<shell_operator> {
public:
  shell_operator() = default;

  explicit shell_operator(std::string command) : command_{std::move(command)} {
  }

  auto operator()(generator<chunk_ptr> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto child = ctrl.self().spawn(make_child_process, command_);
    co_yield {};
    auto output = chunk_ptr{};
    bool running = true;
    auto input_it = input.begin();
    while (running) {
      {
        ctrl.self()
          .request(child, caf::infinite, atom::status_v)
          .await([&]() {},
                 [&](const caf::error&) {
                   running = false;
                 });
        co_yield {};
      }
      if (input_it != input.end()) {
        auto&& next = *input_it;
        if (next) {
          ctrl.self()
            .request(child, caf::infinite, atom::write_v, std::move(next))
            .await(
              [&]() {
                ++input_it;
              },
              [](const caf::error& err) {
                TENZIR_ERROR("write_v err: {}", err);
              });
        }
        co_yield {};
      }
      {
        constexpr auto buffer_size = uint64_t{65'536};
        ctrl.self()
          .request(child, caf::infinite, atom::read_v, buffer_size)
          .await(
            [&](chunk_ptr& chunk) {
              output = std::move(chunk);
            },
            [&](const caf::error& err) {
              TENZIR_ERROR("read_v err: {}", err);
              running = false;
            });
        co_yield {};
        co_yield std::exchange(output, {});
      }
    }
  }

#if 0
  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    boost::asio::io_context ctx{};
    auto worker = std::thread([&ctx]() {
      ctx.run();
    });
    auto cmd = bp::shell(command_);
    boost::asio::readable_pipe read_pipe(ctx);
    bp::process child(ctx, cmd.exe(), cmd.args(),
                      bp::process_stdio{{}, read_pipe, {}});
    co_yield {};
    std::mutex mtx;
    std::condition_variable cv;
    TENZIR_ASSERT(read_pipe.is_open());
    while (true) {
      std::vector<char> read_buffer(block_size);
      auto buffer = boost::asio::buffer(read_buffer);
      read_pipe.async_read_some(buffer, [&](boost::system::error_code ec,
                                            size_t len) {
        TENZIR_INFO("callback: {}", std::this_thread::get_id());
#  if 0
                                  std::unique_lock lk(mtx);
                                  read_buffer.resize(len);
                                  lk.unlock();
                                  cv.notify_one();
#  endif
      });
      TENZIR_INFO("main loop: {}", std::this_thread::get_id());
      std::this_thread::sleep_for(std::chrono::milliseconds{500});
#  if 0
      std::unique_lock lk(mtx);
      cv.wait(lk);
      co_yield chunk::make(std::move(read_buffer));
#  endif
    }
  }
#endif

  auto location() const -> operator_location override {
    // The user expectation is that shell executes relative to the
    // currently executing process.
    return operator_location::local;
  }

  auto detached() const -> bool override {
    // We may execute blocking syscalls.
    return true;
  }

  auto input_independent() const -> bool override {
    // We may produce results without receiving any further input.
    return true;
  }

  auto name() const -> std::string override {
    return "shell";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, shell_operator& x) -> bool {
    return f.apply(x.command_);
  }

private:
  std::string command_;
};

class plugin final : public virtual operator_plugin<shell_operator> {
public:
  auto signature() const -> operator_signature override {
    return {
      .source = true,
      .transformation = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto command = std::string{};
    auto parser = argument_parser{"shell", "https://docs.tenzir.com/next/"
                                           "operators/transformations/shell"};
    parser.add(command, "<command>");
    parser.parse(p);
    return std::make_unique<shell_operator>(std::move(command));
  }
};

} // namespace
} // namespace tenzir::plugins::shell

TENZIR_REGISTER_PLUGIN(tenzir::plugins::shell::plugin)

#if 0
namespace tenzir::plugins::shell {
namespace {

using namespace tenzir::binary_byte_literals;

/// The block size when reading from the child's stdin.
constexpr auto block_size = 16_KiB;

enum class stdin_mode { none, inherit, pipe };

/// Wraps the logic for interacting with a child's stdin and stdout.
class child {
public:
  static auto make(std::string command, stdin_mode mode)
    -> caf::expected<child> {
    auto result = child{std::move(command)};
    // We use `/bin/sh -c "${command}"` to interpret the command.
    auto shell = "/bin/sh";
    try {
      auto exit_handler = [](int exit, std::error_code ec) {
        TENZIR_DEBUG("child exited with code {}: {}", exit, ec.message());
      };
      switch (mode) {
        case stdin_mode::none:
          result.child_ = bp::child{
            shell,
            "-c",
            result.command_,
            bp::std_out > result.stdout_,
            bp::std_in < bp::close,
            bp::on_exit(exit_handler),
          };
          break;
        case stdin_mode::inherit:
          result.child_ = bp::child{
            shell,
            "-c",
            result.command_,
            bp::std_out > result.stdout_,
            bp::on_exit(exit_handler),
          };
          break;
        case stdin_mode::pipe:
          result.child_ = bp::child{
            shell,
            "-c",
            result.command_,
            bp::std_out > result.stdout_,
            bp::std_in < result.stdin_,
            bp::on_exit(exit_handler),
          };
          break;
      }
    } catch (const bp::process_error& e) {
      return caf::make_error(ec::filesystem_error, e.what());
    }
    return result;
  }

  auto read(std::span<std::byte> buffer) -> caf::expected<size_t> {
    TENZIR_ASSERT(!buffer.empty());
    TENZIR_TRACE("trying to read {} bytes", buffer.size());
    auto* data = reinterpret_cast<char*>(buffer.data());
    auto size = detail::narrow<int>(buffer.size());
    auto bytes_read = stdout_.read(data, size);
    TENZIR_TRACE("read {} bytes", bytes_read);
    return detail::narrow<size_t>(bytes_read);
  }

  auto write(std::span<const std::byte> buffer) -> caf::error {
    TENZIR_ASSERT(!buffer.empty());
    TENZIR_TRACE("writing {} bytes to child's stdin", buffer.size());
    const auto* data = reinterpret_cast<const char*>(buffer.data());
    auto size = detail::narrow_cast<std::streamsize>(buffer.size());
    if (not stdin_.write(data, size))
      return caf::make_error(ec::unspecified,
                             "failed to write into child's stdin");
    return caf::none;
  }

  void close_stdin() {
    TENZIR_DEBUG("sending EOF to child's stdin");
    stdin_.close();
  }

  auto wait() -> caf::error {
    auto ec = std::error_code{};
    child_.wait(ec);
    if (ec) {
      return caf::make_error(ec::unspecified,
                             fmt::format("waiting for child process failed: {}",
                                         ec));
    }
    auto code = child_.exit_code();
    if (code != 0) {
      return caf::make_error(
        ec::unspecified,
        fmt::format("child process exited with exit-code {}", code));
    }
    return {};
  }

  void terminate() {
    auto ec = std::error_code{};
    child_.terminate(ec);
    if (ec) {
      TENZIR_WARN("failed to terminate child process: {}", ec);
    }
  }

private:
  explicit child(std::string command) : command_{std::move(command)} {
    TENZIR_ASSERT(!command_.empty());
  }

  std::string command_;
  bp::child child_;
  bp::pipe stdout_;
  bp::pipe stdin_;
};

class shell_operator final : public crtp_operator<shell_operator> {
public:
  shell_operator() = default;

  explicit shell_operator(std::string command) : command_{std::move(command)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto mode = ctrl.has_terminal() ? stdin_mode::inherit : stdin_mode::none;
    auto child = child::make(command_, mode);
    if (!child) {
      diagnostic::error(
        add_context(child.error(), "failed to spawn child process"))
        .emit(ctrl.diagnostics());
      co_return;
    }
    // We yield once because reading below is blocking, but we want to
    // directly signal that our initialization is complete.
    co_yield {};
    auto buffer = std::vector<char>(block_size);
    while (true) {
      auto bytes_read = child->read(as_writeable_bytes(buffer));
      if (not bytes_read) {
        diagnostic::error(
          add_context(bytes_read.error(), "failed to read from child process"))
          .emit(ctrl.diagnostics());
        co_return;
      }
      if (*bytes_read == 0) {
        // Reading 0 bytes indicates EOF.
        break;
      }
      auto chk = chunk::copy(std::span{buffer.data(), *bytes_read});
      TENZIR_TRACE("yielding chunk with {} bytes", chk->size());
      co_yield chk;
    }
    if (auto error = child->wait()) {
      diagnostic::error(add_context(error, "child process execution failed"))
        .emit(ctrl.diagnostics());
      co_return;
    }
  }

  auto operator()(generator<chunk_ptr> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    // TODO: Handle exceptions from `boost::process`.
    auto child = child::make(command_, stdin_mode::pipe);
    if (!child) {
      diagnostic::error(
        add_context(child.error(), "failed to spawn child process"))
        .emit(ctrl.diagnostics());
      co_return;
    }
    // Read from child in separate thread because coroutine-based async
    // I/O is not (yet) feasible. The thread writes the chunks into a
    // queue such that to this coroutine can yield them.
    auto chunks = std::queue<chunk_ptr>{};
    auto chunks_mutex = std::mutex{};
    auto thread = std::thread([&child, &chunks, &chunks_mutex,
                               diagnostics = ctrl.shared_diagnostics()]() {
      auto buffer = std::vector<char>(block_size);
      while (true) {
        auto bytes_read = child->read(as_writeable_bytes(buffer));
        if (not bytes_read) {
          diagnostic::error(add_context(bytes_read.error(),
                                        "failed to read from child process"))
            .emit(diagnostics);
          return;
        }
        if (*bytes_read == 0) {
          // Reading 0 bytes indicates EOF.
          break;
        }
        auto chk = chunk::copy(std::span{buffer.data(), *bytes_read});
        auto lock = std::lock_guard{chunks_mutex};
        chunks.push(std::move(chk));
      }
    });
    {
      // Coroutines require RAII-style exit handling.
      auto unplanned_exit = caf::detail::make_scope_guard([&] {
        child->terminate();
        TENZIR_DEBUG("joining thread");
        thread.join();
      });
      // Loop over input chunks.
      for (auto&& chunk : input) {
        auto stalled = not chunk or chunk->size() == 0;
        if (not stalled) {
          // Pass operator input to the child's stdin.
          // TODO: If the reading end of the pipe to the child's stdin is
          // already closed, this will generate a SIGPIPE.
          if (auto err = child->write(as_bytes(*chunk))) {
            diagnostic::error(
              add_context(err, "failed to write to child process"))
              .emit(ctrl.diagnostics());
            co_return;
          }
        }
        // Try yielding so far accumulated child output.
        auto lock = std::unique_lock{chunks_mutex, std::try_to_lock};
        if (lock.owns_lock()) {
          auto i = size_t{0};
          auto total = chunks.size();
          while (not chunks.empty()) {
            auto chk = chunks.front();
            TENZIR_DEBUG("yielding chunk {}/{} with {} bytes", ++i, total,
                         chk->size());
            co_yield std::move(chk);
            chunks.pop();
          }
          if (stalled) {
            co_yield {};
          }
        } else {
          co_yield {};
        }
      }
      unplanned_exit.disable();
      child->close_stdin();
      thread.join();
      if (auto error = child->wait()) {
        diagnostic::error(add_context(error, "child process execution failed"))
          .emit(ctrl.diagnostics());
        co_return;
      }
    }
    // Yield all accumulated child output.
    auto lock = std::lock_guard{chunks_mutex};
    auto i = size_t{0};
    auto total = chunks.size();
    while (not chunks.empty()) {
      auto& chk = chunks.front();
      TENZIR_DEBUG("yielding chunk {}/{} with {} bytes", ++i, total,
                   chk->size());
      co_yield std::move(chk);
      chunks.pop();
    }
  }

  auto location() const -> operator_location override {
    // The user expectation is that shell executes relative to the
    // currently executing process.
    return operator_location::local;
  }

  auto detached() const -> bool override {
    // We may execute blocking syscalls.
    return true;
  }

  auto input_independent() const -> bool override {
    // We may produce results without receiving any further input.
    return true;
  }

  auto name() const -> std::string override {
    return "shell";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, shell_operator& x) -> bool {
    return f.apply(x.command_);
  }

private:
  std::string command_;
};

class plugin final : public virtual operator_plugin<shell_operator> {
public:
  auto signature() const -> operator_signature override {
    return {
      .source = true,
      .transformation = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto command = std::string{};
    auto parser = argument_parser{"shell", "https://docs.tenzir.com/next/"
                                           "operators/transformations/shell"};
    parser.add(command, "<command>");
    parser.parse(p);
    return std::make_unique<shell_operator>(std::move(command));
  }
};

} // namespace
} // namespace tenzir::plugins::shell

TENZIR_REGISTER_PLUGIN(tenzir::plugins::shell::plugin)
#endif
