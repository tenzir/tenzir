//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/chunk.hpp>
#include <vast/concept/parseable/string/quoted_string.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/si_literals.hpp>

#include <boost/asio.hpp>
#include <boost/process.hpp>

namespace vast::plugins::shell {

namespace {

class shell_operator final : public crtp_operator<shell_operator> {
public:
  explicit shell_operator(std::string command) : command_{std::move(command)} {
  }

  auto operator()(generator<chunk_ptr> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    using namespace binary_byte_literals;
    namespace bp = boost::process;
    // Spawn child process and connect stdin and stdout.
    bp::ipstream child_stdout;
    bp::opstream child_stdin;
    std::error_code ec;
    bp::child child;
    try {
      auto exit_handler = [this](int exit, std::error_code ec) {
        VAST_DEBUG("child \"{}\" exited with code {}: {}", command_, exit,
                   ec.message());
      };
      child = bp::child{command_, bp::std_out > child_stdout,
                        bp::std_in < child_stdin, bp::on_exit(exit_handler)};
    } catch (const bp::process_error& e) {
      ctrl.abort(caf::make_error(ec::filesystem_error, e.what()));
      co_return;
    }
    for (auto&& chunk : input) {
      if (!child.running(ec)) {
        if (ec)
          VAST_DEBUG(ec);
        else
          VAST_ERROR(ec);
        co_yield {};
        break;
      }
      // Shove operator input into the child's stdin.
      auto chunk_data = reinterpret_cast<const char*>(chunk->data());
      VAST_DEBUG("writing {} bytes to child's stdin", chunk->size());
      if (!child_stdin.write(chunk_data, chunk->size())) {
        ctrl.abort(caf::make_error(
          ec::unspecified, fmt::format("failed to write into child's stdin")));
        break;
      }
      // Read child's stdout in blocks and relay them downstream.
      constexpr auto block_size = 16_KiB;
      std::vector<char> buffer(block_size);
      while (true) {
        child_stdout.read(buffer.data(), block_size);
        auto bytes_read = detail::narrow_cast<size_t>(child_stdout.gcount());
        VAST_DEBUG("read {} bytes", bytes_read);
        if (bytes_read == 0) {
          // No output from child, come back next time.
          co_yield {};
          break;
        }
        buffer.resize(bytes_read);
        co_yield chunk::make(std::vector<char>{buffer});
      }
    }
    // FIXME: do this RAII-style.
    VAST_DEBUG("awaiting child");
    child.wait(ec);
    if (ec)
      VAST_DEBUG(ec);
    else
      VAST_ERROR(ec);
  }

  auto to_string() const -> std::string override {
    return fmt::format("shell \"{}\"", command_);
  }

private:
  std::string command_;
};

class plugin final : public virtual operator_plugin {
public:
  // plugin API
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] auto name() const -> std::string override {
    return "shell";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::required_ws_or_comment,
      parsers::end_of_pipeline_operator, parsers::qqstr;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = -(required_ws_or_comment >> qqstr) >> optional_ws_or_comment
                   >> end_of_pipeline_operator;
    std::string command;
    if (!p(f, l, command)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse {} operator: '{}'", name(),
                                    pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<shell_operator>(std::move(command)),
    };
  }
};

} // namespace

} // namespace vast::plugins::shell

VAST_REGISTER_PLUGIN(vast::plugins::shell::plugin)
