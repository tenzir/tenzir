//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/detail/fdinbuf.hpp>
#include <vast/detail/file_path_to_parser.hpp>
#include <vast/detail/posix.hpp>
#include <vast/detail/string.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>

#include <caf/error.hpp>

#include <fcntl.h>
#include <filesystem>
#include <memory>
#include <string_view>
#include <unistd.h>

namespace {
const auto stdin_path = std::string{"-"};
} // namespace

namespace vast::plugins::file {

using file_description_deleter = std::function<void(int*)>;
using file_description_wrapper = std::unique_ptr<int, file_description_deleter>;

class plugin : public virtual loader_plugin, public virtual saver_plugin {
public:
  static constexpr auto max_chunk_size = size_t{16384};

  auto
  make_loader(std::span<std::string const> args, operator_control_plane&) const
    -> caf::expected<generator<chunk_ptr>> override {
    auto read_timeout = read_timeout_;
    auto path = std::string{};
    auto following = false;
    auto is_socket = false;
    for (auto i = size_t{0}; i < args.size(); ++i) {
      const auto& arg = args[i];
      VAST_TRACE("processing loader argument {}", arg);
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
        path = ::stdin_path;
      } else if (arg == "-f" || arg == "--follow") {
        following = true;
      } else if (not arg.starts_with("-")) {
        std::error_code err{};
        auto status = std::filesystem::status(arg, err);
        if (err) {
          return caf::make_error(ec::parse_error,
                                 fmt::format("could not access file {}: {}",
                                             arg, err));
        }
        is_socket = (status.type() == std::filesystem::file_type::socket);
        if (path == "-") {
          return caf::make_error(ec::parse_error,
                                 fmt::format("file argument {} can not be "
                                             "combined with "
                                             "stdin file argument",
                                             arg));
        }
        path = arg;
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
    auto fd = file_description_wrapper(new int(STDIN_FILENO), [](auto* fd) {
      std::default_delete<int>()(fd);
    });
    if (is_socket) {
      if (path == ::stdin_path) {
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
        if (*fd == -1) {
          ::close(*fd);
        }
        std::default_delete<int>()(fd);
      });
    } else {
      if (path != ::stdin_path) {
        fd = file_description_wrapper(new int(::open(path.c_str(), O_RDONLY)),
                                      [](auto fd) {
                                        if (*fd == -1) {
                                          ::close(*fd);
                                        }
                                        std::default_delete<int>()(fd);
                                      });
        if (*fd == -1) {
          return caf::make_error(ec::filesystem_error,
                                 fmt::format("open(2) for file {} failed {}:",
                                             path, std::strerror(errno)));
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
      VAST_TRACE("processing loader argument {}", arg);
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
    const auto* read_timeout_entry
      = get_if<std::string>(&global_config, "vast.import.read-timeout");
    if (!read_timeout_entry) {
      return caf::none;
    }
    if (auto timeout_duration = to<vast::duration>(*read_timeout_entry)) {
      read_timeout_ = std::chrono::duration_cast<std::chrono::milliseconds>(
        *timeout_duration);
    }
    return caf::none;
  }

  auto make_saver(std::span<std::string const> args, type input_schema,
                  operator_control_plane& ctrl) const
    -> caf::expected<saver> override {
    die();
  }

  auto default_printer(std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    /*for (auto i = size_t{0}; i < args.size(); ++i) {
      const auto& arg = args[i];
      VAST_TRACE("processing loader argument {}", arg);
      if (arg == "-") {
        break;
      }
      if (arg == "--timeout") {
        ++i;
      } else if (!arg.starts_with("-")) {
        return {detail::file_path_to_parser(arg), {}};
      }
    }*/
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

} // namespace vast::plugins::file

namespace vast::plugins::stdin_ {
class plugin : public virtual vast::plugins::file::plugin {
public:
  auto make_loader([[maybe_unused]] std::span<std::string const> args,
                   operator_control_plane& ctrl) const
    -> caf::expected<generator<chunk_ptr>> override {
    std::vector<std::string> new_args = {"-"};
    new_args.insert(new_args.end(), args.begin(), args.end());
    return vast::plugins::file::plugin::make_loader(new_args, ctrl);
  }

  auto default_parser([[maybe_unused]] std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"json", {}};
  }

  auto name() const -> std::string override {
    return "stdin";
  }
};

} // namespace vast::plugins::stdin_

VAST_REGISTER_PLUGIN(vast::plugins::file::plugin)
VAST_REGISTER_PLUGIN(vast::plugins::stdin_::plugin)