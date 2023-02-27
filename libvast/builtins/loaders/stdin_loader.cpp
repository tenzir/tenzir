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
#include <vast/detail/string.hpp>
#include <vast/plugin.hpp>

#include <unistd.h>

namespace vast::plugins::stdin_loader {

// -- loader plugin -----------------------------------------------------
class plugin : public virtual loader_plugin {
public:
  static constexpr inline auto max_chunk_size = size_t{16384};

  [[nodiscard]] auto make_loader(const record&, operator_control_plane&) const
    -> caf::expected<loader> override {
    return [this]() -> generator<chunk_ptr> {
      auto in_buf = detail::fdinbuf(STDIN_FILENO, max_chunk_size);
      in_buf.read_timeout() = read_timeout;
      auto current_data = std::vector<std::byte>{};
      current_data.reserve(max_chunk_size);
      auto eof_reached = false;
      while (not eof_reached) {
        auto current_char = in_buf.sbumpc();
        if (current_char != detail::fdinbuf::traits_type::eof()) {
          current_data.emplace_back(static_cast<std::byte>(current_char));
        } else {
          eof_reached = (not in_buf.timed_out());
          if (current_data.empty()) {
            if (not eof_reached) {
              co_yield chunk::make_empty();
              continue;
            } else {
              break;
            }
          }
        }
        if (eof_reached || current_data.size() == max_chunk_size) {
          auto chunk = chunk::make(std::exchange(current_data, {}));
          co_yield std::move(chunk);
          if (not eof_reached) {
            current_data.reserve(max_chunk_size);
          }
        }
      }
      co_return;
    };
  }

  [[nodiscard]] auto
  make_default_parser(const record&, operator_control_plane&) const
    -> caf::expected<parser> override {
    return caf::make_error(ec::unimplemented,
                           "parser currently not implemented");
  }

  [[nodiscard]] caf::error
  initialize([[maybe_unused]] const record& plugin_config,
             const record& global_config) override {
    if (auto vast_settings = caf::get_if<record>(&global_config.at("vast"))) {
      if (auto import_settings
          = caf::get_if<record>(&vast_settings->at("import"))) {
        if (auto read_timeout_entry
            = caf::get_if<std::string>(&import_settings->at("read-timeout"))) {
          if (auto timeout_duration = to<vast::duration>(*read_timeout_entry)) {
            read_timeout
              = std::chrono::duration_cast<std::chrono::milliseconds>(
                *timeout_duration);
            return caf::none;
          }
        }
      }
    }
    VAST_DEBUG("unable to read vast.import.read-timeout, resorting to "
               "default value");
    return caf::none;
  }

  [[nodiscard]] std::string name() const override {
    return "stdin";
  }

private:
  std::chrono::milliseconds read_timeout{vast::defaults::import::read_timeout};
};

} // namespace vast::plugins::stdin_loader

VAST_REGISTER_PLUGIN(vast::plugins::stdin_loader::plugin)
