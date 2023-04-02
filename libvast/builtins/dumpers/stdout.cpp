//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/detail/fdoutbuf.hpp>
#include <vast/plugin.hpp>

#include <unistd.h>

namespace vast::plugins::stdout_dumper {

class plugin : public virtual dumper_plugin {
public:
  [[nodiscard]] auto
  make_dumper(const record&, [[maybe_unused]] type input_schema,
              operator_control_plane&) const -> caf::expected<dumper> override {
    auto outbuf = detail::fdoutbuf(STDOUT_FILENO);
    return [outbuf](chunk_ptr chunk) mutable {
      outbuf.sputn(reinterpret_cast<const char*>(chunk->data()), chunk->size());
    };
  }

  [[nodiscard]] auto make_default_printer() const
    -> std::optional<std::pair<std::string, record>> override {
    return std::pair{"json", record{}};
  }

  [[nodiscard]] auto initialize([[maybe_unused]] const record& plugin_config,
                                [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return caf::none;
  }

  [[nodiscard]] auto name() const -> std::string override {
    return "stdout";
  }

  [[nodiscard]] auto dumper_requires_joining() const -> bool override {
    return true;
  }
};

} // namespace vast::plugins::stdout_dumper

VAST_REGISTER_PLUGIN(vast::plugins::stdout_dumper::plugin)
