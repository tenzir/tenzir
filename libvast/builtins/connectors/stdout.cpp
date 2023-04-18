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

namespace vast::plugins::stdout_ {

class plugin : public virtual saver_plugin {
public:
  auto make_saver(std::span<std::string const> args, type,
                  operator_control_plane&) const
    -> caf::expected<saver> override {
    auto outbuf = detail::fdoutbuf(STDOUT_FILENO);
    return [outbuf](chunk_ptr chunk) mutable {
      if (chunk)
        outbuf.sputn(reinterpret_cast<const char*>(chunk->data()),
                     chunk->size());
    };
  }

  auto default_printer(std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"json", {}};
  }

  auto initialize(const record&, const record&) -> caf::error override {
    return caf::none;
  }

  auto name() const -> std::string override {
    return "stdout";
  }

  auto saver_requires_joining() const -> bool override {
    return true;
  }
};

} // namespace vast::plugins::stdout_

VAST_REGISTER_PLUGIN(vast::plugins::stdout_::plugin)
