//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/plugin.hpp>

namespace vast::plugins::dash {

class plugin final : public virtual loader_parser_plugin,
                     public virtual saver_parser_plugin {
public:
  auto parse_loader(tql::parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    return stdin_plugin_->parse_loader(p);
  }

  auto parse_saver(tql::parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    return stdout_plugin_->parse_saver(p);
  }

  auto initialize(const record&, const record&) -> caf::error override {
    stdin_plugin_ = plugins::find<loader_parser_plugin>("stdin");
    if (not stdin_plugin_) {
      return caf::make_error(ec::logic_error, "stdin plugin unavailable");
    }
    stdout_plugin_ = plugins::find<saver_parser_plugin>("stdout");
    if (not stdout_plugin_) {
      return caf::make_error(ec::logic_error, "stdout plugin unavailable");
    }
    return caf::none;
  }

  auto name() const -> std::string override {
    return "-";
  }

private:
  const loader_parser_plugin* stdin_plugin_ = nullptr;
  const saver_parser_plugin* stdout_plugin_ = nullptr;
};

} // namespace vast::plugins::dash

VAST_REGISTER_PLUGIN(vast::plugins::dash::plugin)
