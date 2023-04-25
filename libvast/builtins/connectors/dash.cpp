//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/plugin.hpp>

namespace vast::plugins::dash {

class plugin final : public virtual loader_plugin, public virtual saver_plugin {
public:
  auto make_loader(std::span<std::string const> args,
                   operator_control_plane& ctrl) const
    -> caf::expected<generator<chunk_ptr>> override {
    return stdin_plugin_->make_loader(args, ctrl);
  }

  auto default_parser(std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return stdin_plugin_->default_parser(args);
  }

  auto make_saver(std::span<std::string const> args, type input_schema,
                  operator_control_plane& ctrl) const
    -> caf::expected<saver> override {
    return stdout_plugin_->make_saver(args, std::move(input_schema), ctrl);
  }

  auto default_printer(std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return stdout_plugin_->default_printer(args);
  }

  auto saver_does_joining() const -> bool override {
    return stdout_plugin_->saver_does_joining();
  }

  auto initialize(const record&, const record&) -> caf::error override {
    stdin_plugin_ = plugins::find<loader_plugin>("stdin");
    if (not stdin_plugin_) {
      return caf::make_error(ec::logic_error, "stdin plugin unavailable");
    }
    stdout_plugin_ = plugins::find<saver_plugin>("stdout");
    if (not stdout_plugin_) {
      return caf::make_error(ec::logic_error, "stdout plugin unavailable");
    }
    return caf::none;
  }

  auto name() const -> std::string override {
    return "-";
  }

private:
  const loader_plugin* stdin_plugin_ = nullptr;
  const saver_plugin* stdout_plugin_ = nullptr;
};

} // namespace vast::plugins::dash

VAST_REGISTER_PLUGIN(vast::plugins::dash::plugin)
