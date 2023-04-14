//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/plugin.hpp>

namespace vast::plugins::dash {

class plugin final : public virtual loader_plugin, dumper_plugin {
public:
  auto make_loader(const record& options, operator_control_plane& ctrl) const
    -> caf::expected<generator<chunk_ptr>> override {
    return stdin_plugin_->make_loader(options, ctrl);
  }

  auto get_default_parser(const record& options) const
    -> std::optional<std::pair<std::string, record>> override {
    return stdin_plugin_->get_default_parser(options);
  }

  auto make_dumper(const record& options, type input_schema,
                   operator_control_plane& ctrl) const
    -> caf::expected<dumper> override {
    return stdout_plugin_->make_dumper(options, std::move(input_schema), ctrl);
  }

  auto make_default_printer() const
    -> std::optional<std::pair<std::string, record>> override {
    return stdout_plugin_->make_default_printer();
  }

  auto dumper_requires_joining() const -> bool override {
    return stdout_plugin_->dumper_requires_joining();
  }

  auto initialize(const record&, const record&) -> caf::error override {
    stdin_plugin_ = plugins::find<loader_plugin>("stdin");
    if (not stdin_plugin_) {
      return caf::make_error(ec::logic_error, "stdin plugin unavailable");
    }
    stdout_plugin_ = plugins::find<dumper_plugin>("stdout");
    if (not stdout_plugin_) {
      return caf::make_error(ec::logic_error, "stdin plugin unavailable");
    }
    return caf::none;
  }

  auto name() const -> std::string override {
    return "-";
  }

private:
  const loader_plugin* stdin_plugin_ = nullptr;
  const dumper_plugin* stdout_plugin_ = nullptr;
};

} // namespace vast::plugins::dash

VAST_REGISTER_PLUGIN(vast::plugins::dash::plugin)
