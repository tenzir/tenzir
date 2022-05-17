//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "broker/reader.hpp"
#include "broker/writer.hpp"

#include <vast/data.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>

#include <caf/error.hpp>
#include <caf/settings.hpp>

namespace vast::plugins::broker {

/// The Broker reader plugin.
class plugin final : public virtual reader_plugin,
                     public virtual writer_plugin {
public:
  /// Initializes a plugin with its respective entries from the YAML config
  /// file, i.e., `plugin.<NAME>`.
  /// @param config The relevant subsection of the configuration.
  caf::error initialize(data config) override {
    if (auto* r = caf::get_if<record>(&config)) {
      if (!r->empty()) {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("{0} expected no configuration "
                                           "under plugin.{0}, but received {1} "
                                           "instead",
                                           name(), *r));
      }
    }
    return caf::none;
  }

  /// Returns the unique name of the plugin.
  [[nodiscard]] const char* name() const override {
    return "broker";
  }

  /// Returns the import format's name.
  [[nodiscard]] const char* reader_format() const override {
    return name();
  }

  /// Returns the `vast import <format>` helptext.
  [[nodiscard]] const char* reader_help() const override {
    return "imports events from Zeek via Broker";
  }

  [[nodiscard]] caf::config_option_set
  reader_options(command::opts_builder&& opts) const override {
    return std::move(opts)
      .add<bool>("listen", "listen instead of connect")
      .add<std::string>("host", "the broker endpoint host")
      .add<uint16_t>("port", "the broker endpoint port")
      .add<std::vector<std::string>>("topic", "list of topics to subscribe to")
      .finish();
  };

  [[nodiscard]] std::unique_ptr<format::reader>
  make_reader(const caf::settings& options) const override {
    return std::make_unique<reader>(options, nullptr);
  }

  [[nodiscard]] const char* writer_format() const override {
    return name();
  }

  [[nodiscard]] const char* writer_help() const override {
    return "exports events to Zeek via Broker";
  }

  [[nodiscard]] caf::config_option_set
  writer_options(command::opts_builder&& opts) const override {
    return std::move(opts)
      .add<bool>("listen", "listen instead of connect")
      .add<std::string>("host", "the broker endpoint host")
      .add<uint16_t>("port", "the broker endpoint port")
      .add<std::string>("topic", "topic to publish to")
      .finish();
  }

  [[nodiscard]] std::unique_ptr<format::writer>
  make_writer(const caf::settings& options) const override {
    return std::make_unique<writer>(options);
  }
};

} // namespace vast::plugins::broker

VAST_REGISTER_PLUGIN(vast::plugins::broker::plugin)
