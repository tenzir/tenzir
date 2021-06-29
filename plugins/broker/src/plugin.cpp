//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "broker/reader.hpp"

#include <vast/data.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>

#include <caf/error.hpp>
#include <caf/settings.hpp>

namespace vast::plugins::broker {

/// The Broker reader plugin.
class plugin final : public virtual reader_plugin {
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
    return "broker";
  }

  /// Returns the `vast import <format>` helptext.
  [[nodiscard]] const char* reader_help() const override {
    return "imports events via Zeek's Broker";
  }

  /// Returns the `vast import <format>` documentation.
  [[nodiscard]] const char* reader_documentation() const override {
    return R"__(The `broker` command imports events via Zeek's Broker.

Broker provides a topic-based publish-subscribe communication layer and 
standardized data model to interact with the Zeek ecosystem. Using the `broker`
reader, VAST can transparently establish a connection to Zeek and subscribe log
events. Letting Zeek send events directly to VAST cuts out the operational
hassles of going through file-based logs.

You connect to a Zeek instance, run the `broker` command without arguments:

    # Spawn a Broker endpoint, connect to localhost:9999/tcp, and subscribe
    # to the topic `zeek/logs/` to acquire Zeek logs.
    vast import broker

Logs should now flow from Zeek to VAST, assuming that Zeek has the following
default settings:

- The script variable `Broker::default_listen_address` is set to `127.0.0.1`.
  Zeek populates this variable with the value from the environment variable
  `ZEEK_DEFAULT_LISTEN_ADDRESS`, which defaults to `127.0.0.1`. 
- The script variable `Broker::default_port` is set to `9999/tcp`.
- The script variable `Log::enable_remote_logging` is set to `T`.

Note: you can spawn Zeek with `Log::enable_local_logging=F` to avoid writing
additional local log files.

You can also spawn a Broker endpoint that is listening instead of connecting:

    # Spawn a Broker endpoint, listen on localhost:8888/tcp, and subscribe
    # to the topic `foo/bar`.
    vast import broker --listen --port=8888 --topic=foo/bar

By default, VAST automatically subscribes to the topic `zeek/logs/` because
this is where Zeek publishes log events. Use `--topic` to set a different topic.
)__";
  }

  /// Returns the options for the `vast import <format>` and `vast spawn source
  /// <format>` commands.
  [[nodiscard]] caf::config_option_set
  reader_options(command::opts_builder&& opts) const override {
    return std::move(opts)
      .add<bool>("listen", "listen instead of connect")
      .add<std::string>("host", "the broker endpoint host")
      .add<uint16_t>("port", "the broker endpoint port")
      .add<std::vector<std::string>>("topic", "list of topics to subscribe to")
      .finish();
  };

  /// Creates a reader, which will be available via `vast import <format>` and
  /// `vast spawn source <format>`.
  [[nodiscard]] std::unique_ptr<format::reader>
  make_reader(const caf::settings& options) const override {
    return std::make_unique<reader>(options, nullptr);
  }
};

} // namespace vast::plugins::broker

VAST_REGISTER_PLUGIN(vast::plugins::broker::plugin)
