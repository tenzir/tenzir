//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/adaptive_table_slice_builder.hpp>
#include <vast/concept/parseable/string/char_class.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>

#include <arrow/util/config.h>
#include <boost/version.hpp>
#include <flatbuffers/base.h>
#include <openssl/configuration.h>

#include <simdjson.h>
#include <xxhash.h>

#include <yaml-cpp/yaml.h>

#if VAST_ENABLE_JEMALLOC
#  include <jemalloc/jemalloc.h>
#endif

namespace vast::plugins::version {

namespace {

class version_operator final : public crtp_operator<version_operator> {
public:
  explicit version_operator(bool dev_mode) : dev_mode_{dev_mode} {
  }

  auto operator()() const -> generator<table_slice> {
    auto builder = adaptive_table_slice_builder{};
    {
      auto row = builder.push_row();
      {
        auto version_field = row.push_field("version");
        version_field.add(vast::version::version);
      }
      if (dev_mode_) {
        auto build_field = row.push_field("build");
        auto build = build_field.push_record();
        {
          auto type_field = build.push_field("type");
          type_field.add(vast::version::build::type);
        }
        {
          auto tree_hash_field = build.push_field("tree_hash");
          tree_hash_field.add(vast::version::build::tree_hash);
        }
        {
          auto assertions_field = build.push_field("assertions");
          assertions_field.add(vast::version::build::has_assertions);
        }
        {
          auto sanitizers_field = build.push_field("sanitizers");
          auto sanitizers = sanitizers_field.push_record();
          {
            auto address_field = sanitizers.push_field("address");
            address_field.add(vast::version::build::has_address_sanitizer);
          }
          {
            auto undefined_behavior_field
              = sanitizers.push_field("undefined_behavior");
            undefined_behavior_field.add(
              vast::version::build::has_undefined_behavior_sanitizer);
          }
        }
      }
      if (dev_mode_) {
        auto dependencies_field = row.push_field("dependencies");
        auto dependencies = dependencies_field.push_list();
#define VAST_ADD_DEPENDENCY(name, version)                                     \
  do {                                                                         \
    auto name##_record = dependencies.push_record();                           \
    {                                                                          \
      auto name_field = name##_record.push_field("name");                      \
      name_field.add(#name);                                                   \
    }                                                                          \
    {                                                                          \
      const auto version_string = std::string{(version)}; /*NOLINT*/           \
      if (version_string.empty()) {                                            \
        auto version_field = name##_record.push_field("version");              \
        version_field.add(std::string_view{version_string});                   \
      }                                                                        \
    }                                                                          \
  } while (false)
        VAST_ADD_DEPENDENCY(arrow, fmt::format("{}.{}.{}", ARROW_VERSION_MAJOR,
                                               ARROW_VERSION_MINOR,
                                               ARROW_VERSION_PATCH));
        VAST_ADD_DEPENDENCY(
          boost, fmt::format("{}.{}.{}", BOOST_VERSION / 100000,
                             BOOST_VERSION / 100 % 1000, BOOST_VERSION % 100));
        VAST_ADD_DEPENDENCY(caf,
                            fmt::format("{}.{}.{}", CAF_MAJOR_VERSION,
                                        CAF_MINOR_VERSION, CAF_PATCH_VERSION));
        VAST_ADD_DEPENDENCY(fast_float, "");
        VAST_ADD_DEPENDENCY(flatbuffers,
                            fmt::format("{}.{}.{}", FLATBUFFERS_VERSION_MAJOR,
                                        FLATBUFFERS_VERSION_MINOR,
                                        FLATBUFFERS_VERSION_REVISION));
        VAST_ADD_DEPENDENCY(fmt, fmt::format("{}.{}.{}", FMT_VERSION / 10000,
                                             FMT_VERSION % 10000 / 100,
                                             FMT_VERSION % 100));
#if VAST_ENABLE_JEMALLOC
        VAST_ADD_DEPENDENCY(jemalloc, JEMALLOC_VERSION);
#endif
#if VAST_ENABLE_LIBUNWIND
        VAST_ADD_DEPENDENCY(libunwind, "");
#endif
        VAST_ADD_DEPENDENCY(
          openssl, fmt::format("{}.{}.{}", OPENSSL_CONFIGURED_API / 10000,
                               OPENSSL_CONFIGURED_API % 10000 / 100,
                               OPENSSL_CONFIGURED_API % 100));
        VAST_ADD_DEPENDENCY(re2, "");
        VAST_ADD_DEPENDENCY(robin_map, "");
        VAST_ADD_DEPENDENCY(simdjson, SIMDJSON_VERSION);
        VAST_ADD_DEPENDENCY(spdlog,
                            fmt::format("{}.{}.{}", SPDLOG_VER_MAJOR,
                                        SPDLOG_VER_MINOR, SPDLOG_VER_PATCH));
        VAST_ADD_DEPENDENCY(xxhash, fmt::format("{}.{}.{}", XXH_VERSION_MAJOR,
                                                XXH_VERSION_MINOR,
                                                XXH_VERSION_RELEASE));
        VAST_ADD_DEPENDENCY(yaml_cpp, "");
#undef VAST_ADD_DEPENDENCY
      }
      {
        auto plugins_field = row.push_field("plugins");
        auto plugins = plugins_field.push_list();
        for (const auto& plugin : plugins::get()) {
          if (not dev_mode_ && plugin.type() == plugin_ptr::type::builtin)
            continue;
          auto plugin_record = plugins.push_record();
          {
            auto name_field = plugin_record.push_field("name");
            name_field.add(plugin->name());
          }
          {
            auto types_field = plugin_record.push_field("types");
            auto types = types_field.push_list();
#define VAST_ADD_PLUGIN_TYPE(category)                                         \
  do {                                                                         \
    if (plugin.as<category##_plugin>()) {                                      \
      types.add(#category);                                                    \
    }                                                                          \
  } while (false)
            VAST_ADD_PLUGIN_TYPE(component);
            VAST_ADD_PLUGIN_TYPE(analyzer);
            VAST_ADD_PLUGIN_TYPE(command);
            VAST_ADD_PLUGIN_TYPE(reader);
            VAST_ADD_PLUGIN_TYPE(writer);
            VAST_ADD_PLUGIN_TYPE(operator);
            VAST_ADD_PLUGIN_TYPE(aggregation_function);
            VAST_ADD_PLUGIN_TYPE(language);
            VAST_ADD_PLUGIN_TYPE(rest_endpoint);
            VAST_ADD_PLUGIN_TYPE(loader);
            VAST_ADD_PLUGIN_TYPE(parser);
            VAST_ADD_PLUGIN_TYPE(printer);
            VAST_ADD_PLUGIN_TYPE(saver);
            VAST_ADD_PLUGIN_TYPE(store);
#undef VAST_ADD_PLUGIN_TYPE
          }
          if (dev_mode_) {
            auto kind_field = plugin_record.push_field("kind");
            switch (plugin.type()) {
              case plugin_ptr::type::builtin:
                kind_field.add("builtin");
                break;
              case plugin_ptr::type::static_:
                kind_field.add("static");
                break;
              case plugin_ptr::type::dynamic:
                kind_field.add("dynamic");
                break;
            }
          }
          if (dev_mode_) {
            auto version_field = plugin_record.push_field("version");
            if (const auto* version = plugin.version()) {
              version_field.add(version);
            } else {
              version_field.add("bundled");
            }
          }
        }
      }
    }
    co_yield builder.finish();
  }

  auto to_string() const -> std::string override {
    return dev_mode_ ? "version --dev" : "version";
  }

private:
  bool dev_mode_;
};

class plugin final : public virtual operator_plugin {
public:
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "version";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::required_ws_or_comment,
      parsers::end_of_pipeline_operator, parsers::str;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    auto dev_mode = std::string{};
    const auto p = -(required_ws_or_comment >> str{"--dev"})
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    if (!p(f, l, dev_mode)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "version operator: '{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<version_operator>(not dev_mode.empty()),
    };
  }
};

} // namespace

} // namespace vast::plugins::version

VAST_REGISTER_PLUGIN(vast::plugins::version::plugin)
