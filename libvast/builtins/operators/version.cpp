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
#include <vast/tql/argument_parser.hpp>
#include <vast/tql/diagnostics.hpp>
#include <vast/tql/parser_interface.hpp>

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

auto add_value(auto& field, auto value) {
  [[maybe_unused]] auto err = field.add(value);
  VAST_ASSERT(not err);
}

class version_operator final : public crtp_operator<version_operator> {
public:
  version_operator() = default;

  explicit version_operator(bool dev_mode) : dev_mode_{dev_mode} {
  }

  auto operator()() const -> generator<table_slice> {
    auto builder = adaptive_table_slice_builder{};
    {
      auto row = builder.push_row();
      {
        auto version_field = row.push_field("version");
        add_value(version_field, vast::version::version);
      }
      if (dev_mode_) {
        auto build_field = row.push_field("build");
        auto build = build_field.push_record();
        {
          auto type_field = build.push_field("type");
          add_value(type_field, vast::version::build::type);
        }
        {
          auto tree_hash_field = build.push_field("tree_hash");
          add_value(tree_hash_field, vast::version::build::tree_hash);
        }
        {
          auto assertions_field = build.push_field("assertions");
          add_value(assertions_field, vast::version::build::has_assertions);
        }
        {
          auto sanitizers_field = build.push_field("sanitizers");
          auto sanitizers = sanitizers_field.push_record();
          {
            auto address_field = sanitizers.push_field("address");
            add_value(address_field,
                      vast::version::build::has_address_sanitizer);
          }
          {
            auto undefined_behavior_field
              = sanitizers.push_field("undefined_behavior");
            add_value(undefined_behavior_field,
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
      add_value(name_field, #name);                                            \
    }                                                                          \
    {                                                                          \
      const auto version_string = std::string{(version)}; /*NOLINT*/           \
      if (version_string.empty()) {                                            \
        auto version_field = name##_record.push_field("version");              \
        add_value(version_field, std::string_view{version_string});            \
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
            add_value(name_field, plugin->name());
          }
          {
            auto version_field = plugin_record.push_field("version");
            if (const auto* version = plugin.version()) {
              add_value(version_field, version);
            } else {
              add_value(version_field, "bundled");
            }
          }
          if (dev_mode_) {
            auto types_field = plugin_record.push_field("types");
            auto types = types_field.push_list();
#define VAST_ADD_PLUGIN_TYPE(category)                                         \
  do {                                                                         \
    if (plugin.as<category##_plugin>()) {                                      \
      add_value(types, #category);                                             \
    }                                                                          \
  } while (false)
            VAST_ADD_PLUGIN_TYPE(component);
            VAST_ADD_PLUGIN_TYPE(analyzer);
            VAST_ADD_PLUGIN_TYPE(command);
            VAST_ADD_PLUGIN_TYPE(reader);
            VAST_ADD_PLUGIN_TYPE(writer);
            VAST_ADD_PLUGIN_TYPE(operator_parser);
            VAST_ADD_PLUGIN_TYPE(operator_serialization);
            VAST_ADD_PLUGIN_TYPE(aggregation_function);
            VAST_ADD_PLUGIN_TYPE(language);
            VAST_ADD_PLUGIN_TYPE(rest_endpoint);
            VAST_ADD_PLUGIN_TYPE(loader_parser);
            VAST_ADD_PLUGIN_TYPE(loader_serialization);
            VAST_ADD_PLUGIN_TYPE(parser_parser);
            VAST_ADD_PLUGIN_TYPE(parser_serialization);
            VAST_ADD_PLUGIN_TYPE(printer_parser);
            VAST_ADD_PLUGIN_TYPE(printer_serialization);
            VAST_ADD_PLUGIN_TYPE(saver_parser);
            VAST_ADD_PLUGIN_TYPE(saver_serialization);
            VAST_ADD_PLUGIN_TYPE(store);
#undef VAST_ADD_PLUGIN_TYPE
          }
          if (dev_mode_) {
            auto kind_field = plugin_record.push_field("kind");
            switch (plugin.type()) {
              case plugin_ptr::type::builtin:
                add_value(kind_field, "builtin");
                break;
              case plugin_ptr::type::static_:
                add_value(kind_field, "static");
                break;
              case plugin_ptr::type::dynamic:
                add_value(kind_field, "dynamic");
                break;
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

  auto name() const -> std::string override {
    return "version";
  }

  friend auto inspect(auto& f, version_operator& x) -> bool {
    return f.apply(x.dev_mode_);
  }

private:
  bool dev_mode_;
};

class plugin final : public virtual operator_plugin<version_operator> {
public:
  auto parse_operator(tql::parser_interface& p) const -> operator_ptr override {
    auto parser = tql::argument_parser{
      "version", "https://vast.io/docs/understand/operators/sources/version"};
    auto dev = false;
    parser.add("--dev", dev);
    parser.parse(p);
    return std::make_unique<version_operator>(dev);
  }
};

} // namespace

} // namespace vast::plugins::version

VAST_REGISTER_PLUGIN(vast::plugins::version::plugin)
