//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/adaptive_table_slice_builder.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/util/config.h>
#include <boost/version.hpp>
#include <flatbuffers/base.h>
#include <openssl/configuration.h>

#include <simdjson.h>
#include <xxhash.h>

#include <yaml-cpp/yaml.h>

#if TENZIR_ENABLE_JEMALLOC
#  include <jemalloc/jemalloc.h>
#endif

namespace tenzir::plugins::dependencies {

namespace {

class plugin final : public virtual aspect_plugin {
public:
  auto name() const -> std::string override {
    return "dependencies";
  }

  auto location() const -> operator_location override {
    return operator_location::anywhere;
  }

  auto show(operator_control_plane&) const -> generator<table_slice> override {
    auto builder = adaptive_table_slice_builder{};
#define TENZIR_ADD_DEPENDENCY(name, version)                                   \
  do {                                                                         \
    auto row = builder.push_row();                                             \
    auto err = row.push_field("name").add(#name);                              \
    TENZIR_ASSERT_CHEAP(not err);                                              \
    const auto version_string = std::string{(version)}; /*NOLINT*/             \
    auto version_field = row.push_field("version");                            \
    if (not version_string.empty()) {                                          \
      err = version_field.add(version_string);                                 \
      TENZIR_ASSERT_CHEAP(not err);                                            \
    }                                                                          \
  } while (false)
    TENZIR_ADD_DEPENDENCY(arrow, fmt::format("{}.{}.{}", ARROW_VERSION_MAJOR,
                                             ARROW_VERSION_MINOR,
                                             ARROW_VERSION_PATCH));
    TENZIR_ADD_DEPENDENCY(boost, fmt::format("{}.{}.{}", BOOST_VERSION / 100000,
                                             BOOST_VERSION / 100 % 1000,
                                             BOOST_VERSION % 100));
    TENZIR_ADD_DEPENDENCY(caf,
                          fmt::format("{}.{}.{}", CAF_MAJOR_VERSION,
                                      CAF_MINOR_VERSION, CAF_PATCH_VERSION));
    TENZIR_ADD_DEPENDENCY(fast_float, "");
    TENZIR_ADD_DEPENDENCY(flatbuffers,
                          fmt::format("{}.{}.{}", FLATBUFFERS_VERSION_MAJOR,
                                      FLATBUFFERS_VERSION_MINOR,
                                      FLATBUFFERS_VERSION_REVISION));
    TENZIR_ADD_DEPENDENCY(fmt, fmt::format("{}.{}.{}", FMT_VERSION / 10000,
                                           FMT_VERSION % 10000 / 100,
                                           FMT_VERSION % 100));
#if TENZIR_ENABLE_JEMALLOC
    TENZIR_ADD_DEPENDENCY(jemalloc, JEMALLOC_VERSION);
#endif
#if TENZIR_ENABLE_LIBUNWIND
    TENZIR_ADD_DEPENDENCY(libunwind, "");
#endif
    TENZIR_ADD_DEPENDENCY(
      openssl, fmt::format("{}.{}.{}", OPENSSL_CONFIGURED_API / 10000,
                           OPENSSL_CONFIGURED_API % 10000 / 100,
                           OPENSSL_CONFIGURED_API % 100));
    TENZIR_ADD_DEPENDENCY(re2, "");
    TENZIR_ADD_DEPENDENCY(robin_map, "");
    TENZIR_ADD_DEPENDENCY(simdjson, SIMDJSON_VERSION);
    TENZIR_ADD_DEPENDENCY(spdlog,
                          fmt::format("{}.{}.{}", SPDLOG_VER_MAJOR,
                                      SPDLOG_VER_MINOR, SPDLOG_VER_PATCH));
    TENZIR_ADD_DEPENDENCY(xxhash,
                          fmt::format("{}.{}.{}", XXH_VERSION_MAJOR,
                                      XXH_VERSION_MINOR, XXH_VERSION_RELEASE));
    TENZIR_ADD_DEPENDENCY(yaml_cpp, "");
#undef TENZIR_ADD_DEPENDENCY
    auto result = builder.finish();
    auto renamed_schema
      = type{"tenzir.dependency", caf::get<record_type>(result.schema())};
    co_yield cast(std::move(result), renamed_schema);
  } // namespace
};  // namespace

} // namespace

} // namespace tenzir::plugins::dependencies

TENZIR_REGISTER_PLUGIN(tenzir::plugins::dependencies::plugin)
