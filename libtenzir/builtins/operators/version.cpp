//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/version.hpp>

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

namespace tenzir::plugins::version {

namespace {

class version_operator final : public crtp_operator<version_operator> {
public:
  version_operator() = default;

  auto operator()(exec_ctx) const -> generator<table_slice> {
    auto builder = series_builder{};
    auto event = builder.record();
    event.field("version", tenzir::version::version);
    event.field("tag", tenzir::version::build_metadata);
    event.field("major", tenzir::version::major);
    event.field("minor", tenzir::version::minor);
    event.field("patch", tenzir::version::patch);
    auto features = event.field("features").list();
    for (const auto& feature : tenzir_features()) {
      features.data(feature);
    }
    auto build = event.field("build").record();
    build.field("type").data(tenzir::version::build::type);
    build.field("tree_hash").data(tenzir::version::build::tree_hash);
    build.field("assertions").data(tenzir::version::build::has_assertions);
    auto sanitizers = build.field("sanitizers").record();
    sanitizers.field("address", tenzir::version::build::has_address_sanitizer);
    sanitizers.field("undefined_behavior",
                     tenzir::version::build::has_undefined_behavior_sanitizer);
    auto dependencies = event.field("dependencies").list();
#define X(name, version)                                                       \
  do {                                                                         \
    auto entry = dependencies.record();                                        \
    entry.field("name").data(#name);                                           \
    const auto version_string = std::string{(version)}; /*NOLINT*/             \
    if (not version_string.empty()) {                                          \
      entry.field("version").data(version_string);                             \
    }                                                                          \
  } while (false)
    X(arrow, fmt::format("{}.{}.{}", ARROW_VERSION_MAJOR, ARROW_VERSION_MINOR,
                         ARROW_VERSION_PATCH));
    X(boost, fmt::format("{}.{}.{}", BOOST_VERSION / 100000,
                         BOOST_VERSION / 100 % 1000, BOOST_VERSION % 100));
    X(caf, fmt::format("{}.{}.{}", CAF_MAJOR_VERSION, CAF_MINOR_VERSION,
                       CAF_PATCH_VERSION));
    X(fast_float, "");
    X(flatbuffers,
      fmt::format("{}.{}.{}", FLATBUFFERS_VERSION_MAJOR,
                  FLATBUFFERS_VERSION_MINOR, FLATBUFFERS_VERSION_REVISION));
    X(fmt, fmt::format("{}.{}.{}", FMT_VERSION / 10000,
                       FMT_VERSION % 10000 / 100, FMT_VERSION % 100));
#if TENZIR_ENABLE_JEMALLOC
    X(jemalloc, JEMALLOC_VERSION);
#endif
#if TENZIR_ENABLE_LIBUNWIND
    X(libunwind, "");
#endif
    X(openssl, fmt::format("{}.{}.{}", OPENSSL_CONFIGURED_API / 10000,
                           OPENSSL_CONFIGURED_API % 10000 / 100,
                           OPENSSL_CONFIGURED_API % 100));
    X(re2, "");
    X(robin_map, "");
    X(simdjson, SIMDJSON_VERSION);
    X(spdlog, fmt::format("{}.{}.{}", SPDLOG_VER_MAJOR, SPDLOG_VER_MINOR,
                          SPDLOG_VER_PATCH));
    X(xxhash, fmt::format("{}.{}.{}", XXH_VERSION_MAJOR, XXH_VERSION_MINOR,
                          XXH_VERSION_RELEASE));
    X(yaml_cpp, "");
#undef X
    for (auto&& slice : builder.finish_as_table_slice("tenzir.version")) {
      co_yield std::move(slice);
    }
  }

  auto name() const -> std::string override {
    return "version";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  auto internal() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, version_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin<version_operator>,
                     operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"version", "https://docs.tenzir.com/"
                                             "operators/version"};
    parser.parse(p);
    return std::make_unique<version_operator>();
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("version").parse(inv, ctx).ignore();
    return std::make_unique<version_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::version

TENZIR_REGISTER_PLUGIN(tenzir::plugins::version::plugin)
