//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/substitute_ctx.hpp"

#include <tenzir/data.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/nova/type_system.hpp>
#include <tenzir/nova/union_array.hpp>
#include <tenzir/nova_flag.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/version.hpp>

#include <arrow/util/config.h>
#include <boost/version.hpp>
#include <flatbuffers/base.h>
#include <folly/coro/Sleep.h>
#include <openssl/configuration.h>

#include <simdjson.h>
#include <xxhash.h>

#include <yaml-cpp/yaml.h>

namespace tenzir::plugins::version {

namespace {

/// Returns the version information for a node with the configuration `config`.
auto make_version_record(const record& config) -> nova::Record {
  auto features = nova::List{};
  for (auto& feature : tenzir_features(config)) {
    features.emplace_back(std::move(feature));
  }
  auto dependencies = nova::List{};
  auto add_dependency = [&](std::string name, std::string version) {
    dependencies.emplace_back(nova::Record{
      {"name", std::move(name)},
      {"version",
       version.empty() ? nova::Data{} : nova::Data{std::move(version)}},
    });
  };
  add_dependency("arrow",
                 fmt::format("{}.{}.{}", ARROW_VERSION_MAJOR,
                             ARROW_VERSION_MINOR, ARROW_VERSION_PATCH));
  add_dependency("boost",
                 fmt::format("{}.{}.{}", BOOST_VERSION / 100000,
                             BOOST_VERSION / 100 % 1000, BOOST_VERSION % 100));
  add_dependency("caf", fmt::format("{}.{}.{}", CAF_MAJOR_VERSION,
                                    CAF_MINOR_VERSION, CAF_PATCH_VERSION));
  add_dependency("flatbuffers",
                 fmt::format("{}.{}.{}", FLATBUFFERS_VERSION_MAJOR,
                             FLATBUFFERS_VERSION_MINOR,
                             FLATBUFFERS_VERSION_REVISION));
  add_dependency("fmt",
                 fmt::format("{}.{}.{}", FMT_VERSION / 10000,
                             FMT_VERSION % 10000 / 100, FMT_VERSION % 100));
#if TENZIR_ENABLE_LIBUNWIND
  add_dependency("libunwind", "");
#endif
  add_dependency("openssl",
                 fmt::format("{}.{}.{}", OPENSSL_CONFIGURED_API / 10000,
                             OPENSSL_CONFIGURED_API % 10000 / 100,
                             OPENSSL_CONFIGURED_API % 100));
  add_dependency("re2", "");
  add_dependency("robin_map", "");
  add_dependency("simdjson", SIMDJSON_VERSION);
  add_dependency("spdlog", fmt::format("{}.{}.{}", SPDLOG_VER_MAJOR,
                                       SPDLOG_VER_MINOR, SPDLOG_VER_PATCH));
  add_dependency("xxhash", fmt::format("{}.{}.{}", XXH_VERSION_MAJOR,
                                       XXH_VERSION_MINOR, XXH_VERSION_RELEASE));
  add_dependency("yaml_cpp", "");
  return nova::Record{
    {"version", std::string{tenzir::version::version}},
    {"tag", std::string{tenzir::version::build_metadata}},
    {"major", uint64_t{tenzir::version::major}},
    {"minor", uint64_t{tenzir::version::minor}},
    {"patch", uint64_t{tenzir::version::patch}},
    {"features", std::move(features)},
    {
      "build",
      nova::Record{
        {"type", std::string{tenzir::version::build::type}},
        {"assertions", tenzir::version::build::has_assertions},
        {
          "sanitizers",
          nova::Record{
            {"address", tenzir::version::build::has_address_sanitizer},
            {"undefined_behavior",
             tenzir::version::build::has_undefined_behavior_sanitizer},
          },
        },
      },
    },
    {"dependencies", std::move(dependencies)},
  };
}

auto make_version_record(const caf::actor_system_config& config)
  -> nova::Record {
  return make_version_record(check(to<record>(caf::content(config))));
}

auto make_version(const caf::settings& settings) -> table_slice {
  auto builder = series_builder{type{
    "tenzir.version",
    record_type{
      {"version", string_type{}},
      {"tag", string_type{}},
      {"major", uint64_type{}},
      {"minor", uint64_type{}},
      {"patch", uint64_type{}},
      {"features", list_type{string_type{}}},
      {
        "build",
        record_type{
          {"type", string_type{}},
          {"assertions", bool_type{}},
          {
            "sanitizers",
            record_type{
              {"address", bool_type{}},
              {"undefined_behavior", bool_type{}},
            },
          },
        },
      },
      {
        "dependencies",
        list_type{record_type{
          {"name", string_type{}},
          {"version", string_type{}},
        }},
      },
    },
  }};
  auto event = builder.record();
  event.field("version", tenzir::version::version);
  event.field("tag", tenzir::version::build_metadata);
  event.field("major", tenzir::version::major);
  event.field("minor", tenzir::version::minor);
  event.field("patch", tenzir::version::patch);
  auto features = event.field("features").list();
  for (const auto& feature : tenzir_features(check(to<record>(settings)))) {
    features.data(feature);
  }
  auto build = event.field("build").record();
  build.field("type").data(tenzir::version::build::type);
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
  X(flatbuffers,
    fmt::format("{}.{}.{}", FLATBUFFERS_VERSION_MAJOR,
                FLATBUFFERS_VERSION_MINOR, FLATBUFFERS_VERSION_REVISION));
  X(fmt, fmt::format("{}.{}.{}", FMT_VERSION / 10000, FMT_VERSION % 10000 / 100,
                     FMT_VERSION % 100));
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
  return builder.finish_assert_one_slice("tenzir.version");
}

class VersionEvents final : public Operator<void, nova::Events> {
public:
  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (count_ == total) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    if (count_ != 0) {
      co_await folly::coro::sleep(std::chrono::seconds{2});
    }
    co_return {};
  }

  auto process_task(Any, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_ASSERT(count_ < total);
    auto result = nova::Array<nova::Record>{
      nova::storage::ConstantStorage<nova::Record, nova::RowView<nova::Record>>{
        1, make_version_record(ctx.actor_system().config())}};
    co_await push(
      nova::Events{std::move(result), nova::storage::BitMap{1, true},
                   nova::Events::Meta::make_empty(1, "tenzir.version")});
    count_ += 1;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("count", count_);
  }

  auto state() -> OperatorState override {
    return count_ == total ? OperatorState::done : OperatorState::normal;
  }

private:
  static constexpr size_t total = 1;
  size_t count_ = 0;
};

class Version final : public Operator<void, table_slice> {
public:
  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    // This is just a test to see what happens if we want to return the version
    // a certain number of times with 1 second of sleep in between.
    if (count_ == total) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    if (count_ != 0) {
      co_await folly::coro::sleep(std::chrono::seconds{2});
    }
    co_return {};
  }

  auto process_task(Any, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_ASSERT(count_ < total);
    auto slice = make_version(caf::content(ctx.actor_system().config()));
    co_await push(slice);
    count_ += 1;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("count", count_);
  }

  auto state() -> OperatorState override {
    if (count_ == total) {
      return OperatorState::done;
    }
    return OperatorState::normal;
  }

private:
  static constexpr size_t total = 1;
  size_t count_ = 0;
};

class version_ir final : public ir::Operator {
public:
  version_ir() = default;

  explicit version_ir(location self) : self_{self} {
  }

  auto name() const -> std::string override {
    return "version_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TENZIR_UNUSED(ctx, instantiate);
    return {};
  }
  auto spawn(element_type_tag input, registry const&) const
    -> AnyOperator override {
    TENZIR_ASSERT(input.is<void>());
    if (nova_enabled()) {
      return VersionEvents{}.with_name("version");
    }
    return Version{}.with_name("version");
  }

  auto infer_type(element_type_tag input, base_ctx ctx) const
    -> failure_or<element_type_tag> override {
    // TODO: Refactor.
    if (not input.is<void>()) {
      diagnostic::error("expected void, got {}", input)
        .primary(main_location())
        .emit(ctx);
      return failure::promise();
    }
    if (nova_enabled()) {
      return tag_v<nova::Events>;
    }
    return tag_v<table_slice>;
  }

  auto main_location() const -> location override {
    return self_;
  }

  friend auto inspect(auto& f, version_ir& x) -> bool {
    return f.object(x).fields(f.field("self", x.self_));
  }

private:
  location self_;
};

class plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "version";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    // TODO
    TENZIR_UNUSED(ctx);
    TENZIR_ASSERT(inv.args.empty());
    return version_ir{inv.op.get_location()};
  }
};

struct VersionArgs {
  nova::Data version;
};

struct VersionFunction {
  static auto eval(VersionArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova::repeat(args.version, frame.length());
  }
};

class version_function final : public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "version-function";
  }

  auto function_name() const -> std::string override {
    return "version";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    TENZIR_UNUSED(plugin_config);
    version_ = make_version_record(global_config);
    return {};
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<VersionArgs, VersionFunction>{};
    d.validate(
      [this](VersionArgs& args, diagnostic_handler&) -> failure_or<void> {
        args.version = version_;
        return {};
      });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    diagnostic::error("`version` requires `--nova`").primary(inv.call).emit(ctx);
    return failure::promise();
  }

private:
  nova::Record version_;
};

} // namespace

} // namespace tenzir::plugins::version

TENZIR_REGISTER_PLUGIN(tenzir::plugins::version::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::version::version_function)
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::ir::Operator,
                            tenzir::plugins::version::version_ir>);
