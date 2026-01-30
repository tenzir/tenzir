//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/substitute_ctx.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/ir.hpp>
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
          {"tree_hash", string_type{}},
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

class version_operator final : public crtp_operator<version_operator> {
public:
  version_operator() = default;

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    using T = exec_node_actor::base;
    co_yield make_version(content(ctrl.self().config()));
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

class Version final : public Operator<void, table_slice> {
public:
  auto start(OpCtx& ctx) -> Task<void> override {
    // diagnostic::warning("HELLO from version").emit(ctx);
    // auto slice = make_version(caf::content(ctx.actor_system().config()));
    // co_await push(slice);
    TENZIR_INFO("leaving Version::start");
    co_return;
  }

  auto await_task() const -> Task<std::any> override {
    // This is just a test to see what happens if we want to return the version
    // a certain number of times with 1 second of sleep in between.
    if (count_ == total) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    if (count_ != 0) {
      co_await folly::coro::sleep(std::chrono::milliseconds{200});
    }
    co_return {};
  }

  auto process_task(std::any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    // throw std::runtime_error{"oh no"};
    // auto fs = ctx.actor_system().spawn(posix_filesystem, "/");
    // auto license
    //   = co_await ctx
    //       .mail(atom::read_v,
    //             std::filesystem::path{"/Users/jannis/tenzir/LICENSE"})
    //       .request(fs);
    // if (license) {
    //   auto& chunk = *license;
    //   TENZIR_ERROR(
    //     "{}", std::string_view{reinterpret_cast<const char*>(chunk->data()),
    //                            chunk->size()});
    // } else {
    //   TENZIR_ERROR("got error");
    // }
    TENZIR_WARN("processing task with count == {}", count_);
    TENZIR_ASSERT(count_ < total);
    auto slice = make_version(caf::content(ctx.actor_system().config()));
    co_await push(slice);
    count_ += 1;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("count", count_);
  }

  auto state() -> OperatorState override {
    TENZIR_ERROR("querying state of version with {}", count_);
    if (count_ == total) {
      return OperatorState::done;
    }
    return OperatorState::unspecified;
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
  auto spawn(element_type_tag input) && -> AnyOperator override {
    TENZIR_ASSERT(input.is<void>());
    return Version{};
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    // TODO: Refactor.
    if (not input.is<void>()) {
      diagnostic::error("expected void, got {}", input)
        .primary(main_location())
        .emit(dh);
      return failure::promise();
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

class plugin final : public virtual operator_plugin<version_operator>,
                     public virtual operator_factory_plugin,
                     public virtual operator_compiler_plugin {
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

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<Box<ir::Operator>> override {
    // TODO
    TENZIR_UNUSED(ctx);
    TENZIR_ASSERT(inv.args.empty());
    return version_ir{inv.op.get_location()};
  }
};

} // namespace

} // namespace tenzir::plugins::version

TENZIR_REGISTER_PLUGIN(tenzir::plugins::version::plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::ir::Operator,
                            tenzir::plugins::version::version_ir>);
