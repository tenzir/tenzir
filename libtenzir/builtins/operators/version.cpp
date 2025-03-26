//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/exec/operator.hpp"
#include "tenzir/finalize_ctx.hpp"
#include "tenzir/substitute_ctx.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/exec/pipeline.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/version.hpp>

#include <arrow/util/config.h>
#include <boost/version.hpp>
#include <caf/actor_from_state.hpp>
#include <caf/scheduled_actor/flow.hpp>
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
  return builder.finish_assert_one_slice("tenzir.version");
}

class version_operator final : public crtp_operator<version_operator> {
public:
  version_operator() = default;

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
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

class version_exec {
public:
  explicit version_exec(exec::operator_actor::pointer self,
                        exec::shutdown_handler_actor shutdown_handler,
                        exec::stop_handler_actor stop_handler)
    : self_{self},
      shutdown_handler_{std::move(shutdown_handler)},
      stop_handler_{std::move(stop_handler)} {
  }

  auto make_behavior() -> exec::operator_actor::behavior_type {
    return {
      [this](exec::handshake hs) -> caf::result<exec::handshake_response> {
        auto version = make_version(content(self_->config()));
        auto out
          = self_->observe(as<exec::stream<void>>(hs.input), 30, 10)
              .map([](exec::message<void> msg) -> exec::message<table_slice> {
                return msg;
              })
              // TODO: Concat keeps order. We just want to inject, so merge?
              .merge(self_->make_observable().just(
                exec::message<table_slice>{std::move(version)}))
              // TODO: This is quite bad.
              .concat_map([this](exec::message<table_slice> message) {
                // TODO: This should be sent after we send the table slice?
                auto out = std::vector<exec::message<table_slice>>{};
                if (is<table_slice>(message)) {
                  TENZIR_WARN("version completed, notifying executor");
                  self_
                    ->mail(atom::done_v)
                    // TODO: Timeout.
                    .request(shutdown_handler_, std::chrono::seconds{1})
                    .then(
                      []() {
                        TENZIR_WARN("shutdown notified");
                      },
                      [](caf::error err) {
                        TENZIR_WARN("ERROR: {}", err);
                      });
                  out.reserve(2);
                  out.push_back(std::move(message));
                  out.emplace_back(exec::exhausted{});
                  TENZIR_ASSERT(stop_handler_);
                  self_->mail(atom::stop_v)
                    .request(stop_handler_, caf::infinite)
                    .then(
                      [] {
                        TENZIR_WARN("stop notified");
                      },
                      [](caf::error err) {
                        TENZIR_WARN("ERROR: {}", err);
                      });
                } else {
                  out.reserve(1);
                  out.push_back(std::move(message));
                }
                return self_->make_observable().from_container(std::move(out));
              })
              .do_on_complete([] {
                TENZIR_WARN("version stream terminated");
              })
              .to_typed_stream("version-exec", std::chrono::milliseconds{1}, 1);
        return {std::move(out)};
      },
      [](exec::checkpoint) -> caf::result<void> {
        // no post-commit logic here
        return {};
      },
      [](atom::stop) -> caf::result<void> {
        // No need to react, we are one-shot anyway.
        return {};
      },
    };
  }

private:
  exec::operator_actor::pointer self_;
  exec::shutdown_handler_actor shutdown_handler_;
  exec::stop_handler_actor stop_handler_;
};

class version_bp final : public plan::operator_base {
public:
  version_bp() = default;

  auto name() const -> std::string override {
    return "version_bp";
  }

  auto spawn(plan::operator_spawn_args args) const
    -> exec::operator_actor override {
    // TODO: Rewrite this in terms of exec::spawn_operator
    return args.sys.spawn(caf::actor_from_state<version_exec>,
                          args.shutdown_handler, args.stop_handler);
  }

  friend auto inspect(auto& f, version_bp& x) -> bool {
    return f.object(x).fields();
  }
};

class version_ir final : public ir::operator_base {
public:
  version_ir() = default;

  auto name() const -> std::string override {
    return "version_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TENZIR_UNUSED(ctx, instantiate);
    return {};
  }

  auto finalize(finalize_ctx ctx) && -> failure_or<plan::pipeline> override {
    TENZIR_UNUSED(ctx);
    return std::make_unique<version_bp>();
  }

  auto infer_type(element_type_tag input, diagnostic_handler&) const
    -> failure_or<std::optional<element_type_tag>> override {
    TENZIR_ASSERT(input == tag_v<void>);
    return tag_v<table_slice>;
  }

  friend auto inspect(auto& f, version_ir& x) -> bool {
    return f.object(x).fields();
  }
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
    -> failure_or<ir::operator_ptr> override {
    // TODO
    TENZIR_UNUSED(ctx);
    TENZIR_ASSERT(inv.args.empty());
    return std::make_unique<version_ir>();
  }
};

} // namespace

} // namespace tenzir::plugins::version

TENZIR_REGISTER_PLUGIN(tenzir::plugins::version::plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::ir::operator_base,
                            tenzir::plugins::version::version_ir>);
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::plan::operator_base,
                            tenzir::plugins::version::version_bp>);
