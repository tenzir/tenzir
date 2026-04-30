//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::openapi {

namespace {

auto openapi_record() -> record {
  auto paths = record{};
  auto schemas = record{};
  for (const auto* plugin : plugins::get<rest_endpoint_plugin>()) {
    auto spec = plugin->openapi_endpoints();
    for (auto& [key, value] : spec) {
      paths.emplace(key, value);
    }
    if (auto schemas_spec = plugin->openapi_schemas();
        not schemas_spec.empty()) {
      for (auto& [key, value] : schemas_spec) {
        schemas.emplace(key, value);
      }
    }
  }
  std::sort(paths.begin(), paths.end(), [](const auto& l, const auto& r) {
    return l.first < r.first;
  });
  std::sort(schemas.begin(), schemas.end(), [](const auto& l, const auto& r) {
    return l.first < r.first;
  });
  auto description
    = "This API can be used to interact with a Tenzir Node in a RESTful "
      "manner.\n\n"
      "All API requests must be authenticated with a valid token, which must "
      "be supplied in the `X-Tenzir-Token` request header. The token can be "
      "generated on the command-line using `tenzir-ctl web generate-token`.\n\n"
      "All endpoints are versioned, and must be prefixed with `/v0`.";
  auto openapi = record{
    {"openapi", "3.0.0"},
    {"info",
     record{
       {"title", "Tenzir REST API"},
       {"version", "\"v0\""},
       {"description", std::move(description)},
     }},
    {
      "servers",
      list{{record{
        {"url", "https://tenzir.example.com/api/v0"},
      }}},
    },
    {
      "security",
      list{{record{
        {"TenzirToken", list{}},
      }}},
    },
    {"components",
     record{
       {"schemas", std::move(schemas)},
       {"securitySchemes",
        record{
          {"TenzirToken",
           record{
             {"type", "apiKey"},
             {"in", "header"},
             {"name", "X-Tenzir-Token"},
           }},
        }},
     }},
    {"paths", std::move(paths)},
  };
  return openapi;
}

class openapi_operator final : public crtp_operator<openapi_operator> {
public:
  openapi_operator() = default;

  auto operator()(operator_control_plane&) const -> generator<table_slice> {
    auto builder = series_builder{};
    builder.data(openapi_record());
    co_yield builder.finish_assert_one_slice("tenzir.openapi");
  }

  auto name() const -> std::string override {
    return "openapi";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, openapi_operator& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.openapi.openapi_operator")
      .fields();
  }
};

struct OpenapiArgs {
  // No arguments.
};

class Openapi final : public Operator<void, table_slice> {
public:
  explicit Openapi(OpenapiArgs /*args*/) {
  }

  auto start(OpCtx&) -> Task<void> override {
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return {};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(result, ctx);
    auto builder = series_builder{};
    builder.data(openapi_record());
    co_await push(builder.finish_assert_one_slice("tenzir.openapi"));
    done_ = true;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  bool done_ = false;
};

class Plugin final : public virtual operator_plugin<openapi_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(argument_parser2::operator_("openapi").parse(inv, ctx));
    return std::make_unique<openapi_operator>();
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"openapi", "https://docs.tenzir.com/"
                                             "operators/openapi"};
    parser.parse(p);
    return std::make_unique<openapi_operator>();
  }

  auto describe() const -> Description override {
    auto d = Describer<OpenapiArgs, Openapi>{};
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::openapi

TENZIR_REGISTER_PLUGIN(tenzir::plugins::openapi::Plugin)
