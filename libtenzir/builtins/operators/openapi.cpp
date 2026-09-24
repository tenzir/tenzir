//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/events.hpp>
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
    = "Use the Tenzir REST API to manage pipelines and read pipeline output "
      "from a Tenzir node.\n\n"
      "Authenticate every request with a token in the `X-Tenzir-Token` request "
      "header. Generate tokens with `tenzir-ctl web generate-token`.\n\n"
      "All endpoints are versioned. Prefix every path in this specification "
      "with `/v0`.";
  auto openapi = record{
    {"openapi", "3.0.0"},
    {"info",
     record{
       {"title", "Tenzir REST API"},
       {"version", "v0"},
       {"description", std::move(description)},
       {"license",
        record{
          {"name", "BSD-3-Clause"},
          {"url", "https://github.com/tenzir/tenzir/blob/main/LICENSE"},
        }},
     }},
    {
      "servers",
      list{{record{
        {"url", "/api/v0"},
        {"description", "Versioned API endpoint on the current Tenzir node."},
      }}},
    },
    {
      "tags",
      list{
        record{{"name", "Health"}, {"description", "Node health checks."}},
        record{{"name", "Pipelines"},
               {"description", "Pipeline lifecycle management."}},
        record{{"name", "Pipeline output"},
               {"description", "Read events produced by running pipelines."}},
      },
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

template <class Builder>
auto append_openapi_data(Builder builder, data const& value) -> void {
  match(
    value,
    [&](caf::none_t) {
      builder.null();
    },
    [&](record const& record) {
      auto result = builder.record();
      for (auto const& [name, field] : record) {
        append_openapi_data(result.field(name), field);
      }
    },
    [&](list const& list) {
      auto result = builder.list();
      for (auto const& element : list) {
        append_openapi_data(result, element);
      }
    },
    [&](std::string const& string) {
      builder.data(std::string_view{string});
    },
    [&](pattern const& pattern) {
      builder.data(pattern.string());
    },
    [&](enumeration enumeration) {
      builder.data(static_cast<uint64_t>(enumeration));
    },
    [&](map const& map) {
      auto result = builder.list();
      for (auto const& [key, field] : map) {
        auto entry = result.record();
        append_openapi_data(entry.field("key"), key);
        append_openapi_data(entry.field("value"), field);
      }
    },
    [&](blob const& blob) {
      builder.data(blob_view{blob});
    },
    [&](secret const&) {
      builder.null();
    },
    [&](auto const& x) {
      builder.data(x);
    });
}

struct OpenapiArgs {
  // No arguments.
};

class OpenapiEvents final : public Operator<void, nova::Events> {
public:
  explicit OpenapiEvents(OpenapiArgs /*args*/) {
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

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(result, ctx);
    auto builder = nova::ArrayBuilder<nova::Record>{};
    auto record = builder.record();
    for (auto const& [name, field] : openapi_record()) {
      append_openapi_data(record.field(name), field);
    }
    auto output = builder.finish();
    auto const rows = output.length();
    co_await push(
      nova::Events{std::move(output), nova::storage::BitMap{rows, true},
                   nova::Events::Meta::make_empty(rows, "tenzir.openapi")});
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

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "openapi";
  }

  auto describe() const -> Description override {
    auto d = Describer<OpenapiArgs, Openapi, OpenapiEvents>{};
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::openapi

TENZIR_REGISTER_PLUGIN(tenzir::plugins::openapi::Plugin)
