//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/enumerate.hpp"

#include <tenzir/tql2/plugin.hpp>

#include "easy_client.hpp"

using namespace clickhouse;

namespace tenzir::plugins::clickhouse {

TENZIR_ENUM(mode, create_append, create, append);
TENZIR_ENUM(unsupported_types, drop, null, string);

class clickhouse_sink_operator final
  : public crtp_operator<clickhouse_sink_operator> {
public:
  struct arguments {
    tenzir::location operator_location;
    located<std::string> host;
    located<uint64_t> port;
    located<std::string> password;
    located<std::string> table;
    located<enum unsupported_types> unsupported_types
      = located{unsupported_types::string, operator_location};
    located<enum mode> mode = located{mode::create_append, operator_location};

    static auto try_parse(operator_factory_plugin::invocation inv,
                          session ctx) -> failure_or<arguments> {
      auto res = arguments{};
      res.operator_location = inv.self.get_location();
      auto unsupported_types_str = std::optional<located<std::string>>{};
      auto mode_str = std::optional<located<std::string>>{};
      auto parser
        = argument_parser2::operator_(clickhouse_sink_operator{}.name());
      parser.named("host", res.host);
      parser.named("port", res.port);
      parser.named("password", res.password);
      parser.named("table", res.table);
      parser.named("unsupported_types", unsupported_types_str);
      parser.named("mode", mode_str);
      TRY(parser.parse(inv, ctx));
#define X(TYPE)                                                                \
  if (TYPE##_str) {                                                            \
    if (auto x = from_string<enum TYPE>(TYPE##_str->inner)) {                  \
      res.TYPE = located{*x, TYPE##_str->source};                              \
    } else {                                                                   \
      diagnostic::error(                                                       \
        "`unsupported_types` must be one of `drop`, `null` or `string`")       \
        .primary(*TYPE##_str, "got `{}`", TYPE##_str->inner)                   \
        .emit(ctx);                                                            \
      return failure::promise();                                               \
    }                                                                          \
  }
      X(unsupported_types);
      X(mode);
#undef X
      return res;
    }

    auto make_client(diagnostic_handler& dh) const -> Easy_Client {
      return Easy_Client{ClientOptions()
                           .SetHost(host.inner)
                           .SetPort(static_cast<uint16_t>(port.inner))
                           .SetPassword(password.inner),
                         operator_location, dh};
    }

    friend auto inspect(auto& f, arguments& x) -> bool {
      return f.object(x).fields(
        f.field("operator_location", x.operator_location),
        f.field("host", x.host), f.field("port", x.port),
        f.field("password", x.password), f.field("table", x.table),
        f.field("unsupported_types", x.unsupported_types),
        f.field("mode", x.mode));
    }
  };

  clickhouse_sink_operator() = default;

  clickhouse_sink_operator(arguments args) : args_{std::move(args)} {
  }

  friend auto inspect(auto& f, clickhouse_sink_operator& x) -> bool {
    return f.apply(x.args_);
  }

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
    (void)filter, (void)order;
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "to_clickhouse";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto detached() const -> bool override {
    return true;
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> try {
    auto client = args_.make_client(ctrl.diagnostics());
    auto table_existed = client.table_exists(args_.table.inner);
    TENZIR_TRACE("table exists: {}", table_existed);
    auto transformations = std::optional<schema_transformations>{};
    if (table_existed) {
      transformations = client.get_schema_transformations(args_.table.inner);
      TENZIR_TRACE("got table schema: {}", transformations.has_value());
    }
    auto dropmask = std::vector<char>{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      slice = resolve_enumerations(slice);
      const auto& schema = as<record_type>(slice.schema());
      if (not transformations) {
        TENZIR_ASSERT(not table_existed);
        transformations = client.create_table(args_.table.inner, schema);
        if (not transformations) {
          diagnostic::error("failed to create table")
            .primary(args_.operator_location)
            .emit(ctrl.diagnostics());
          co_return;
        }
        TENZIR_TRACE("created table");
      }
      dropmask.clear();
      dropmask.resize(slice.rows());
      // Update the dropmask
      for (auto [i, kt] : detail::enumerate(schema.fields())) {
        const auto& [k, t] = kt;
        const auto it = transformations->find(k);
        if (it == transformations->end()) {
          diagnostic::warning("column `{}` does not exist in ClickHouse table "
                              "`{}`",
                              k, args_.table.inner)
            .primary(args_.operator_location)
            .emit(ctrl.diagnostics());
          continue;
        }
        auto offset = tenzir::offset{};
        offset.push_back(i);
        auto [t2, arr] = offset.get(slice);
        auto& trafo = it->second;
        if (trafo->update_dropmask(t2, *arr, dropmask, ctrl.diagnostics())) {
          diagnostic::warning("field `{}` is null, but the ClickHouse table "
                              "does not support null values",
                              k)
            .primary(args_.operator_location)
            .note("event will be dropped")
            .emit(ctrl.diagnostics());
        }
      }

      // Create the block
      auto block = Block{};
      for (auto [i, kt] : detail::enumerate(schema.fields())) {
        const auto& [k, t] = kt;
        const auto it = transformations->find(k);
        if (it == transformations->end()) {
          continue;
        }
        auto offset = tenzir::offset{};
        offset.push_back(i);
        auto [t2, arr] = offset.get(slice);
        auto& trafo = it->second;
        auto column
          = trafo->create_columns(t2, *arr, dropmask, ctrl.diagnostics());
        block.AppendColumn(std::string{k}, std::move(column));
      }
      if (block.GetColumnCount() > 0) {
        client.client.Insert(args_.table.inner, block);
      }
      co_yield {};
    }
  } catch (std::exception& e) {
    diagnostic::error("unexpected error: {}", e.what())
      .primary(args_.operator_location)
      .emit(ctrl.diagnostics());
    co_return;
  }

private:
  arguments args_;
};

class to_operator_plugin final
  : public operator_plugin2<clickhouse_sink_operator> {
public:
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    TRY(auto args, clickhouse_sink_operator::arguments::try_parse(inv, ctx));
    return std::make_unique<clickhouse_sink_operator>(std::move(args));
  }
};

} // namespace tenzir::plugins::clickhouse

TENZIR_REGISTER_PLUGIN(tenzir::plugins::clickhouse::to_operator_plugin)
