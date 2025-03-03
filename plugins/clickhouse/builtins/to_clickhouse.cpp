//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/enumerate.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <boost/url/parse.hpp>

#include "easy_client.hpp"

using namespace clickhouse;

namespace tenzir::plugins::clickhouse {

TENZIR_ENUM(mode, create_append, create, append);

/// * ssl
/// * required primary key for create/create_append
/// * primary not null

class clickhouse_sink_operator final
  : public crtp_operator<clickhouse_sink_operator> {
public:
  struct arguments {
    tenzir::location operator_location;
    located<std::string> host = {"localhost", operator_location};
    located<uint16_t> port = {9000, operator_location};
    located<std::string> user = {"default", operator_location};
    located<std::string> password = {"", operator_location};
    located<std::string> table = {"REQUIRED", location::unknown};
    located<enum mode> mode = located{mode::create_append, operator_location};
    std::optional<located<std::string>> primary = std::nullopt;

    static auto try_parse(operator_factory_plugin::invocation inv,
                          session ctx) -> failure_or<arguments> {
      auto res = arguments{inv.self.get_location()};
      auto mode_str = located<std::string>{
        to_string(mode::create_append),
        res.operator_location,
      };
      auto url_str = located<std::string>{
        res.host.inner + ":" + std::to_string(res.port.inner),
        res.operator_location,
      };
      auto primary_selector = std::optional<ast::simple_selector>{};
      auto parser
        = argument_parser2::operator_(clickhouse_sink_operator{}.name());
      parser.named_optional("url", url_str);
      parser.named_optional("user", res.user);
      parser.named_optional("password", res.password);
      parser.named("table", res.table);
      parser.named_optional("mode", mode_str);
      parser.named("primary", primary_selector, "field");
      TRY(parser.parse(inv, ctx));
      auto parsed_url = boost::urls::parse_uri(url_str.inner);
      if (not parsed_url) {
        res.host = url_str;
      } else {
        if (parsed_url->has_port()) {
          res.port = {parsed_url->port_number(), url_str.source};
        }
        res.host = {parsed_url->host(), url_str.source};
      }
      if (auto x = from_string<enum mode>(mode_str.inner)) {
        res.mode = located{*x, mode_str.source};
      } else {
        diagnostic::error(
          "`mode` must be one of `create`, `append` or `create_append`")
          .primary(mode_str, "got `{}`", mode_str.inner)
          .emit(ctx);
        return failure::promise();
      }
      if (res.mode.inner == mode::create and not res.primary) {
        diagnostic::error("mode `create` requires `primary` to be set")
          .primary(mode_str)
          .emit(ctx);
        return failure::promise();
      }
      if (primary_selector) {
        auto p = primary_selector->path();
        if (p.size() > 1) {
          diagnostic::error("`primary`, must be a top level field")
            .primary(primary_selector->get_location())
            .emit(ctx);
          return failure::promise();
        }
        res.primary = {p.front().name, primary_selector->get_location()};
      }
      return res;
    }

    auto make_client(diagnostic_handler& dh) const -> Easy_Client {
      auto opts = ClientOptions()
                    .SetEndpoints({{host.inner, port.inner}})
                    .SetUser(user.inner)
                    .SetPassword(password.inner);
      return Easy_Client{
        std::move(opts),
        operator_location,
        dh,
      };
    }

    friend auto inspect(auto& f, arguments& x) -> bool {
      return f.object(x).fields(
        f.field("operator_location", x.operator_location),
        f.field("host", x.host), f.field("port", x.port),
        f.field("user", x.user), f.field("password", x.password),
        f.field("table", x.table), f.field("mode", x.mode),
        f.field("primary", x.primary));
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
    const auto table_existed = client.table_exists(args_.table.inner);
    TENZIR_TRACE("table exists: {}", table_existed);
    if (args_.mode.inner == mode::create and table_existed) {
      diagnostic::error("mode is `create`, but table `{}` already exists",
                        args_.table.inner)
        .primary(args_.mode)
        .primary(args_.table)
        .emit(ctrl.diagnostics());
      co_yield {};
      co_return;
    }
    if (args_.mode.inner == mode::create_append and not table_existed
        and not args_.primary) {
      diagnostic::error("table `{}` does not exist, but no `primary` was "
                        "specified",
                        args_.table.inner)
        .primary(args_.table)
        .emit(ctrl.diagnostics());
      co_yield {};
      co_return;
    }
    if (args_.mode.inner == mode::append and not table_existed) {
      diagnostic::error("mode is `append`, but table `{}` does not exist",
                        args_.table.inner)
        .primary(args_.mode)
        .primary(args_.table)
        .emit(ctrl.diagnostics());
      co_yield {};
      co_return;
    }
    auto transformations = std::optional<schema_transformations>{};
    if (table_existed) {
      transformations = client.get_schema_transformations(args_.table.inner);
      TENZIR_TRACE("got table schema: {}", transformations.has_value());
    }
    /// TODO. somehow merge this with the nested record implementation
    auto dropmask = dropmask_type{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      if (slice.columns() == 0) {
        co_yield {};
        continue;
      }
      slice = resolve_enumerations(slice);
      const auto& schema = as<record_type>(slice.schema());
      if (not transformations) {
        TENZIR_ASSERT(not table_existed);
        transformations
          = client.create_table(args_.table.inner, *args_.primary, schema);
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
        const auto updated
          = trafo->update_dropmask(t2, *arr, dropmask, ctrl.diagnostics());
        if (updated == transformer::drop::some) {
          diagnostic::warning("field `{}` contains null, but the ClickHouse "
                              "table does not support null values",
                              k)
            .primary(args_.operator_location)
            .note("event will be dropped")
            .emit(ctrl.diagnostics());
        }
        /// A diagnostic for this was already emitted
        if (updated == transformer::drop::all) {
          continue;
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
          = trafo->create_column(t2, *arr, dropmask, ctrl.diagnostics());
        block.AppendColumn(std::string{k}, std::move(column));
      }
      if (block.GetColumnCount() > 0) {
        client.client.Insert(args_.table.inner, block);
      }
      co_yield {};
    }
  } catch (std::exception& e) {
    // TODO `TENZIR_ASSERT` currently throws a runtime error, which is caught
    // here. Once the custom exception type for this is merged, we can
    // catch&rethrow that instead of reporting it ourselves.
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
