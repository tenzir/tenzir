//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>

namespace tenzir::plugins::schemaof {

namespace {

class schemaof_operator final : public crtp_operator<schemaof_operator> {
public:
  schemaof_operator() = default;

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    auto builder = series_builder{};
    auto seen_schemas = std::unordered_set<type>{};
    for (auto&& events : input) {
      if (events.rows() == 0) {
        co_yield {};
        continue;
      }
      const auto [_, inserted] = seen_schemas.insert(events.schema());
      if (not inserted) {
        continue;
      }
      auto result = builder.record();
      result.field("schema", events.schema().name());
      result.field("schema_id", events.schema().make_fingerprint());
      result.field("definition",
                   fmt::to_string(caf::get<record_type>(events.schema())));
      co_yield builder.finish_assert_one_slice("tenzir.schema");
    }
  }

  auto name() const -> std::string override {
    return "schemaof";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    (void)order;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, schemaof_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin<schemaof_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"schemaof",
                                  "https://docs.tenzir.com/operators/schemaof"};
    parser.parse(p);
    return std::make_unique<schemaof_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::schemaof

TENZIR_REGISTER_PLUGIN(tenzir::plugins::schemaof::plugin)
