//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/cast.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::set_attributes {

namespace {

using configuration = detail::stable_map<std::string, std::string>;

class set_attributes_operator final
  : public crtp_operator<set_attributes_operator> {
public:
  set_attributes_operator() = default;

  explicit set_attributes_operator(configuration&& cfg) : cfg_(std::move(cfg)) {
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    std::unordered_map<type, type> enriched_schemas_cache{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto original_schema = slice.schema();
      if (auto it = enriched_schemas_cache.find(original_schema);
          it != enriched_schemas_cache.end()) {
        const auto& [_, cached_schema] = *it;
        co_yield cast(std::move(slice), cached_schema);
        continue;
      }
      std::vector<type::attribute_view> attrs;
      attrs.reserve(cfg_.size());
      std::ranges::transform(cfg_, std::back_inserter(attrs),
                             [](const configuration::value_type& a) {
                               return type::attribute_view{a.first, a.second};
                             });
      auto new_schema = type{original_schema, std::move(attrs)};
      TENZIR_ASSERT(new_schema);
      co_yield cast(std::move(slice), new_schema);
      enriched_schemas_cache.emplace(std::move(original_schema),
                                     std::move(new_schema));
    }
  }

  auto name() const -> std::string override {
    return "set-attributes";
  }

  auto optimize(const expression& filter, event_order order,
                select_optimization const& selection) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order, selection);
  }

  friend auto inspect(auto& f, set_attributes_operator& x) -> bool {
    return f.object(x)
      .pretty_name("set-attributes")
      .fields(f.field("config", x.cfg_));
  }

private:
  configuration cfg_{};
};

class plugin final : public virtual operator_plugin<set_attributes_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    configuration attributes{};
    auto docs = fmt::format("https://docs.tenzir.com/operators/{}", name());
    while (not p.at_end()) {
      auto key = p.accept_shell_arg();
      if (not key) {
        diagnostic::error("failed to parse attribute flag")
          .primary(p.current_span())
          .docs(docs)
          .throw_();
      }
      if (not key->inner.starts_with("--")) {
        diagnostic::error("invalid attribute flag")
          .primary(key->source)
          .note("flag must start with `--`")
          .docs(docs)
          .throw_();
      }
      // Strip preceding `--`
      key->inner = key->inner.substr(2);
      if (auto eq_idx = key->inner.find('='); eq_idx != std::string::npos) {
        // --key=value
        attributes.emplace(key->inner.substr(0, eq_idx),
                           key->inner.substr(eq_idx + 1));
        continue;
      }
      // --key value
      auto value = p.accept_shell_arg();
      if (not value) {
        diagnostic::error("failed to parse attribute value")
          .primary(p.current_span())
          .docs(docs)
          .throw_();
      }
      if (value->inner.starts_with("--")) {
        diagnostic::error("invalid attribute value")
          .primary(value->source)
          .note("value cannot start with `--`")
          .docs(docs)
          .throw_();
      }
      attributes.emplace(std::move(key->inner), std::move(value->inner));
    }
    return std::make_unique<set_attributes_operator>(std::move(attributes));
  }
};

} // namespace

} // namespace tenzir::plugins::set_attributes

TENZIR_REGISTER_PLUGIN(tenzir::plugins::set_attributes::plugin)
