//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/argument_parser.hpp>
#include <vast/concept/parseable/string/char_class.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/detail/string_literal.hpp>
#include <vast/error.hpp>
#include <vast/location.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>

namespace vast::plugins::top_rare {

namespace {

template <detail::string_literal Name>
// This operator does nothing but provide the operator name - the actual
// functionality is currently implemented as a "summarize | sort" subpipeline.
class top_rare_operator final : public crtp_operator<top_rare_operator<Name>> {
public:
  template <operator_input_batch T>
  auto operator()(T x) const -> T {
    return x;
  }

  auto predicate_pushdown(expression const& expr) const
    -> std::optional<std::pair<expression, operator_ptr>> override {
    return std::pair{expr, std::make_unique<top_rare_operator>(*this)};
  }

  auto name() const -> std::string override {
    return std::string{Name.str()};
  }

  friend auto
  inspect([[maybe_unused]] auto&, [[maybe_unused]] top_rare_operator&) -> bool {
    return true;
  }
};

template <detail::string_literal Name, detail::string_literal SortOrder>
class top_rare_plugin final
  : public virtual operator_plugin<top_rare_operator<Name>> {
  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{
      std::string{Name.str()}, fmt::format("https://docs.tenzir.com/docs/next/"
                                           "operators/transformations/{}",
                                           Name.str())};
    auto field = located<std::string>{};
    auto count_field = std::optional<located<std::string>>{};
    parser.add(field, "<str>");
    parser.add("-c,--count-field", count_field, "<str>");
    parser.parse(p);
    if (count_field) {
      if (count_field->inner.empty()) {
        diagnostic::error("Need a string value for 'count-field' parameter")
          .primary(field.source)
          .throw_();
      }
      if (count_field->inner == field.inner) {
        diagnostic::error("Invalid duplicate field value `{}` for count and "
                          "value fields",
                          field.inner)
          .primary(field.source)
          .secondary(count_field->source)
          .throw_();
      }
    } else {
      if (field.inner == default_count_field) {
        diagnostic::error("Invalid duplicate field value `{}` for count and "
                          "value fields",
                          field.inner)
          .primary(field.source)
          .throw_();
      } else {
        count_field.emplace();
        count_field->inner = default_count_field;
      }
    }
    // TODO: Replace this textual parsing with a subpipeline to improve
    // diagnostics for this operator.
    auto repr = fmt::format("summarize "
                            "{0}=count({1}) by "
                            "{1} | sort {0} {2}",
                            count_field->inner, field.inner, SortOrder.str());
    return std::move(*pipeline::internal_parse_as_operator(repr));
  }

private:
  static constexpr auto default_count_field = "count";
};

using top_plugin = top_rare_plugin<"top", "desc">;
using rare_plugin = top_rare_plugin<"rare", "asc">;

} // namespace

} // namespace vast::plugins::top_rare

VAST_REGISTER_PLUGIN(vast::plugins::top_rare::top_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::top_rare::rare_plugin)
