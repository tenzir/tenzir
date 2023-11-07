//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/cast.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/set_attributes_operator_helper.hpp>
#include <tenzir/pipeline.hpp>

namespace tenzir::detail {

auto set_attributes_operator_helper::configuration::get_attributes() const
  -> generator<type::attribute_view> {
  for (const auto& attr : attributes_)
    co_yield {attr.first, attr.second};
}

auto set_attributes_operator_helper::configuration::to_string() const
  -> std::string {
  const auto format_attr = [](const auto& attr) {
    if (attr.second.find_first_of(" \n\r\t\v\f") != std::string::npos)
      // Add quotes if `value` has whitespace
      return fmt::format("{}=\"{}\"", attr.first, attr.second);
    return fmt::format("{}={}", attr.first, attr.second);
  };
  return fmt::to_string(
    fmt::join(attributes_ | std::views::transform(format_attr), " "));
}

auto set_attributes_operator_helper::parse(std::string_view pipeline,
                                           parse_verify verify)
  -> std::pair<std::string_view, caf::error> {
  using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
    parsers::optional_ws_or_comment, parsers::alnum, parsers::chr,
    parsers::qqstr;
  const auto* f = pipeline.begin();
  const auto* const l = pipeline.end();
  const auto name_char = (alnum | chr{'-'} | chr{'_'});
  const auto field_name_parser = +name_char;
  const auto object_name_parser
    = (field_name_parser % '.').then([](std::vector<std::string> in) {
        return fmt::to_string(fmt::join(in.begin(), in.end(), "."));
      });
  // Arguments are in the format of key=value,
  // where value can also be a "quoted string with spaces"
  const auto arguments_parser
    = (field_name_parser >> optional_ws_or_comment >> '='
       >> optional_ws_or_comment >> (qqstr | object_name_parser))
      % required_ws_or_comment;
  const auto p = required_ws_or_comment >> ~arguments_parser
                 >> optional_ws_or_comment >> end_of_pipeline_operator;
  std::vector<std::pair<std::string, std::string>> parsed_arguments;
  if (!p(f, l, parsed_arguments))
    return {std::string_view{f, l},
            caf::make_error(ec::syntax_error,
                            fmt::format("failed to parse operator arguments: "
                                        "'{}'",
                                        pipeline))};
  cfg_.attributes_.reserve(cfg_.attributes_.size() + parsed_arguments.size());
  cfg_.attributes_.insert(cfg_.attributes_.end(),
                          std::make_move_iterator(parsed_arguments.begin()),
                          std::make_move_iterator(parsed_arguments.end()));
  if (auto err = verify(cfg_); err)
    return {std::string_view{f, l}, err};
  return {std::string_view{f, l}, caf::error{}};
}

auto set_attributes_operator_helper::process(table_slice&& slice,
                                             process_verify verify) const
  -> std::pair<table_slice, caf::error> {
  if (slice.rows() == 0) {
    return {};
  }
  auto schema = slice.schema();
  if (auto e = verify(schema, cfg_))
    return {{}, e};
  schema
    = type{schema, collect(cfg_.get_attributes(), cfg_.count_attributes())};
  TENZIR_ASSERT(schema);
  return {cast(std::move(slice), schema), {}};
}

} // namespace tenzir::detail
