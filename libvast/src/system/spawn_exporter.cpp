//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_exporter.hpp"

#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/query_options.hpp"
#include "vast/system/exporter.hpp"
#include "vast/system/make_pipelines.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"
#include "vast/system/transformer.hpp"

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/send.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

namespace {

caf::expected<std::pair<expression, std::optional<pipeline>>>
parse_arguments(const std::vector<std::string>& args) {
  static const auto match_everything = expression{predicate{
    meta_extractor{meta_extractor::kind::type},
    relational_operator::not_equal,
    data{"this expression matches everything"},
  }};
  if (args.empty()) {
    return std::pair{
      match_everything,
      std::optional<pipeline>{},
    };
  }
  const auto repr = detail::join(args.begin(), args.end(), " ");
  using parsers::blank, parsers::expr, parsers::eoi;
  auto f = repr.begin();
  const auto l = repr.end();
  auto parsed_expr = expression{};
  const auto optional_ws = ignore(*blank);
  bool has_expr = true;
  if (!expr(f, l, parsed_expr)) {
    VAST_DEBUG("failed to parse expr from '{}'", repr);
    parsed_expr = match_everything;
    has_expr = false;
  }
  VAST_DEBUG("parsed expr = {}", parsed_expr);
  // <expr> | <pipeline>
  //       ^ we start here
  const auto has_no_pipeline_parser = optional_ws >> eoi;
  if (has_no_pipeline_parser(f, l, unused)) {
    return std::pair{
      std::move(parsed_expr),
      std::optional<pipeline>{},
    };
  }
  if (has_expr) {
    const auto has_pipeline_parser = optional_ws >> '|';
    if (!has_pipeline_parser(f, l, unused)) {
      return caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                           "pipeline in query "
                                                           "'{}': missing pipe",
                                                           repr));
    }
  }
  const auto pipeline_repr = std::string_view{f, l};
  auto parsed_pipeline = pipeline::parse("export", pipeline_repr);
  if (!parsed_pipeline) {
    return caf::make_error(ec::syntax_error,
                           fmt::format("failed to parse pipeline in query "
                                       "'{}': {}",
                                       repr, parsed_pipeline.error()));
  }
  VAST_DEBUG("parsed pipeline = {}", pipeline_repr);
  return std::pair{
    std::move(parsed_expr),
    std::move(*parsed_pipeline),
  };
}

} // namespace

caf::expected<caf::actor>
spawn_exporter(node_actor::stateful_pointer<node_state> self,
               spawn_arguments& args) {
  VAST_TRACE_SCOPE("{}", VAST_ARG(args));
  // Pipelines from configuration.
  auto pipelines
    = make_pipelines(pipelines_location::server_export, args.inv.options);
  if (!pipelines)
    return pipelines.error();
  // Parse given expression.
  auto parse_result = parse_arguments(args.inv.arguments);
  if (!parse_result)
    return parse_result.error();
  auto [expr, pipeline] = std::move(*parse_result);
  if (pipeline)
    pipelines->push_back(std::move(*pipeline));
  // Parse query options.
  auto query_opts = no_query_options;
  if (get_or(args.inv.options, "vast.export.continuous", false))
    query_opts = query_opts + continuous;
  if (get_or(args.inv.options, "vast.export.unified", false))
    query_opts = unified;
  // Default to historical if no options provided.
  if (query_opts == no_query_options)
    query_opts = historical;
  // Mark the query as low priority if explicitly requested.
  if (get_or(args.inv.options, "vast.export.low-priority", false))
    query_opts = query_opts + low_priority;
  auto max_events = get_or(args.inv.options, "vast.export.max-events",
                           defaults::export_::max_events);
  auto handle = self->spawn(exporter, expr, query_opts, max_events,
                            std::move(*pipelines));
  VAST_VERBOSE("{} spawned an exporter for {}", *self, to_string(expr));
  // Wire the exporter to all components.
  auto [accountant, importer, index]
    = self->state.registry.find<accountant_actor, importer_actor, index_actor>();
  if (accountant)
    self->send(handle, atom::set_v, accountant);
  if (importer && has_continuous_option(query_opts))
    self
      ->request(importer, caf::infinite,
                static_cast<stream_sink_actor<table_slice>>(handle))
      .then(
        [=](caf::outbound_stream_slot<table_slice>) {
          // nop
        },
        [=, importer = importer](caf::error err) {
          VAST_ERROR("{} failed to connect to importer {}: {}", *self, importer,
                     err);
        });
  if (index) {
    VAST_DEBUG("{} connects index to new exporter", *self);
    self->send(handle, atom::set_v, index);
  }
  // Setting max-events to 0 means infinite.
  if (max_events > 0)
    self->send(handle, atom::extract_v, static_cast<uint64_t>(max_events));
  else
    self->send(handle, atom::extract_v);
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
