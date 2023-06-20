//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/actors.hpp>
#include <vast/atoms.hpp>
#include <vast/catalog.hpp>
#include <vast/concept/parseable/string/char_class.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/node_control.hpp>
#include <vast/passive_partition.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/query_context.hpp>
#include <vast/table_slice.hpp>
#include <vast/uuid.hpp>

#include <arrow/type.h>
#include <caf/attach_stream_source.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/timespan.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::export_ {

class export_operator final : public crtp_operator<export_operator> {
public:
  export_operator() = default;

  explicit export_operator(expression expr) : expr_{std::move(expr)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // TODO: Some of the the requests this operator makes are blocking, so we
    // have to create a scoped actor here; once the operator API uses async we
    // can offer a better mechanism here.
    auto blocking_self = caf::scoped_actor(ctrl.self().system());
    auto components
      = get_node_components<catalog_actor, accountant_actor, filesystem_actor>(
        blocking_self, ctrl.node());
    auto [catalog, accountant, fs] = std::move(*components);
    auto current_slice = std::optional<table_slice>{};
    auto query_context
      = vast::query_context::make_extract("export", blocking_self, expr_);
    auto current_result = catalog_lookup_result{};
    auto current_error = caf::error{};
    blocking_self
      ->request(catalog, caf::infinite, atom::candidates_v, query_context)
      .receive(
        [&current_result](catalog_lookup_result result) {
          current_result = std::move(result);
        },
        [&current_error](caf::error e) {
          current_error = std::move(e);
        });
    if (current_error) {
      ctrl.abort(std::move(current_error));
      co_return;
    }
    for (const auto& [type, info] : current_result.candidate_infos) {
      for (const auto& partition_info : info.partition_infos) {
        const auto& uuid = partition_info.uuid;
        auto partition = blocking_self->spawn(
          passive_partition, uuid, accountant, fs,
          std::filesystem::path{"index"} / fmt::format("{:l}", uuid));
        auto recieving_slices = true;
        blocking_self->send(partition, atom::query_v, query_context);
        while (recieving_slices) {
          blocking_self->receive(
            [&current_slice](table_slice slice) {
              current_slice = std::move(slice);
            },
            [&recieving_slices](uint64_t) {
              recieving_slices = false;
            },
            [&recieving_slices, &current_error](caf::error e) {
              recieving_slices = false;
              current_error = std::move(e);
            });
          if (current_error) {
            ctrl.warn(std::move(current_error));
            continue;
          }
          if (current_slice) {
            co_yield *current_slice;
            current_slice.reset();
          } else {
            co_yield {};
          }
        }
      }
    }
  }

  auto name() const -> std::string override {
    return "export";
  }

  auto detached() const -> bool override {
    return true;
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
  }

  auto predicate_pushdown(expression const& expr) const
    -> std::optional<std::pair<expression, operator_ptr>> override {
    return std::pair{
      trivially_true_expression(),
      std::make_unique<export_operator>(conjunction{expr_, expr})};
  }

  friend auto inspect(auto& f, export_operator& x) -> bool {
    return f.apply(x.expr_);
  }

private:
  expression expr_;
};

class plugin final : public virtual operator_plugin<export_operator> {
public:
  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::end_of_pipeline_operator;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = optional_ws_or_comment >> end_of_pipeline_operator;
    if (!p(f, l, unused)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "export operator: '{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<export_operator>(trivially_true_expression()),
    };
  }
};

} // namespace vast::plugins::export_

VAST_REGISTER_PLUGIN(vast::plugins::export_::plugin)
