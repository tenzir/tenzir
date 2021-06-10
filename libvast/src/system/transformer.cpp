//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/transformer.hpp"

#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice.hpp"

#include <caf/attach_continuous_stream_stage.hpp>
#include <caf/attach_stream_stage.hpp>
#include <caf/settings.hpp>

namespace vast::system {

transformer_stream_stage_ptr attach_transform_stage(
  transformer_actor::stateful_pointer<transformer_state> self) {
  return caf::attach_continuous_stream_stage(
    self,
    [](caf::unit_t&) {
      // nop
    },
    [self](caf::unit_t&, caf::downstream<stream_controlled<table_slice>>& out,
           stream_controlled<table_slice> x) {
      if (!caf::holds_alternative<table_slice>(x)) {
        self->send_exit(self, caf::make_error(ec::end_of_input));
        return;
      }
      auto& slice = caf::get<table_slice>(x);
      VAST_ASSERT(slice.encoding() != table_slice_encoding::none);
      auto transformed = self->state.transforms.apply(std::move(slice));
      if (!transformed) {
        VAST_ERROR("discarding data: error in transformation "
                   "step. {}",
                   transformed.error());
        return;
      }
      // Re-attach the flush listeners, if any.
      VAST_ASSERT(transformed->encoding() != table_slice_encoding::none);
      x = std::move(*transformed);
      out.push(std::move(x));
    },
    [=](caf::unit_t&, const caf::error&) {
      // nop
    });
}

transformer_actor::behavior_type
transformer(transformer_actor::stateful_pointer<transformer_state> self,
            std::string name, std::vector<transform>&& transforms) {
  VAST_TRACE_SCOPE("{}", VAST_ARG(name));
  self->state.transformer_name = std::move(name);
  auto& status
    = put_dictionary(self->state.status, self->state.transformer_name);
  auto& transform_names = put_list(status, "transforms");
  for (const auto& t : transforms)
    transform_names.emplace_back(t.name());
  self->state.transforms = transformation_engine{std::move(transforms)};
  self->state.stage = attach_transform_stage(self);
  return {
    [self](const stream_sink_actor<stream_controlled<table_slice>>& out)
      -> caf::outbound_stream_slot<stream_controlled<table_slice>> {
      VAST_DEBUG("{} adds stream sink {}", self->state.transformer_name, out);
      return self->state.stage->add_outbound_path(out);
    },
    [self](
      const stream_sink_actor<stream_controlled<table_slice>, std::string>& sink,
      std::string name) {
      VAST_DEBUG("{} adds stream sink {} (registers as {})",
                 self->state.transformer_name, sink, name);
      self->state.stage->add_outbound_path(sink,
                                           std::make_tuple(std::move(name)));
    },
    [self](caf::stream<stream_controlled<table_slice>> in)
      -> caf::inbound_stream_slot<stream_controlled<table_slice>> {
      VAST_DEBUG("{} got a new stream source", self->state.transformer_name);
      return self->state.stage->add_inbound_path(in);
    },
    [self](atom::status, status_verbosity) {
      return self->state.status;
    }};
}

} // namespace vast::system
