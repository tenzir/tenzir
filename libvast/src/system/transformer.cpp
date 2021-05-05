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
#include <caf/settings.hpp>

namespace vast::system {

transformer_stream_stage_ptr
make_transform_stage(stream_sink_actor<table_slice>::pointer self,
                     std::vector<transform>&& transforms) {
  transformation_engine transformer{std::move(transforms)};
  return caf::attach_continuous_stream_stage(
    self,
    [](caf::unit_t&) {
      // nop
    },
    [transformer = std::move(transformer)](
      caf::unit_t&, caf::downstream<table_slice>& out, table_slice x) {
      auto transformed = transformer.apply(std::move(x));
      if (!transformed) {
        VAST_ERROR("discarding data: error in transformation step. {}",
                   transformed.error());
        return;
      }
      out.push(std::move(*transformed));
    },
    [](caf::unit_t&, const caf::error&) {
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
  self->state.stage = make_transform_stage(
    caf::actor_cast<stream_sink_actor<table_slice>::pointer>(self),
    std::move(transforms));
  return {
    [self](const stream_sink_actor<table_slice>& out)
      -> caf::outbound_stream_slot<table_slice> {
      VAST_DEBUG("{} adds stream sink {}", self->state.transformer_name, out);
      return self->state.stage->add_outbound_path(out);
    },
    [self](const stream_sink_actor<table_slice, std::string>& sink,
           std::string name) {
      VAST_DEBUG("{} adding named stream sink {}", self->state.transformer_name,
                 name);
      self->state.stage->add_outbound_path(sink,
                                           std::make_tuple(std::move(name)));
    },
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      VAST_DEBUG("{} got a new stream source", self->state.transformer_name);
      return self->state.stage->add_inbound_path(in);
    },
    [self](atom::status, status_verbosity) { return self->state.status; }};
}

} // namespace vast::system