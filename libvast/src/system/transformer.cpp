//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/transformer.hpp"

#include "vast/detail/fill_status_map.hpp"
#include "vast/detail/framed.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/system/status.hpp"
#include "vast/table_slice.hpp"

#include <caf/attach_continuous_stream_stage.hpp>
#include <caf/attach_stream_sink.hpp>
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
    [self](caf::unit_t&, caf::downstream<table_slice>& out,
           detail::framed<table_slice> x) {
      if (x.header == detail::stream_control_header::eof) {
        VAST_DEBUG("{} quits after receiving EOF control message in stream",
                   *self);
        self->send_exit(self, caf::make_error(ec::end_of_input));
        return;
      }
      auto transformed = self->state.transforms.apply(std::move(x.body));
      if (!transformed) {
        VAST_ERROR("discarding data: error in transformation step. {}",
                   transformed.error());
        return;
      }
      out.push(std::move(*transformed));
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
  auto transform_names = list{};
  for (const auto& t : transforms)
    transform_names.emplace_back(t.name());
  self->state.status["transforms"] = std::move(transform_names);
  self->state.transforms = transformation_engine{std::move(transforms)};
  self->state.stage = attach_transform_stage(self);
  return {
    [self](const stream_sink_actor<table_slice>& out)
      -> caf::outbound_stream_slot<table_slice> {
      VAST_DEBUG("{} adds stream sink {}", self->state.transformer_name, out);
      return self->state.stage->add_outbound_path(out);
    },
    [self](const stream_sink_actor<table_slice, std::string>& sink,
           std::string name) {
      VAST_DEBUG("{} adds stream sink {} (registers as {})",
                 self->state.transformer_name, sink, name);
      self->state.stage->add_outbound_path(sink,
                                           std::make_tuple(std::move(name)));
    },
    [self](caf::stream<detail::framed<table_slice>> in)
      -> caf::inbound_stream_slot<detail::framed<table_slice>> {
      VAST_DEBUG("{} got a new stream source", self->state.transformer_name);
      return self->state.stage->add_inbound_path(in);
    },
    [self](atom::status, status_verbosity v) {
      auto result = self->state.status;
      // General state such as open streams.
      if (v >= status_verbosity::debug)
        detail::fill_status_map(result, self);
      return result;
    }};
}

stream_sink_actor<table_slice>::behavior_type
dummy_transformer_sink(stream_sink_actor<table_slice>::pointer self) {
  return {
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      auto result = caf::attach_stream_sink(
        self, in,
        [=](size_t& num_discarded) {
          num_discarded = 0;
        },
        [=](size_t& num_discarded, const std::vector<table_slice>& slices) {
          for (const auto& slice : slices)
            num_discarded += slice.rows();
        },
        [=](size_t& num_discarded, const caf::error& err) {
          if (num_discarded > 0)
            VAST_WARN("transformer discarded {} undeliverable events",
                      num_discarded);
          self->quit(err);
        });
      return result.inbound_slot();
    },
  };
}

} // namespace vast::system
