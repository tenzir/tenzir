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

transformer_stream_stage_ptr attach_pipeline_stage(
  transformer_actor::stateful_pointer<transformer_state> self) {
  return caf::attach_continuous_stream_stage(
    self,
    [](caf::unit_t&) {
      // nop
    },
    [self](caf::unit_t&, caf::downstream<table_slice>& out, table_slice x) {
      /*if (x.header == detail::stream_control_header::eof) {
        VAST_DEBUG("{} quits after receiving EOF control message in stream",
                   self->state.transformer_name);
        self->send_exit(self, caf::make_error(ec::end_of_input));
        return;
      }
      auto offset = x.body.offset();
      auto rows = x.body.rows();
      if (auto err = self->state.executor.add(std::move(x.body))) {
        VAST_WARN("{} skips slice because add failed: {}",
                  self->state.transformer_name, err);
        return;
      }
      auto transformed = self->state.executor.finish();
      if (!transformed) {
        VAST_WARN("{} skips slice because of an error in pipeline: {}",
                  self->state.transformer_name, transformed.error());
        return;
      }
      if (self->state.reassign_offset_ranges) {
        auto next_offset = offset;
        for (auto& t1 : *transformed) {
          t1.offset(next_offset);
          next_offset += t1.rows();
        }
        if (next_offset > offset + rows) {
          VAST_WARN(caf::make_error(ec::invalid_result,
                                    "pipeline returned a slice "
                                    "with an offset outside the "));
          return;
        }
      }
      for (auto& t2 : *transformed)
        out.push(std::move(t2));*/
    },
    [=](caf::unit_t&, const caf::error&) {
      // nop
    });
}

transformer_actor::behavior_type
transformer(transformer_actor::stateful_pointer<transformer_state> self,
            std::string name, std::vector<pipeline>&& pipelines) {
  VAST_TRACE_SCOPE("transformer {} {}", VAST_ARG(self->id()), VAST_ARG(name));
  self->state.transformer_name = std::move(name);
  auto pipeline_names = list{};
  for (const auto& t : pipelines)
    pipeline_names.emplace_back(t.name());
  self->state.source_requires_shutdown = false;
  self->state.status["pipelines"] = std::move(pipeline_names);
  self->state.executor = pipeline_executor{std::move(pipelines)};
  if (auto err = self->state.executor.validate(
        pipeline_executor::allow_aggregate_pipelines::no)) {
    VAST_ERROR("transformer is not allowed to use aggregate pipeline {}", err);
    self->quit();
    return transformer_actor::behavior_type::make_empty_behavior();
  }
  self->state.stage = attach_pipeline_stage(self);
  return {
    [self](const stream_sink_actor<table_slice>& out)
      -> caf::outbound_stream_slot<table_slice> {
      if (out->getf(caf::abstract_actor::is_shutting_down_flag)) {
        VAST_DEBUG("{} ignores stream sink {} because the actor is already "
                   "shutting down",
                   self->state.transformer_name, out);
        return {};
      }
      VAST_DEBUG("{} adds stream sink {}", self->state.transformer_name, out);
      return self->state.stage->add_outbound_path(out);
    },
    [self](const stream_sink_actor<table_slice, std::string>& sink,
           std::string name) {
      if (sink->getf(caf::abstract_actor::is_shutting_down_flag)) {
        VAST_DEBUG("{} ignores stream sink {} because the actor is already "
                   "shutting down",
                   self->state.transformer_name, sink);
        return;
      }
      VAST_DEBUG("{} adds stream sink {} (registers as {})",
                 self->state.transformer_name, sink, name);
      self->state.stage->add_outbound_path(sink,
                                           std::make_tuple(std::move(name)));
    },
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      // There's a race condition (mostly when using `vast -N`) that prevents
      // shutdown when the importer shuts down before the stream handshake
      // finishes. In this case the `eof` it sends never arrives (because we
      // call `push()` manually and there are no paths at that time) and thus
      // the transformer never quits, so we have to exit manually here. We also
      // have to open a stream slot and `shutdown()` the stream again, or the
      // importer will be stuck with a pending stream manager and not be
      // properly cleaned up, also causing a deadlock on shutdown. On the other
      // hand, some sources will shut down independently of the system shutdown,
      // e.g. when importing from a file. In this case we may have the opposite
      // race, where there is still data that the stream manager needs to push
      // onto the stream, and we must not shutdown in this case since that would
      // cause the removal of the path upstream. Since we cannot possibly tell
      // from here which behavior is correct, we require the party that spawns
      // the transformer to decide what they require.
      VAST_DEBUG("{} got a new stream source", self->state.transformer_name);
      auto slot = self->state.stage->add_inbound_path(in);
      auto sender = self->current_sender();
      if (self->state.source_requires_shutdown
          && sender->get()->getf(caf::abstract_actor::is_shutting_down_flag)) {
        VAST_DEBUG("{} shuts down after receiving dead source",
                   self->state.transformer_name);
        self->state.stage->shutdown();
        self->send_exit(self, caf::make_error(ec::end_of_input));
      }
      return slot;
    },
    [self](atom::status, status_verbosity v) {
      auto result = self->state.status;
      // General state such as open streams.
      if (v >= status_verbosity::debug)
        detail::fill_status_map(result, self);
      return result;
    }};
}

transformer_actor::behavior_type
importer_transformer(transformer_actor::stateful_pointer<transformer_state> self,
                     std::string name, std::vector<pipeline>&& pipelines) {
  auto handle = transformer(self, std::move(name), std::move(pipelines));
  self->state.source_requires_shutdown = true;
  self->state.reassign_offset_ranges = true;
  return handle;
}

stream_sink_actor<table_slice>::behavior_type
dummy_transformer_sink(stream_sink_actor<table_slice>::pointer self) {
  VAST_TRACE_SCOPE("dummy transformer sink {}", VAST_ARG(self->id()));
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
