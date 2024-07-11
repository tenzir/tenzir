//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/importer.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/atoms.hpp"
#include "tenzir/concept/printable/numeric/integral.hpp"
#include "tenzir/concept/printable/std/chrono.hpp"
#include "tenzir/concept/printable/tenzir/error.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/fill_status_map.hpp"
#include "tenzir/detail/shutdown_stream_stage.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/status.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/uuid.hpp"

#include <caf/config_value.hpp>
#include <caf/detail/stream_stage_impl.hpp>
#include <caf/settings.hpp>
#include <caf/stream_stage_driver.hpp>

#include <filesystem>
#include <fstream>

namespace tenzir {

namespace {

class driver : public caf::stream_stage_driver<
                 table_slice, caf::broadcast_downstream_manager<table_slice>> {
public:
  driver(caf::broadcast_downstream_manager<table_slice>& out,
         importer_state& state)
    : stream_stage_driver(out), state{state} {
    // nop
  }

  void process(caf::downstream<table_slice>& out,
               std::vector<table_slice>& slices) override {
    TENZIR_TRACE_SCOPE("{}", TENZIR_ARG(slices));
    auto now = time::clock::now();
    for (auto& slice : slices) {
      slice.import_time(now);
      state.on_process(slice);
      out.push(std::move(slice));
    }
  }

  void finalize(const caf::error& err) override {
    TENZIR_DEBUG("{} stopped with message: {}", *state.self, render(err));
  }

  importer_state& state;
};

class stream_stage : public caf::detail::stream_stage_impl<driver> {
public:
  /// Constructs the import stream stage.
  /// @note This must explictly initialize the stream_manager because it does
  /// not provide a default constructor, and for reason unbeknownst to me the
  /// forwaring in the stream_stage_impl does not suffice.
  stream_stage(importer_actor::stateful_pointer<importer_state> self)
    : stream_manager(self), stream_stage_impl(self, self->state) {
    // nop
  }

  void register_input_path(caf::inbound_path* ptr) override {
    driver_.state.inbound_descriptions[ptr]
      = std::exchange(driver_.state.inbound_description, "anonymous");
    TENZIR_INFO("{} adds {} source", *driver_.state.self,
                driver_.state.inbound_descriptions[ptr]);
    super::register_input_path(ptr);
  }

  void deregister_input_path(caf::inbound_path* ptr) noexcept override {
    if ((flags_ & (is_stopped_flag | is_shutting_down_flag)) == 0) {
      TENZIR_INFO("{} removes {} source", *driver_.state.self,
                  driver_.state.inbound_descriptions[ptr]);
      driver_.state.inbound_descriptions.erase(ptr);
    }
    super::deregister_input_path(ptr);
  }
};

caf::intrusive_ptr<stream_stage>
make_importer_stage(importer_actor::stateful_pointer<importer_state> self) {
  auto result = caf::make_counted<stream_stage>(self);
  result->continuous(true);
  return result;
}

} // namespace

importer_state::importer_state(importer_actor::pointer self) : self{self} {
  // nop
}

importer_state::~importer_state() = default;

caf::typed_response_promise<record>
importer_state::status(status_verbosity v) const {
  auto rs = make_status_request_state(self);
  // Gather general importer status.
  // TODO: caf::config_value can only represent signed 64 bit integers, which
  // intermediate workaround, we convert the values to strings.
  record result;
  if (v >= status_verbosity::detailed) {
    auto sources_status = list{};
    sources_status.reserve(inbound_descriptions.size());
    for (const auto& kv : inbound_descriptions) {
      sources_status.emplace_back(kv.second);
    }
    rs->content["sources"] = std::move(sources_status);
  }
  // General state such as open streams.
  if (v >= status_verbosity::debug) {
    detail::fill_status_map(rs->content, self);
  }
  return rs->promise;
}

void importer_state::on_process(const table_slice& slice) {
  TENZIR_ASSERT(slice.rows() > 0);
  auto t = timer::start(measurement_);
  auto rows = slice.rows();
  auto name = slice.schema().name();
  if (auto it = schema_counters.find(name); it != schema_counters.end()) {
    it.value() += rows;
  } else {
    schema_counters.emplace(std::string{name}, rows);
  }
  for (const auto& [subscriber, internal] : subscribers) {
    if (slice.schema().attribute("internal").has_value() == internal) {
      self->send(subscriber, slice);
    }
  }
  t.stop(rows);
}

importer_actor::behavior_type
importer(importer_actor::stateful_pointer<importer_state> self,
         const std::filesystem::path& dir, index_actor index) {
  TENZIR_TRACE_SCOPE("importer {} {}", TENZIR_ARG(self->id()), TENZIR_ARG(dir));
  if (auto ec = std::error_code{};
      std::filesystem::exists(dir / "current_id_block", ec)) {
    std::filesystem::remove(dir / "current_id_block", ec);
  }
  namespace defs = defaults;
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    for (auto* inbound : self->state.stage->inbound_paths()) {
      self->send_exit(inbound->hdl, msg.reason);
    }
    self->quit(msg.reason);
  });
  self->state.stage = make_importer_stage(self);
  if (index) {
    self->state.index = std::move(index);
    self->state.stage->add_outbound_path(self->state.index);
  }
  self->set_down_handler([self](const caf::down_msg& msg) {
    const auto subscriber
      = std::remove_if(self->state.subscribers.begin(),
                       self->state.subscribers.end(),
                       [&](const auto& subscriber) {
                         return subscriber.first.address() == msg.source;
                       });
    self->state.subscribers.erase(subscriber, self->state.subscribers.end());
  });
  return {
    // Add a new sink.
    [self](stream_sink_actor<table_slice> sink) -> caf::result<void> {
      TENZIR_DEBUG("{} adds a new sink", *self);
      self->state.stage->add_outbound_path(sink);
      return {};
    },
    // Register a FLUSH LISTENER actor.
    [self](atom::subscribe, atom::flush, flush_listener_actor listener) {
      TENZIR_DEBUG("{} adds new subscriber {}", *self, listener);
      TENZIR_ASSERT(self->state.stage != nullptr);
      self->send(self->state.index, atom::subscribe_v, atom::flush_v,
                 std::move(listener));
    },
    [self](atom::subscribe, receiver_actor<table_slice>& subscriber,
           bool internal) {
      self->monitor(subscriber);
      self->state.subscribers.emplace_back(std::move(subscriber), internal);
    },
    // Push buffered slices downstream to make the data available.
    [self](atom::flush) -> caf::result<void> {
      auto rp = self->make_response_promise<void>();
      self->state.stage->out().fan_out_flush();
      self->state.stage->out().force_emit_batches();
      // The stream flushing only takes effect after we've returned to the
      // scheduler, so we delegate to the index only after doing that with an
      // immediately scheduled action.
      detail::weak_run_delayed(self, duration::zero(), [self, rp]() mutable {
        rp.delegate(self->state.index, atom::flush_v);
      });
      return rp;
    },
    [self](table_slice& slice) -> caf::result<void> {
      slice.import_time(time::clock::now());
      self->state.on_process(slice);
      self->state.stage->out().push(std::move(slice));
      return {};
    },
    // -- stream_sink_actor<table_slice> ---------------------------------------
    [self](caf::stream<table_slice> in) {
      // NOTE: Architecturally it would make more sense to put the transformer
      // stage *before* the import actor, but that is not possible: The message
      // sent is originally sent from the other side is a `caf::open_stream_msg`.
      // This contains a field `msg` with a `caf::stream<>`. The caf streaming
      // system recognizes this message and only passes the `caf::stream<>` to
      // the handler. This means we can not delegate() this message, since we
      // would only create a new message containing a `caf::stream` object but
      // lose the surrounding `open_stream_msg` which contains the important
      // parts. Sadly, the current actor is already stored as the "other side"
      // of the stream in the outbound path, so we can't even hack around this
      // with `caf::unsafe_send_as()` or similar black magic.
      TENZIR_DEBUG("{} adds a new source", *self);
      return self->state.stage->add_inbound_path(in);
    },
    // -- stream_sink_actor<table_slice, std::string> --------------------------
    [self](caf::stream<table_slice> in, std::string desc) {
      self->state.inbound_description = std::move(desc);
      TENZIR_DEBUG("{} adds a new {} source", *self, desc);
      return self->state.stage->add_inbound_path(in);
    },
    // -- status_client_actor --------------------------------------------------
    [self](atom::status, status_verbosity v, duration) { //
      return self->state.status(v);
    },
  };
}

} // namespace tenzir
