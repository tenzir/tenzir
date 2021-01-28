/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/system/importer.hpp"

#include "vast/fwd.hpp"

#include "vast/atoms.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/si_literals.hpp"
#include "vast/system/report.hpp"
#include "vast/system/status_verbosity.hpp"
#include "vast/table_slice.hpp"
#include "vast/uuid.hpp"

#include <caf/attach_continuous_stream_stage.hpp>
#include <caf/config_value.hpp>
#include <caf/settings.hpp>

#include <fstream>

namespace vast::system {

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
    VAST_TRACE(VAST_ARG(slices));
    uint64_t events = 0;
    auto t = timer::start(state.measurement_);
    for (auto&& slice : std::exchange(slices, {})) {
      VAST_ASSERT(slice.rows() <= static_cast<size_t>(state.available_ids()));
      auto rows = slice.rows();
      events += rows;
      slice.offset(state.next_id(rows));
      out.push(std::move(slice));
    }
    t.stop(events);
  }

  void finalize(const caf::error& err) override {
    VAST_LOG_SPD_DEBUG("{} stopped with message: {}",
                       detail::id_or_name(state.self), render(err));
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
    VAST_LOG_SPD_INFO("{} adds {} source",
                      detail::id_or_name(driver_.state.self),
                      driver_.state.inbound_descriptions[ptr]);
    super::register_input_path(ptr);
  }

  void deregister_input_path(caf::inbound_path* ptr) noexcept override {
    VAST_LOG_SPD_INFO("{} removes {} source",
                      detail::id_or_name(driver_.state.self),
                      driver_.state.inbound_descriptions[ptr]);
    driver_.state.inbound_descriptions.erase(ptr);
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

importer_state::~importer_state() {
  write_state(write_mode::with_next);
}

caf::error importer_state::read_state() {
  auto file = dir / "current_id_block";
  if (exists(file)) {
    VAST_LOG_SPD_VERBOSE("{} reads persistent state from {}",
                         detail::id_or_name(self), file);
    std::ifstream state_file{to_string(file)};
    state_file >> current.end;
    if (!state_file)
      return caf::make_error(ec::parse_error,
                             "unable to read importer state file", file.str());
    state_file >> current.next;
    if (!state_file) {
      VAST_WARNING(self, "did not find next ID position in state file; "
                         "irregular shutdown detected");
      current.next = current.end;
    }
  } else {
    VAST_LOG_SPD_VERBOSE("{} did not find a state file at {}",
                         detail::id_or_name(self), file);
    current.end = 0;
    current.next = 0;
  }
  return get_next_block();
}

caf::error importer_state::write_state(write_mode mode) {
  if (!exists(dir)) {
    if (auto err = mkdir(dir))
      return err;
  }
  std::ofstream state_file{to_string(dir / "current_id_block")};
  state_file << current.end;
  if (mode == write_mode::with_next) {
    state_file << " " << current.next;
    VAST_LOG_SPD_VERBOSE("{} persisted next available ID at {}",
                         detail::id_or_name(self), current.next);
  } else {
    VAST_LOG_SPD_VERBOSE("{} persisted ID block boundary at {}",
                         detail::id_or_name(self), current.end);
  }
  return caf::none;
}

caf::error importer_state::get_next_block(uint64_t required) {
  using namespace si_literals;
  while (current.next + required >= current.end)
    current.end += 8_Mi;
  return write_state(write_mode::without_next);
}

id importer_state::next_id(uint64_t advance) {
  id pre = current.next;
  id post = pre + advance;
  if (post >= current.end)
    get_next_block(advance);
  current.next = post;
  VAST_ASSERT(current.next < current.end);
  return pre;
}

id importer_state::available_ids() const noexcept {
  return max_id - current.next;
}

caf::dictionary<caf::config_value>
importer_state::status(status_verbosity v) const {
  auto result = caf::settings{};
  auto& importer_status = put_dictionary(result, "importer");
  // TODO: caf::config_value can only represent signed 64 bit integers, which
  // may make it look like overflow happened in the status report. As an
  // intermediate workaround, we convert the values to strings.
  if (v >= status_verbosity::detailed) {
    caf::put(importer_status, "ids.available", to_string(available_ids()));
    caf::put(importer_status, "ids.block.next", to_string(current.next));
    caf::put(importer_status, "ids.block.end", to_string(current.end));
    auto& sources_status = put_list(importer_status, "sources");
    for (const auto& kv : inbound_descriptions)
      sources_status.emplace_back(kv.second);
  }
  // General state such as open streams.
  if (v >= status_verbosity::debug)
    detail::fill_status_map(importer_status, self);
  return result;
}

void importer_state::send_report() {
  auto now = stopwatch::now();
  if (measurement_.events > 0) {
    using namespace std::string_literals;
    auto elapsed = std::chrono::duration_cast<duration>(now - last_report);
    auto node_throughput = measurement{elapsed, measurement_.events};
    auto r = performance_report{
      {{"importer"s, measurement_}, {"node_throughput"s, node_throughput}}};
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_VERBOSE
    auto beat = [&](const auto& sample) {
      if (auto rate = sample.value.rate_per_sec(); std::isfinite(rate))
        VAST_LOG_SPD_VERBOSE(
          "{} handled {} events at a rate of {} events/sec in {}",
          detail::id_or_name(self), sample.value.events,
          static_cast<uint64_t>(rate), to_string(sample.value.duration));
      else
        VAST_LOG_SPD_VERBOSE("{} handled {} events in {}",
                             detail::id_or_name(self), sample.value.events,
                             to_string(sample.value.duration));
    };
    beat(r[1]);
#endif
    measurement_ = measurement{};
    self->send(accountant, std::move(r));
  }
  last_report = now;
}

importer_actor::behavior_type
importer(importer_actor::stateful_pointer<importer_state> self, path dir,
         const archive_actor& archive, index_actor index,
         const type_registry_actor& type_registry) {
  VAST_TRACE(VAST_ARG(dir));
  self->state.dir = std::move(dir);
  auto err = self->state.read_state();
  if (err) {
    VAST_ERROR(self, "failed to load state:", render(err));
    self->quit(std::move(err));
    return importer_actor::behavior_type::make_empty_behavior();
  }
  namespace defs = defaults::system;
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    self->state.send_report();
    self->quit(msg.reason);
  });
  self->state.stage = make_importer_stage(self);
  if (type_registry)
    self->state.stage->add_outbound_path(type_registry);
  if (archive)
    self->state.stage->add_outbound_path(archive);
  if (index) {
    self->state.index = std::move(index);
    self->state.stage->add_outbound_path(self->state.index);
  }
  for (auto& plugin : plugins::get())
    if (auto p = plugin.as<analyzer_plugin>())
      if (auto analyzer = p->make_analyzer(self->system()))
        self->state.stage->add_outbound_path(analyzer);
  return {
    // Register the ACCOUNTANT actor.
    [=](accountant_actor accountant) {
      VAST_LOG_SPD_DEBUG("{} registers accountant {}", detail::id_or_name(self),
                         accountant);
      self->state.accountant = std::move(accountant);
      self->send(self->state.accountant, atom::announce_v, self->name());
    },
    // Add a new sink.
    [=](stream_sink_actor<table_slice> sink) {
      VAST_LOG_SPD_DEBUG("{} adds a new sink: {}", detail::id_or_name(self),
                         sink);
      return self->state.stage->add_outbound_path(std::move(sink));
    },
    // Register a FLUSH LISTENER actor.
    [=](atom::subscribe, atom::flush, flush_listener_actor listener) {
      VAST_ASSERT(self->state.stage != nullptr);
      self->send(self->state.index, atom::subscribe_v, atom::flush_v,
                 std::move(listener));
    },
    // The internal telemetry loop of the IMPORTER.
    [=](atom::telemetry) {
      self->state.send_report();
      self->delayed_send(self, defs::telemetry_rate, atom::telemetry_v);
    },
    // -- stream_sink_actor<table_slice> ---------------------------------------
    [=](caf::stream<table_slice> in) {
      VAST_LOG_SPD_DEBUG("{} adds a new source: {}", detail::id_or_name(self),
                         self->current_sender());
      return self->state.stage->add_inbound_path(in);
    },
    // -- stream_sink_actor<table_slice, std::string> --------------------------
    [=](caf::stream<table_slice> in, std::string desc) {
      self->state.inbound_description = std::move(desc);
      VAST_LOG_SPD_DEBUG("{} adds a new {} source: {}",
                         detail::id_or_name(self), desc,
                         self->current_sender());
      return self->state.stage->add_inbound_path(in);
    },
    // -- status_client_actor --------------------------------------------------
    [=](atom::status, status_verbosity v) { //
      return self->state.status(v);
    },
  };
}

} // namespace vast::system
