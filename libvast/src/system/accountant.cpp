//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/accountant.hpp"

#include "vast/accountant/config.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/coding.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/detail/posix.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/system/report.hpp"
#include "vast/system/status.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/time.hpp"
#include "vast/view.hpp"

#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <chrono>
#include <cmath>
#include <filesystem>
#include <ios>
#include <limits>
#include <queue>
#include <string>

namespace vast::system {

namespace {

constexpr std::chrono::seconds overview_delay(3);

} // namespace

struct accountant_state_impl {
  // -- member types -----------------------------------------------------------

  using downstream_manager = caf::broadcast_downstream_manager<table_slice>;

  // -- constructor, destructors, and assignment operators ---------------------

  accountant_state_impl(accountant_actor::pointer self, accountant_config cfg,
                        std::filesystem::path root)
    : self{self}, root{std::move(root)} {
    apply_config(std::move(cfg));
  }

  // -- member variables -------------------------------------------------------

  /// Stores the parent actor handle.
  accountant_actor::pointer self;

  /// The root path of the database.
  std::filesystem::path root;

  /// Stores the names of known actors to fill into the actor_name column.
  std::unordered_map<caf::actor_id, std::string> actor_map;

  /// Accumulates the importer throughput until the next heartbeat.
  measurement accumulator;

  /// Stores the builder instance.
  table_slice_builder_ptr builder;

  /// Buffers table_slices, acting as a adaptor between the push based
  /// ACCOUNTANT interface and the pull based stream to the IMPORTER.
  std::queue<table_slice> slice_buffer;

  /// Takes care of transmitting batches.
  caf::stream_source_ptr<downstream_manager> mgr;

  /// Handle to the file output channel.
  std::unique_ptr<std::ostream> file_sink = nullptr;

  /// Handle to the uds output channel.
  std::unique_ptr<std::ostream> uds_sink = nullptr;

  /// Handle to the uds output channel.
  std::unique_ptr<detail::uds_datagram_sender> uds_datagram_sink = nullptr;

  /// The configuration.
  accountant_config cfg;

  // -- utility functions ------------------------------------------------------

  void finish_slice() {
    // Do nothing if builder has not been created or no rows have been added yet.
    if (!builder || builder->rows() == 0)
      return;
    auto slice = builder->finish();
    VAST_DEBUG("{} generated slice with {} rows", *self, slice.rows());

    slice_buffer.push(std::move(slice));
    mgr->advance();
  }

  void record_internally(const std::string& key, real x, time ts) {
    // This is a workaround to a bug that is somewhere else -- the index cannot
    // handle NaN, and a bug that we were unable to reproduce reliably caused
    // the accountant to forward NaN to the index here.
    if (!std::isfinite(x)) {
      VAST_DEBUG("{} cannot record a non-finite metric", *self);
      return;
    }
    auto actor_id = self->current_sender()->id();
    if (!builder) {
      auto layout = legacy_record_type{
      {"ts", legacy_time_type{}.name("timestamp")},
      {"actor", legacy_string_type{}},
      {"key", legacy_string_type{}},
      {"value", legacy_real_type{}},
    }.name("vast.metrics");
      builder
        = factory<table_slice_builder>::make(cfg.self_sink.slice_type, layout);
      VAST_DEBUG("{} obtained a table slice builder", *self);
    }
    VAST_ASSERT(builder->add(ts, actor_map[actor_id], key, x));
    if (builder->rows() == static_cast<size_t>(cfg.self_sink.slice_size))
      finish_slice();
  }

  std::vector<char> to_json_line(time ts, const std::string& key, real x) {
    using namespace std::string_view_literals;
    auto actor_id = self->current_sender()->id();
    json_printer<policy::oneline, policy::human_readable_durations> printer;
    std::vector<char> buf;
    auto iter = std::back_inserter(buf);
    *iter++ = '{';
    printer.print(iter, std::pair{"ts"sv, make_data_view(ts)});
    *iter++ = ',';
    printer.print(iter,
                  std::pair{"actor"sv, make_data_view(actor_map[actor_id])});
    *iter++ = ',';
    printer.print(iter, std::pair{"key"sv, make_data_view(key)});
    *iter++ = ',';
    printer.print(iter, std::pair{"value"sv, make_data_view(x)});
    *iter++ = '}';
    *iter++ = '\n';
    return buf;
  }

  std::ostream& record_to_output(const std::string& key, real x, time ts,
                                 std::ostream& os, bool real_time) {
    auto buf = to_json_line(ts, key, x);
    os.write(buf.data(), buf.size());
    if (real_time)
      os << std::flush;
    return os;
  }

  void record_to_unix_datagram(const std::string& key, real x, time ts,
                               detail::uds_datagram_sender& dest) {
    auto buf = to_json_line(ts, key, x);
    dest.send(buf);
  }

  void record(const std::string& key, real x,
              time ts = std::chrono::system_clock::now()) {
    if (cfg.self_sink.enable)
      record_internally(key, x, ts);
    if (file_sink)
      record_to_output(key, x, ts, *file_sink, cfg.file_sink.real_time);
    if (uds_sink)
      record_to_output(key, x, ts, *uds_sink, cfg.uds_sink.real_time);
    if (uds_datagram_sink)
      record_to_unix_datagram(key, x, ts, *uds_datagram_sink);
  }

  void record(const std::string& key, duration x,
              time ts = std::chrono::system_clock::now()) {
    auto ms = std::chrono::duration<double, std::milli>{x}.count();
    record(key, ms, std::move(ts));
  }

  void record(const std::string& key, time x,
              time ts = std::chrono::system_clock::now()) {
    record(key, x.time_since_epoch(), ts);
  }

  void command_line_heartbeat() {
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_DEBUG
    if (accumulator.events > 0)
      if (auto rate = accumulator.rate_per_sec(); std::isfinite(rate))
        VAST_DEBUG("{} received {} events at a rate of {} events/sec", *self,
                   accumulator.events, static_cast<uint64_t>(rate));
#endif
    accumulator = {};
  }

  void apply_config(accountant_config cfg) {
    auto& old = this->cfg;
    // Act on file sink config.
    bool start_file_sink = cfg.file_sink.enable && !old.file_sink.enable;
    bool stop_file_sink = !cfg.file_sink.enable && old.file_sink.enable;
    if (stop_file_sink) {
      VAST_INFO("{} closing metrics output file {}", *self, old.file_sink.path);
      file_sink.reset(nullptr);
    }
    if (start_file_sink) {
      auto s = detail::make_output_stream(root / cfg.file_sink.path,
                                          std::filesystem::file_type::regular);
      if (s) {
        VAST_INFO("{} writing metrics to {}", *self, cfg.file_sink.path);
        file_sink = std::move(*s);
      } else {
        VAST_INFO("{} could not open {} for metrics: {}", *self,
                  cfg.file_sink.path, s.error());
      }
    }
    // Act on uds sink config.
    bool start_uds_sink = cfg.uds_sink.enable && !old.uds_sink.enable;
    bool stop_uds_sink = !cfg.uds_sink.enable && old.uds_sink.enable;
    if (stop_uds_sink) {
      VAST_INFO("{} closing metrics output socket {}", *self,
                old.uds_sink.path);
      uds_sink.reset(nullptr);
    }
    if (start_uds_sink) {
      if (cfg.uds_sink.type == detail::socket_type::datagram) {
        auto s = detail::uds_datagram_sender::make(root / cfg.uds_sink.path);
        if (s) {
          VAST_INFO("{} writes metrics to {}", *self, cfg.uds_sink.path);
          uds_datagram_sink
            = std::make_unique<detail::uds_datagram_sender>(std::move(*s));
        } else {
          VAST_INFO("{} could not open {} for metrics: {}", *self,
                    cfg.uds_sink.path, s.error());
        }
      } else {
        auto s = detail::make_output_stream(root / cfg.uds_sink.path,
                                            cfg.uds_sink.type);
        if (s) {
          VAST_INFO("{} writes metrics to {}", *self, cfg.uds_sink.path);
          uds_sink = std::move(*s);
        } else {
          VAST_INFO("{} could not open {} for metrics: {}", *self,
                    cfg.uds_sink.path, s.error());
        }
      }
    }
    this->cfg = std::move(cfg);
  }
};

accountant_actor::behavior_type
accountant(accountant_actor::stateful_pointer<accountant_state> self,
           accountant_config cfg, std::filesystem::path root) {
  self->state.reset(
    new accountant_state_impl{self, std::move(cfg), std::move(root)});
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    VAST_DEBUG("{} got EXIT from {}", *self, msg.source);
    self->state->finish_slice();
    self->quit(msg.reason);
  });
  self->set_down_handler([=](const caf::down_msg& msg) {
    auto& st = *self->state;
    auto i = st.actor_map.find(msg.source.id());
    if (i != st.actor_map.end())
      VAST_DEBUG("{} received DOWN from {} aka {}", *self, i->second,
                 msg.source);
    else
      VAST_DEBUG("{} received DOWN from {}", *self, msg.source);
    st.actor_map.erase(msg.source.id());
  });
  self->state->mgr = self->make_continuous_source(
    // init
    [](bool&) {},
    // get next element
    [self](bool&, caf::downstream<table_slice>& out, size_t num) {
      auto& st = *self->state;
      size_t produced = 0;
      while (num-- > 0 && !st.slice_buffer.empty()) {
        auto& slice = st.slice_buffer.front();
        produced += slice.rows();
        out.push(std::move(slice));
        st.slice_buffer.pop();
      }
      VAST_TRACE_SCOPE("{} was asked for {} slices and produced {} ; {} are "
                       "remaining in buffer",
                       *self, num, produced, st.slice_buffer.size());
    },
    // done?
    [](const bool&) {
      return false;
    });
  VAST_DEBUG("{} animates heartbeat loop", *self);
  self->delayed_send(self, overview_delay, atom::telemetry_v);
  return {
    [self](atom::announce, const std::string& name) {
      auto& st = *self->state;
      st.actor_map[self->current_sender()->id()] = name;
      self->monitor(self->current_sender());
      if (name == "importer")
        st.mgr->add_outbound_path(self->current_sender(),
                                  std::make_tuple(std::string{"accountant"}));
    },
    [self](const std::string& key, duration value) {
      VAST_TRACE_SCOPE("{} received {} from {}", *self, key,
                       self->current_sender());
      self->state->record(key, value);
    },
    [self](const std::string& key, time value) {
      VAST_TRACE_SCOPE("{} received {} from {}", *self, key,
                       self->current_sender());
      self->state->record(key, value);
    },
    [self](const std::string& key, integer value) {
      VAST_TRACE_SCOPE("{} received {} from {}", *self, key,
                       self->current_sender());
      self->state->record(key, value.value);
    },
    [self](const std::string& key, count value) {
      VAST_TRACE_SCOPE("{} received {} from {}", *self, key,
                       self->current_sender());
      self->state->record(key, value);
    },
    [self](const std::string& key, real value) {
      VAST_TRACE_SCOPE("{} received {} from {}", *self, key,
                       self->current_sender());
      self->state->record(key, value);
    },
    [self](const report& r) {
      VAST_TRACE_SCOPE("{} received a report from {}", *self,
                       self->current_sender());
      time ts = std::chrono::system_clock::now();
      for (const auto& [key, value] : r) {
        auto f = [&, key = key](const auto& x) {
          self->state->record(key, x, ts);
        };
        caf::visit(f, value);
      }
    },
    [self](const performance_report& r) {
      VAST_TRACE_SCOPE("{} received a performance report from {}", *self,
                       self->current_sender());
      time ts = std::chrono::system_clock::now();
      for (const auto& [key, value] : r) {
        self->state->record(key + ".events", value.events, ts);
        self->state->record(key + ".duration", value.duration, ts);
        if (value.events == 0) {
          self->state->record(key + ".rate", 0.0, ts);
        } else {
          auto rate = value.rate_per_sec();
          if (std::isfinite(rate))
            self->state->record(key + ".rate", rate, ts);
          else
            self->state->record(key + ".rate",
                                std::numeric_limits<decltype(rate)>::max(), ts);
        }
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
        auto logger = caf::logger::current_logger();
        if (logger && logger->verbosity() >= VAST_LOG_LEVEL_INFO)
          if (key == "node_throughput")
            self->state->accumulator += value;
#endif
      }
    },
    [self](atom::status, status_verbosity v) {
      auto result = record{};
      if (v >= status_verbosity::detailed) {
        auto components = record{};
        for (const auto& [aid, name] : self->state->actor_map)
          components.emplace(name, aid);
        result["components"] = std::move(components);
      }
      if (v >= status_verbosity::debug)
        detail::fill_status_map(result, self);
      return result;
    },
    [self](atom::telemetry) {
      self->state->command_line_heartbeat();
      self->delayed_send(self, overview_delay, atom::telemetry_v);
    },
    [self](atom::config, accountant_config cfg) {
      self->state->apply_config(std::move(cfg));
      return atom::ok_v;
    },
  };
}

void accountant_state_deleter::operator()(accountant_state_impl* ptr) {
  delete ptr;
}

} // namespace vast::system
