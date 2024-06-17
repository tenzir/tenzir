//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/accountant.hpp"

#include "tenzir/accountant_config.hpp"
#include "tenzir/concept/printable/std/chrono.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/coding.hpp"
#include "tenzir/detail/fill_status_map.hpp"
#include "tenzir/detail/make_io_stream.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/posix.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/report.hpp"
#include "tenzir/status.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/table_slice_builder.hpp"
#include "tenzir/time.hpp"
#include "tenzir/view.hpp"

#include <caf/attach_continuous_stream_source.hpp>
#include <caf/broadcast_downstream_manager.hpp>
#include <caf/downstream.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <chrono>
#include <cmath>
#include <filesystem>
#include <ios>
#include <limits>
#include <queue>
#include <string>
#include <string_view>

namespace tenzir {

struct accountant_state_impl {
  // -- member types -----------------------------------------------------------

  using downstream_manager = caf::broadcast_downstream_manager<table_slice>;

  // -- constructor, destructors, and assignment operators ---------------------

  accountant_state_impl(accountant_actor::pointer self, accountant_config cfg,
                        std::filesystem::path root)
    : self{self}, root{std::move(root)} {
    apply_config(std::move(cfg));
    actor_map[self->id()] = self->name();
    record(self->id(), "startup", 0, {}, {});
  }

  // -- member variables -------------------------------------------------------

  /// Stores the parent actor handle.
  accountant_actor::pointer self;

  /// The root path of the database.
  std::filesystem::path root;

  /// Stores the names of known actors to fill into the actor_name column.
  std::unordered_map<caf::actor_id, std::string> actor_map;

  /// Stores the builder instance per type name.
  std::unordered_map<std::string, table_slice_builder_ptr> builders;

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

  /// Handle to the UDS output channel is currently dropping its input.
  bool uds_datagram_sink_dropping = false;

  /// The error code of the last error that came out of the uds sink.
  /// This variable is used to prevent repeating the same warning for
  /// every metric in case the receiver is not present of misconfigured.
  ec last_uds_error = ec::no_error;

  /// The configuration.
  accountant_config cfg;

  // -- utility functions ------------------------------------------------------

  void finish_slice(table_slice_builder_ptr& builder) {
    // Do nothing if builder has not been created or no rows have been added yet.
    if (!builder || builder->rows() == 0)
      return;
    auto slice = builder->finish();
    TENZIR_DEBUG("{} generated slice with {} rows", *self, slice.rows());
    slice_buffer.push(std::move(slice));
    mgr->tick(self->clock().now());
  }

  void record_internally(const caf::actor_id actor_id, const std::string& key,
                         double x, const metrics_metadata& meta1,
                         const metrics_metadata& meta2, time ts) {
    // This is a workaround to a bug that is somewhere else -- the index cannot
    // handle NaN, and a bug that we were unable to reproduce reliably caused
    // the accountant to forward NaN to the index here.
    if (!std::isfinite(x)) {
      TENZIR_DEBUG("{} cannot record a non-finite metric", *self);
      return;
    }
    auto& builder = builders[key];
    if (!builder) {
      auto schema_fields = std::vector<record_type::field_view>{
        {"ts", time_type{}},
        {"version", string_type{}},
        {"actor", string_type{}},
        {"value", double_type{}},
      };
      schema_fields.reserve(schema_fields.size() + meta1.size() + meta2.size());
      for (const auto& [key, _] : meta1)
        schema_fields.emplace_back(key, string_type{});
      for (const auto& [key, _] : meta2)
        schema_fields.emplace_back(key, string_type{});
      auto schema = type{
        fmt::format("tenzir.metrics.{}", key),
        record_type{schema_fields},
      };
      builder = std::make_shared<table_slice_builder>(schema);
      TENZIR_DEBUG("{} obtained a table slice builder", *self);
    }
    {
      const auto added = builder->add(ts, std::string_view{version::version},
                                      actor_map[actor_id], x);
      TENZIR_ASSERT(added);
    }
    for (const auto& [_, value] : meta1) {
      const auto added = builder->add(value);
      TENZIR_ASSERT(added);
    }
    for (const auto& [_, value] : meta2) {
      const auto added = builder->add(value);
      TENZIR_ASSERT(added);
    }
    if (builder->rows() == static_cast<size_t>(cfg.self_sink.slice_size))
      finish_slice(builder);
  }

  std::vector<char>
  to_json_line(const caf::actor_id actor_id, time ts, const std::string& key,
               double x, const metrics_metadata& meta1,
               const metrics_metadata& meta2) {
    using namespace std::string_view_literals;
    auto printer = json_printer{json_printer_options{
      .oneline = true,
      .omit_empty_records = true,
    }};
    auto metadata = tenzir::record{};
    metadata.reserve(meta1.size() + meta2.size());
    for (const auto& [key, value] : meta1)
      metadata.emplace(key, value);
    for (const auto& [key, value] : meta2)
      metadata.emplace(key, value);
    const auto entry = tenzir::record{
      {"ts", ts},
      {"version", version::version},
      {"actor", actor_map[actor_id]},
      {"key", key},
      {"value", x},
      {"metadata", metadata},
    };
    auto buf = std::vector<char>{};
    auto iter = std::back_inserter(buf);
    const auto ok = printer.print(iter, entry);
    TENZIR_ASSERT(ok);
    *iter++ = '\n';
    return buf;
  }

  std::ostream&
  record_to_output(const caf::actor_id actor_id, const std::string& key,
                   double x, const metrics_metadata& meta1,
                   const metrics_metadata& meta2, time ts, std::ostream& os,
                   bool real_time) {
    auto buf = to_json_line(actor_id, ts, key, x, meta1, meta2);
    os.write(buf.data(), detail::narrow_cast<std::streamsize>(buf.size()));
    if (real_time)
      os << std::flush;
    return os;
  }

  void
  record_to_unix_datagram(const caf::actor_id actor_id, const std::string& key,
                          double x, const metrics_metadata& meta1,
                          const metrics_metadata& meta2, time ts,
                          detail::uds_datagram_sender& dest) {
    auto buf = to_json_line(actor_id, ts, key, x, meta1, meta2);
    // Only poll the socket for write readiness if it did not drop the previous
    // line. Not doing this could increase the average message processing time
    // of the accountant by the timeout value, and just as with any actor,
    // receiving messages at a higher frequency that they can be processed will
    // eventually consume all available memory. The initial timeout of 1 second
    // is somewhat arbitrary. Long enough so any re-initialization of the
    // listening side would not incur data loss without being excessive.
    auto timeout_usec = uds_datagram_sink_dropping ? 0 : 1'000'000;
    if (auto err = dest.send(
          std::span<char>{reinterpret_cast<char*>(buf.data()), buf.size()},
          timeout_usec)) {
      const auto code = ec{err.code()};
      if (code == ec::timeout)
        uds_datagram_sink_dropping = true;
      if (code != last_uds_error) {
        last_uds_error = code;
        TENZIR_WARN("{} failed to write metrics to UDS sink: {}", *self, err);
      }
      return;
    } else {
      last_uds_error = ec::no_error;
    }
  }

  void record(const caf::actor_id actor_id, const std::string& key, double x,
              const metrics_metadata& meta1, const metrics_metadata& meta2,
              time ts = std::chrono::system_clock::now()) {
    if (cfg.self_sink.enable)
      record_internally(actor_id, key, x, meta1, meta2, ts);
    if (file_sink)
      record_to_output(actor_id, key, x, meta1, meta2, ts, *file_sink,
                       cfg.file_sink.real_time);
    if (uds_sink)
      record_to_output(actor_id, key, x, meta1, meta2, ts, *uds_sink,
                       cfg.uds_sink.real_time);
    if (uds_datagram_sink)
      record_to_unix_datagram(actor_id, key, x, meta1, meta2, ts,
                              *uds_datagram_sink);
  }

  void record(const caf::actor_id actor_id, const std::string& key, duration x,
              const metrics_metadata& meta1, const metrics_metadata& meta2,
              time ts = std::chrono::system_clock::now()) {
    auto ms = std::chrono::duration<double, std::milli>{x}.count();
    record(actor_id, key, ms, meta1, meta2, ts);
  }

  void record(const caf::actor_id actor_id, const std::string& key, time x,
              const metrics_metadata& meta1, const metrics_metadata& meta2,
              time ts = std::chrono::system_clock::now()) {
    record(actor_id, key, x.time_since_epoch(), meta1, meta2, ts);
  }

  void apply_config(accountant_config cfg) {
    auto& old = this->cfg;
    // Act on file sink config.
    bool start_file_sink = cfg.file_sink.enable && !old.file_sink.enable;
    bool stop_file_sink = !cfg.file_sink.enable && old.file_sink.enable;
    if (stop_file_sink) {
      TENZIR_INFO("{} closing metrics output file {}", *self,
                  old.file_sink.path);
      file_sink.reset(nullptr);
    }
    if (start_file_sink) {
      auto s = detail::make_output_stream(root / cfg.file_sink.path,
                                          std::filesystem::file_type::regular,
                                          std::ios_base::app);
      if (s) {
        TENZIR_VERBOSE("{} writes metrics to {}", *self, cfg.file_sink.path);
        file_sink = std::move(*s);
      } else {
        TENZIR_WARN("{} failed to open {} for metrics: {}", *self,
                    cfg.file_sink.path, s.error());
      }
    }
    // Act on uds sink config.
    bool start_uds_sink = cfg.uds_sink.enable && !old.uds_sink.enable;
    bool stop_uds_sink = !cfg.uds_sink.enable && old.uds_sink.enable;
    if (stop_uds_sink) {
      TENZIR_INFO("{} closing metrics output socket {}", *self,
                  old.uds_sink.path);
      uds_sink.reset(nullptr);
    }
    if (start_uds_sink) {
      if (cfg.uds_sink.type == detail::uds_socket_type::datagram) {
        auto s = detail::uds_datagram_sender::make(root / cfg.uds_sink.path);
        if (s) {
          TENZIR_INFO("{} writes metrics to {}", *self, cfg.uds_sink.path);
          uds_datagram_sink
            = std::make_unique<detail::uds_datagram_sender>(std::move(*s));
        } else {
          TENZIR_INFO("{} could not open {} for metrics: {}", *self,
                      cfg.uds_sink.path, s.error());
        }
      } else {
        auto s = detail::make_output_stream(root / cfg.uds_sink.path,
                                            cfg.uds_sink.type);
        if (s) {
          TENZIR_INFO("{} writes metrics to {}", *self, cfg.uds_sink.path);
          uds_sink = std::move(*s);
        } else {
          TENZIR_INFO("{} could not open {} for metrics: {}", *self,
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
    TENZIR_DEBUG("{} got EXIT from {}", *self, msg.source);
    for (auto& [_, builder] : self->state->builders)
      self->state->finish_slice(builder);
    self->state->record(self->id(), "shutdown", 0, {}, {});
    self->quit(msg.reason);
  });
  self->set_down_handler([=](const caf::down_msg& msg) {
    auto& st = *self->state;
    auto i = st.actor_map.find(msg.source.id());
    if (i != st.actor_map.end())
      TENZIR_DEBUG("{} received DOWN from {} aka {}", *self, i->second,
                   msg.source);
    else
      TENZIR_DEBUG("{} received DOWN from {}", *self, msg.source);
    st.actor_map.erase(msg.source.id());
  });
  self->state->mgr = caf::attach_continuous_stream_source(
    self,
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
      TENZIR_TRACE_SCOPE("{} was asked for {} slices and produced {} ; {} are "
                         "remaining in buffer",
                         *self, num, produced, st.slice_buffer.size());
    },
    // done?
    [](const bool&) {
      return false;
    });
  return {
    [self](atom::announce, const std::string& name) {
      auto& st = *self->state;
      st.actor_map[self->current_sender()->id()] = name;
      self->monitor(self->current_sender());
      if (name == "importer" && st.cfg.self_sink.enable)
        st.mgr->add_outbound_path(self->current_sender(),
                                  std::make_tuple(std::string{"accountant"}));
    },
    [self](atom::metrics, const std::string& key, duration value,
           metrics_metadata& metadata) {
      TENZIR_TRACE_SCOPE("{} received {} from {}", *self, key,
                         self->current_sender());
      self->state->record(self->current_sender()->id(), key, value, metadata,
                          {});
    },
    [self](atom::metrics, const std::string& key, time value,
           metrics_metadata& metadata) {
      TENZIR_TRACE_SCOPE("{} received {} from {}", *self, key,
                         self->current_sender());
      self->state->record(self->current_sender()->id(), key, value, metadata,
                          {});
    },
    [self](atom::metrics, const std::string& key, int64_t value,
           metrics_metadata& metadata) {
      TENZIR_TRACE_SCOPE("{} received {} from {}", *self, key,
                         self->current_sender());
      self->state->record(self->current_sender()->id(), key,
                          detail::narrow_cast<double>(value), metadata, {});
    },
    [self](atom::metrics, const std::string& key, uint64_t value,
           metrics_metadata& metadata) {
      TENZIR_TRACE_SCOPE("{} received {} from {}", *self, key,
                         self->current_sender());
      self->state->record(self->current_sender()->id(), key,
                          detail::narrow_cast<double>(value), metadata, {});
    },
    [self](atom::metrics, const std::string& key, double value,
           metrics_metadata& metadata) {
      TENZIR_TRACE_SCOPE("{} received {} from {}", *self, key,
                         self->current_sender());
      self->state->record(self->current_sender()->id(), key, value, metadata,
                          {});
    },
    [self](atom::metrics, const report& r) {
      TENZIR_TRACE_SCOPE("{} received a report from {}", *self,
                         self->current_sender());
      time ts = std::chrono::system_clock::now();
      for (const auto& [key, value, meta] : r.data) {
        auto f = [&, &key = key, &meta = meta](const auto& x) {
          self->state->record(self->current_sender()->id(), key, x, meta,
                              r.metadata, ts);
        };
        caf::visit(f, value);
      }
    },
    [self](atom::metrics, const performance_report& r) {
      TENZIR_TRACE_SCOPE("{} received a performance report from {}", *self,
                         self->current_sender());
      time ts = std::chrono::system_clock::now();
      for (const auto& [key, value, meta] : r.data) {
        self->state->record(self->current_sender()->id(), key + ".events",
                            detail::narrow_cast<double>(value.events), meta,
                            r.metadata, ts);
        self->state->record(self->current_sender()->id(), key + ".duration",
                            value.duration, meta, r.metadata, ts);
        if (value.events == 0) {
          self->state->record(self->current_sender()->id(), key + ".rate", 0.0,
                              meta, r.metadata, ts);
        } else {
          auto rate = value.rate_per_sec();
          if (std::isfinite(rate))
            self->state->record(self->current_sender()->id(), key + ".rate",
                                rate, meta, r.metadata, ts);
          else
            self->state->record(self->current_sender()->id(), key + ".rate",
                                std::numeric_limits<decltype(rate)>::max(),
                                meta, r.metadata, ts);
        }
      }
    },
    [self](atom::status, status_verbosity v, duration) {
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
    [self](atom::config, accountant_config cfg) {
      self->state->apply_config(std::move(cfg));
      return atom::ok_v;
    },
  };
}

void accountant_state_deleter::operator()(accountant_state_impl* ptr) {
  delete ptr;
}

} // namespace tenzir
