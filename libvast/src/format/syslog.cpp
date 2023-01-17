//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/syslog.hpp"

#include "vast/fwd.hpp"

#include "vast/error.hpp"
#include "vast/type.hpp"

#include <caf/expected.hpp>
#include <caf/settings.hpp>

namespace vast {
namespace format {
namespace syslog {

namespace {

type make_rfc5424_type() {
  return type{
    "syslog.rfc5424",
    record_type{{
      {"facility", uint64_type{}},
      {"severity", uint64_type{}},
      {"version", uint64_type{}},
      {"ts", type{"timestamp", time_type{}}},
      {"hostname", string_type{}},
      {"app_name", string_type{}},
      {"process_id", string_type{}},
      {"message_id", string_type{}},
      // TODO: The index is currently incapable of handling map_type. Hence, the
      // structured_data is disabled.
      // {"structered_data", map_type{
      //   string_type{}.name("id"),
      //   map_type{string_type{}.name("key"),
      //   string_type{}.name("value")}.name("params")},
      // },
      {"message", string_type{}},
    }},
  };
}

type make_unknown_type() {
  // clang-format off
  return type{"syslog.unknown", record_type{{
    {"syslog_message", string_type{}}
  }},};
  // clang-format on
}

} // namespace

reader::reader(const caf::settings& options, std::unique_ptr<std::istream> in)
  : super(options),
    syslog_rfc5424_type_{make_rfc5424_type()},
    syslog_unkown_type_{make_unknown_type()} {
  if (in != nullptr)
    reset(std::move(in));
}

caf::error reader::module(vast::module x) {
  // clang-format off
  return replace_if_congruent({
    &syslog_rfc5424_type_,
    &syslog_unkown_type_
  }, x);
  // clang-format on
}

vast::module reader::module() const {
  vast::module mod;
  mod.add(syslog_rfc5424_type_);
  mod.add(syslog_unkown_type_);
  return mod;
}

void reader::reset(std::unique_ptr<std::istream> in) {
  VAST_ASSERT(in != nullptr);
  input_ = std::move(in);
  lines_ = std::make_unique<detail::line_range>(*input_);
}

const char* reader::name() const {
  return "syslog-reader";
}

caf::error
reader::read_impl(size_t max_events, size_t max_slice_size, consumer& f) {
  table_slice_builder_ptr bptr = nullptr;
  size_t produced = 0;
  while (produced < max_events) {
    if (lines_->done())
      return finish(f, caf::make_error(ec::end_of_input, "input exhausted"));
    if (batch_events_ > 0 && batch_timeout_ > reader_clock::duration::zero()
        && last_batch_sent_ + batch_timeout_ < reader_clock::now()) {
      VAST_DEBUG("{} reached batch timeout", detail::pretty_type_name(this));
      return finish(f, ec::timeout);
    }
    auto timed_out = lines_->next_timeout(read_timeout_);
    if (timed_out) {
      VAST_DEBUG("{} stalled at line {}", detail::pretty_type_name(this),
                 lines_->line_number());
      return ec::stalled;
    }
    auto& line = lines_->get();
    if (line.empty()) {
      // Ignore empty lines.
      VAST_DEBUG("{} ignores empty line at {}", detail::pretty_type_name(this),
                 lines_->line_number());
      continue;
    }
    message sys_msg;
    auto parser = message_parser{};
    if (parser(line, sys_msg)) {
      bptr = builder(syslog_rfc5424_type_);
      if (!bptr) {
        return finish(
          f, caf::make_error(ec::format_error,
                             fmt::format("failed to get create table "
                                         "slice builder for type {}",
                                         syslog_rfc5424_type_.name())));
      }
      // TODO: The index is currently incapable of handling map_type.
      // Hence, the structured_data is disabled.
      if (!bptr->add(sys_msg.hdr.facility, sys_msg.hdr.severity,
                     sys_msg.hdr.version, sys_msg.hdr.ts, sys_msg.hdr.hostname,
                     sys_msg.hdr.app_name, sys_msg.hdr.process_id,
                     sys_msg.hdr.msg_id,
                     /*sys_msg.data,*/ sys_msg.msg))
        return finish(
          f, caf::make_error(ec::format_error,
                             fmt::format("failed to produce table slice row "
                                         "for {}",
                                         syslog_rfc5424_type_.name())));
      if (bptr->rows() >= max_slice_size)
        if (auto err = finish(f))
          return err;
    } else {
      bptr = builder(syslog_unkown_type_);
      if (!bptr)
        return finish(f,
                      caf::make_error(ec::format_error,
                                      fmt::format("failed to get create table "
                                                  "slice builder for type {}",
                                                  syslog_unkown_type_.name())));
      if (!bptr->add(line))
        return finish(
          f, caf::make_error(ec::format_error,
                             fmt::format("failed to produce table slice row "
                                         "for {}",
                                         syslog_unkown_type_.name())));
      if (bptr->rows() >= max_slice_size)
        if (auto err = finish(f))
          return err;
    }
    ++produced;
    ++batch_events_;
  }
  return finish(f);
}

} // namespace syslog
} // namespace format
} // namespace vast
