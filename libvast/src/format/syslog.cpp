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

#include "vast/format/syslog.hpp"

#include "vast/error.hpp"
#include "vast/fwd.hpp"
#include "vast/type.hpp"

#include <caf/expected.hpp>

namespace vast {
namespace format {
namespace syslog {

namespace {

// At the moment map_type cannot be indexed, therefore it is not part of the
// schema for now
type make_syslog_msg_type() {
  // clang-format off
  return record_type{{
    {"facility", count_type{}},
    {"severity", count_type{}},
    {"version", count_type{}},
    {"ts", time_type{}.attributes({{"timestamp"}})},
    {"hostname", string_type{}},
    {"app_name", string_type{}},
    {"process_id", string_type{}},
    {"message_id", string_type{}},  
    // {"structered_data", map_type{
    //   string_type{}.name("id"),
    //   map_type{string_type{}.name("key"), string_type{}.name("value")}.name("params")},
    // },
    {"message", string_type{}},
  }}.name("syslog::msg");
  // clang-format on
}

} // namespace

reader::reader(caf::atom_value table_slice_type,
               [[maybe_unused]] const caf::settings& options,
               std::unique_ptr<std::istream> in)
  : super(table_slice_type), syslog_msg_type_{make_syslog_msg_type()} {
  if (in != nullptr)
    reset(std::move(in));
}

caf::error reader::schema(vast::schema x) {
  return replace_if_congruent({&syslog_msg_type_}, x);
}

vast::schema reader::schema() const {
  vast::schema sch;
  sch.add(syslog_msg_type_);
  return sch;
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
  if (builder_ == nullptr) {
    if (auto r = caf::get_if<record_type>(&syslog_msg_type_)) {
      reset_builder(*r);
    } else {
      return finish(f, make_error(ec::format_error, "failed to get record type "
                                                    "for builder"));
    }
  }
  size_t produced = 0;
  while (produced < max_events) {
    if (lines_->done())
      return finish(f, make_error(ec::end_of_input, "input exhausted"));
    // Parse curent line.
    auto& line = lines_->get();
    message sys_msg;
    auto parser = message_parser{};
    if (!parser(line, sys_msg)) {
      return finish(f, make_error(ec::parse_error, "failed to parse syslog "
                                                   "message"));
    }
    // until map_types are supported, the structured data of a message won't be
    // stored
    if (!builder_->add(sys_msg.header.facility, sys_msg.header.severity,
                       sys_msg.header.version, sys_msg.header.ts,
                       sys_msg.header.hostname, sys_msg.header.app_name,
                       sys_msg.header.process_id, sys_msg.header.msg_id,
                       /*sys_msg.data,*/ sys_msg.msg)) {
      return finish(f, make_error(ec::format_error,
                                  "failed to produce table slice row for "
                                    + builder_->layout().name()));
    }
    if (builder_->rows() >= max_slice_size) {
      if (auto err = finish(f)) {
        return err;
      };
    }
    ++produced;
    lines_->next();
  }
  return caf::none;
}

} // namespace syslog
} // namespace format
} // namespace vast
