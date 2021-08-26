//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "broker/reader.hpp"

#include "broker/zeek.hpp"

#include <vast/community_id.hpp>
#include <vast/concept/printable/to_string.hpp>
#include <vast/concept/printable/vast/data.hpp>
#include <vast/detail/assert.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/detail/overload.hpp>
#include <vast/detail/zeekify.hpp>
#include <vast/error.hpp>
#include <vast/flow.hpp>
#include <vast/logger.hpp>

#include <broker/zeek.hh>
#include <caf/none.hpp>
#include <caf/settings.hpp>
#include <caf/sum_type.hpp>

using namespace std::string_literals;

namespace vast::plugins::broker {

reader::reader(const caf::settings& options, caf::actor_system& sys)
  : super{options} {
  // TODO: consider spawning the endpoint core actor manually here because it
  // contains an actor system.
  endpoint_ = make_endpoint(options, "vast.import.broker");
  // Subscribe to control plane events.
  status_subscriber_ = std::make_unique<::broker::status_subscriber>(
    endpoint_->make_status_subscriber());
  // Subscribe to data plane events.
  auto topics = caf::get_or(options, "vast.import.broker.topic",
                            std::vector{"zeek/logs"s});
  subscriber_ = make_subscriber(*endpoint_, std::move(topics));
}

void reader::reset(std::unique_ptr<std::istream>) {
  // Nothing to do here; we're getting data via a Broker socket.
}

caf::error
reader::read_impl(size_t max_events, size_t max_slice_size, consumer& f) {
  // Sanity checks.
  VAST_ASSERT(max_events > 0);
  VAST_ASSERT(max_slice_size > 0);
  // First check the control plane: did something change with our peering?
  for (const auto& x : status_subscriber_->poll()) {
    auto f = detail::overload{
      [&](::broker::none) {
        VAST_WARN("{} ignores invalid Broker status", name());
      },
      [&](const ::broker::error& error) {
        VAST_WARN("{} got Broker error: {}", name(), to_string(error));
        // TODO: decide what to do based on error type.
      },
      [&](const ::broker::status& status) {
        VAST_INFO("{} got Broker status: {}", name(), to_string(status));
        // TODO: decide what to do based on status type.
      },
    };
    caf::visit(f, x);
  }
  // Then check the data plane: process available events.
  if (zeek_mode_) {
    // When imitating a Zeek logger node, we have a well-defined message order:
    // log create messages precede log write messages. The former
    // contain the type information and the latter the event data.
    auto xs = subscriber_->get(max_events, batch_timeout_);
    for (const auto& x : xs)
      if (auto err = dispatch(get_data(x), max_slice_size, f)) {
        VAST_ERROR("{} failed to dispatch Zeek message: {}", name(), err);
        return finish(f, err);
      }
    if (xs.empty())
      return finish(f, ec::stalled);
    if (xs.size() < max_events)
      return finish(f, ec::timeout);
  } else {
    return ec::unimplemented;
    // TODO: We don't have a well-defined Broker wire format yet that allows
    // us to map payload data to event types. Most naturally, the topic
    // suffix, e.g., x in /foo/bar/x, will be the event name and then the
    // Broker data must be a vector containing the record fields.
    // for (auto x : subscriber_->poll()) {
    //  auto& topic = get_topic(x);
    //  auto& data = get_data(x);
    //  VAST_DEBUG("{} got message: {} -> {}", name(), to_string(topic),
    //             to_string(data));
    //  auto f = detail::overload{
    //    [&](auto&&) -> caf::error {
    //      return ec::unimplemented;
    //    },
    //  };
    //  if (auto err = caf::visit(f, data))
    //    return err;
    //}
  }
  return finish(f);
}

caf::error reader::schema([[maybe_unused]] class schema schema) {
  // The VAST types are automatically generated and cannot be changed.
  return caf::make_error(ec::no_error,
                         "schema cannot be changed as it is generated "
                         "dynamically");
}

schema reader::schema() const {
  return schema_;
}

const char* reader::name() const {
  return "broker-reader";
}

caf::error reader::dispatch(const ::broker::data& msg, size_t max_slice_size,
                            consumer& f) {
  switch (::broker::zeek::Message::type(msg)) {
    default:
      VAST_WARN("{} skips unknown message", name());
      break;
    case ::broker::zeek::Message::Type::Invalid: {
      VAST_WARN("{} skips invalid message: {}", name(), msg);
      break;
    }
    case ::broker::zeek::Message::Type::Event: {
      auto event = ::broker::zeek::Event{msg};
      VAST_WARN("{} skips indigestible event: {}", name(), event.name());
      break;
    }
    case ::broker::zeek::Message::Type::LogCreate: {
      auto log_create = ::broker::zeek::LogCreate{msg};
      VAST_DEBUG("{} received log create message: {}", name(),
                 log_create.stream_id());
      const auto& stream_id = log_create.stream_id().name;
      auto layout = process(log_create);
      if (!layout)
        return layout.error();
      *layout = detail::zeekify(*layout);
      auto& builder = log_layouts_[stream_id];
      if (builder) {
        if (*layout != builder->layout()) {
          VAST_INFO("{} received schema change for stream ID {}", name(),
                    stream_id);
          if (auto err = finish(f, builder, caf::none))
            return err;
          builder = this->builder(*layout); // use new layout from now on
          if (!builder)
            return caf::make_error(ec::parse_error, "failed to create table "
                                                    "slice builder");
        } else {
          VAST_DEBUG("{} ignores identical layout for stream ID {}: {}", name(),
                     stream_id);
        }
      } else {
        VAST_INFO("{} got schema for new stream {}", name(), stream_id);
        builder = this->builder(*layout);
        if (!builder)
          return caf::make_error(ec::parse_error, "failed to create table "
                                                  "slice builder");
      }
      break;
    }
    case ::broker::zeek::Message::Type::LogWrite: {
      auto log_write = ::broker::zeek::LogWrite{msg};
      VAST_DEBUG("{} received log write message: {}", name(),
                 log_write.stream_id());
      const auto& stream_id = log_write.stream_id().name;
      auto xs = process(log_write);
      if (!xs)
        return xs.error();
      auto i = log_layouts_.find(stream_id);
      if (i == log_layouts_.end()) {
        VAST_WARN("{} has no layout for stream {}, stream out of sync?", name(),
                  stream_id);
        break;
      }
      auto builder = i->second;
      VAST_ASSERT(xs->size() == builder->columns());
      for (const auto& x : *xs)
        if (!builder->add(x))
          return finish(f, builder,
                        caf::make_error(ec::parse_error,
                                        fmt::format("failed to add value {} to "
                                                    "event stream {}",
                                                    x, stream_id)));
      ++batch_events_;
      if (builder->rows() >= max_slice_size)
        if (auto err = finish(f, builder, caf::none))
          return err;
      break;
    }
    case ::broker::zeek::Message::Type::IdentifierUpdate: {
      auto id_update = ::broker::zeek::IdentifierUpdate{msg};
      VAST_DEBUG("{} skips indigestible identifier update: {} -> {}", name(),
                 id_update.id_name(), id_update.id_value());
      break;
    }
    case ::broker::zeek::Message::Type::Batch: {
      auto batch = ::broker::zeek::Batch{msg};
      VAST_DEBUG("{} received batch of {} messages", name(),
                 batch.batch().size());
      for (auto& x : batch.batch())
        if (auto err = dispatch(x, max_slice_size, f)) // recurse
          return err;
      break;
    }
  }
  return {};
}

} // namespace vast::plugins::broker
