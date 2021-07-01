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
#include <vast/detail/assert.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/detail/overload.hpp>
#include <vast/error.hpp>
#include <vast/flow.hpp>
#include <vast/logger.hpp>

#include <broker/zeek.hh>
#include <caf/none.hpp>
#include <caf/settings.hpp>
#include <caf/sum_type.hpp>

using namespace std::string_literals;

namespace vast::plugins::broker {

reader::reader(const caf::settings& options, std::unique_ptr<std::istream> in)
  : super{options} {
  // We're not getting data via the passed istream and ignore the parameter.
  VAST_ASSERT(in == nullptr);
  // TODO: figure out how to pass VAST's actor system
  endpoint_ = std::make_unique<::broker::endpoint>();
  static const auto category = "vast.import.broker"s;
  auto addr = caf::get_or(options, category + ".host", "localhost");
  auto port = caf::get_or(options, category + ".port", uint16_t{9999});
  auto listen = caf::get_or(options, category + ".listen", false);
  // Either open a socket and listen or peer with the remote endpoint.
  if (listen) {
    VAST_INFO("{} listens on {}:{}", name(), addr, port);
    endpoint_->listen(addr, port);
  } else {
    auto timeout = caf::get_or(options, "vast.import.broker.retry-timeout", 10);
    VAST_INFO("{} connects to {}:{} (retries every {} seconds)", name(), addr,
              port, timeout);
    endpoint_->peer(addr, port, ::broker::timeout::seconds(timeout));
  }
  // Subscribe to control plane events.
  status_subscriber_ = std::make_unique<::broker::status_subscriber>(
    endpoint_->make_status_subscriber());
  // Subscribe to data plane events.
  auto topics = caf::get_or(options, "vast.import.broker.topic",
                            std::vector{"zeek/logs"s});

  std::vector<::broker::topic> broker_topics;
  broker_topics.reserve(topics.size());
  for (auto& topic : topics) {
    VAST_INFO("{} subscribes to topic {}", name(), topic);
    broker_topics.push_back(std::move(topic));
  }
  // auto max_queue_size = size_t{1'024}; // default is 20
  auto max_queue_size = size_t{20}; // default is 20
  subscriber_ = std::make_unique<::broker::subscriber>(
    endpoint_->make_subscriber(std::move(broker_topics), max_queue_size));
}

void reader::reset(std::unique_ptr<std::istream>) {
  // Nothing to do here; we're getting data via a Broker socket.
}

caf::error
reader::read_impl(size_t max_events, size_t max_slice_size, consumer& f) {
  // FIXME: remove this endless loop and make this event-driven. This is a
  // pathetic hack to keep the control in this function to synchronize with the
  // remote Broker peer.
  while (true) {
    // First check the control plane: did something change with our peering?
    for (auto x : status_subscriber_->poll()) {
      auto f = detail::overload{
        [&](::broker::none) {
          VAST_WARN("{} ignores invalid Broker status: {}", name());
        },
        [&](const ::broker::error& error) {
          VAST_WARN("{} got Broker error: {}", name(), to_string(error));
          // TODO: decide what to do based on error type.
        },
        [&](const ::broker::status& status) {
          VAST_WARN("{} got Broker status: {}", name(), to_string(status));
          // TODO: decide what to do based on status type.
        },
      };
      caf::visit(f, x);
    }
    // Then check the data plane: process available events.
    if (zeek_) {
      for (auto x : subscriber_->poll())
        if (auto err = dispatch_message(get_topic(x), get_data(x)))
          VAST_ERROR("{} failed to dispatch Zeek message: {}", err);
    } else {
      for (auto x : subscriber_->poll()) {
        auto& topic = get_topic(x);
        auto& data = get_data(x);
        // Not in Zeek mode; do our regular thing.
        VAST_DEBUG("{} got message: {} -> {}", name(), to_string(topic),
                   to_string(data));
        // Package data up in table slices.
        auto f = detail::overload{
          [&](auto&&) {
            // TODO
          },
        };
        caf::visit(f, data);
      }
    }
    // FIXME: remove silly loop.
    std::this_thread::sleep_for(std::chrono::seconds(1));
    VAST_DEBUG("{} loops", name());
  }
  return {};
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

caf::error reader::dispatch_message(const ::broker::topic& topic,
                                    const ::broker::data& msg) {
  switch (::broker::zeek::Message::type(msg)) {
    default:
      VAST_WARN("{} skips unknown message [{}]", topic);
      break;
    case ::broker::zeek::Message::Type::Invalid: {
      VAST_WARN("{} skips invalid message [{}]: {}", name(), topic,
                to_string(msg));
      break;
    }
    case ::broker::zeek::Message::Type::Event: {
      auto event = ::broker::zeek::Event{msg};
      VAST_WARN("{} skips indigestible event [{}]: {}", name(), topic,
                event.name());
      break;
    }
    case ::broker::zeek::Message::Type::LogCreate: {
      auto log_create = ::broker::zeek::LogCreate{msg};
      VAST_DEBUG("{} received log create message [{}]: {}", name(), topic,
                 log_create.stream_id());
      // TODO: create a table slice builder out of the data in here.
      break;
    }
    case ::broker::zeek::Message::Type::LogWrite: {
      auto log_write = ::broker::zeek::LogWrite{msg};
      VAST_DEBUG("{} received log write message [{}]: {}", name(), topic,
                 log_write.stream_id());
      // TODO: write the contained event into the table slice builder. Thus
      // far, we print all fields on log level INFO.
      return process(log_write);
    }
    case ::broker::zeek::Message::Type::IdentifierUpdate: {
      auto id_update = ::broker::zeek::IdentifierUpdate{msg};
      VAST_DEBUG("{} skips indigestible identifier update [{}]: {} -> {}",
                 name(), topic, id_update.id_name(), id_update.id_value());
      break;
    }
    case ::broker::zeek::Message::Type::Batch: {
      auto batch = ::broker::zeek::Batch{msg};
      VAST_DEBUG("{} received Zeek message batch [{}]", name(), topic);
      for (auto& msg : batch.batch())
        dispatch_message(topic, msg); // recurse
      break;
    }
  }
  return {};
}

} // namespace vast::plugins::broker
