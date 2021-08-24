//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "broker/writer.hpp"

#include "broker/zeek.hpp"

#include <vast/logger.hpp>
#include <vast/table_slice.hpp>

#include <broker/data.hh>

namespace vast::plugins::broker {

namespace {

caf::error convert(view<data> in, ::broker::data& out) {
  auto f = detail::overload{
    [&](const auto& x) -> caf::error {
      out = x;
      return {};
    },
    [&](caf::none_t) -> caf::error {
      out = {};
      return {};
    },
    [&](view<integer> x) -> caf::error {
      out = x.value;
      return {};
    },
    // FIXME: use double-dispatch together with the type to differentiate when
    // we should use broker::port vs. plain counts. Using single dispatch on
    // the data alone is not sufficient to make a proper type conversion.
    [&](view<count> x) -> caf::error {
      out = x;
      return {};
    },
    [&](view<std::string> x) -> caf::error {
      out = std::string{x};
      return {};
    },
    [&](view<pattern> x) -> caf::error {
      out = std::string{x.string()};
      return {};
    },
    [&](view<address> x) -> caf::error {
      auto bytes
        = reinterpret_cast<const uint32_t*>(std::launder(x.data().data()));
      out = ::broker::address{bytes, ::broker::address::family::ipv6,
                              ::broker::address::byte_order::network};
      return {};
    },
    [&](view<subnet> x) -> caf::error {
      auto bytes = reinterpret_cast<const uint32_t*>(
        std::launder(x.network().data().data()));
      auto addr = ::broker::address{bytes, ::broker::address::family::ipv6,
                                    ::broker::address::byte_order::network};
      out = ::broker::subnet(addr, x.length());
      return {};
    },
    [&](view<enumeration> x) -> caf::error {
      // FIXME: use double-dispatch over (data_view, type) to get type
      // information instead of just the raw integer.
      //
      // Details: we face two different implementation approaches for enums. To
      // represent the actual enum value, Broker uses a string whereas VAST
      // uses a 32-bit unsigned integer. We currently lose the type information
      // by converting the VAST enum into a Broker count. A wholistic approach
      // would include the type information for this data instance and perform
      // the string conversion.
      out = ::broker::count{x};
      return {};
    },
    [&](view<list> xs) -> caf::error {
      ::broker::vector result;
      result.reserve(xs.size());
      // TODO
      // std::transform(xs.begin(), xs.end(), std::back_inserter(result),
      //               [](const auto& x) {
      //                 return to_broker(x);
      //               });
      out = std::move(result);
      return {};
    },
    [&](view<map> xs) -> caf::error {
      ::broker::table result;
      // TODO
      // auto f = [](const auto& x) {
      //  return std::pair{to_broker(x.first), to_broker(x.second)};
      //};
      // std::transform(xs.begin(), xs.end(), std::inserter(result,
      // result.end()),
      //               f);
      out = std::move(result);
      return {};
    },
    [&](view<record> xs) -> caf::error {
      ::broker::vector result;
      result.reserve(xs.size());
      // TODO
      // auto f = [](const auto& x) {
      //  return to_broker(x.second);
      //};
      // std::transform(xs.begin(), xs.end(), std::back_inserter(result), f);
      out = std::move(result);
      return {};
    },
  };
  return caf::visit(f, in);
}

} // namespace

writer::writer(const caf::settings& options) {
  std::string category = "vast.export.broker";
  endpoint_ = make_endpoint(options, category);
  status_subscriber_ = std::make_unique<::broker::status_subscriber>(
    endpoint_->make_status_subscriber());
  topic_ = caf::get_or(options, category + '.' + "topic", "vast/data");
  event_name_ = caf::get_or(options, category + '.' + "event", "VAST::data");
}

caf::error writer::write(const table_slice& slice) {
  // First check the control plane: did something change with our peering?
  for (const auto& x : status_subscriber_->poll()) {
    auto f = detail::overload{
      [&](::broker::none) -> caf::error {
        VAST_WARN("{} ignores invalid Broker status", name());
        return {};
      },
      [&](const ::broker::error& error) -> caf::error {
        VAST_WARN("{} got Broker error: {}", name(), to_string(error));
        return error;
      },
      [&](const ::broker::status& status) -> caf::error {
        VAST_INFO("{} got Broker status: {}", name(), to_string(status));
        // TODO: decide what to do based on status type.
        return {};
      },
    };
    if (auto err = caf::visit(f, x))
      return err;
  }
  // Ship data to Zeek via Broker.
  for (size_t row = 0; row < slice.rows(); ++row) {
    // Assemble an event as a list of broker data values.
    ::broker::vector xs;
    auto&& layout = slice.layout();
    auto columns = layout.fields.size();
    xs.reserve(columns);
    for (size_t col = 0; col < columns; ++col) {
      ::broker::data x;
      if (auto err = convert(slice.at(row, col), x))
        return err;
      xs.push_back(std::move(x));
    }
    // TODO: rethink how we are going to map the type name to Zeek events.
    // The current format here assumes one global event that Zeek handles:
    //     global result: event(id: string, data: any);
    // We probably want a more fine-grained solution.
    ::broker::vector args(2);
    args[0] = slice.layout().name();
    args[1] = std::move(xs);
    auto event = ::broker::zeek::Event{event_name_, std::move(args)};
    endpoint_->publish(topic_, std::move(event));
  }
  return caf::none;
}

caf::expected<void> writer::flush() {
  return caf::no_error;
}

const char* writer::name() const {
  return "broker-writer";
}

} // namespace vast::plugins::broker
