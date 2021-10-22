//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "broker/writer.hpp"

#include "broker/zeek.hpp"

#include <vast/legacy_type.hpp>
#include <vast/logger.hpp>
#include <vast/table_slice.hpp>

#include <broker/data.hh>

#include <new>

namespace vast::plugins::broker {

namespace {

caf::error
convert(view<data> in, ::broker::data& out, const legacy_type& field_type) {
  auto f = detail::overload{
    [&](const auto& x, const auto& y) -> caf::error {
      return caf::make_error(ec::type_clash, detail::pretty_type_name(x),
                             detail::pretty_type_name(y));
    },
    [&](caf::none_t, const auto&) -> caf::error {
      out = {};
      return {};
    },
    [&](caf::none_t, const legacy_alias_type&) -> caf::error {
      out = {};
      return {};
    },
    [&](count x, const legacy_alias_type& y) -> caf::error {
      if (y.name() == "port") {
        out = ::broker::port{static_cast<::broker::port::number_type>(x),
                             ::broker::port::protocol::unknown};
        return {};
      }
      return convert(in, out, y.value_type);
    },
    [&](const auto&, const legacy_alias_type& y) -> caf::error {
      return convert(in, out, y.value_type);
    },
    [&](view<bool> x, const legacy_bool_type&) -> caf::error {
      out = x;
      return {};
    },
    [&](view<integer> x, const legacy_integer_type&) -> caf::error {
      out = x.value;
      return {};
    },
    [&](view<count> x, const legacy_count_type&) -> caf::error {
      out = x;
      return {};
    },
    [&](view<time> x, const legacy_time_type&) -> caf::error {
      out = x;
      return {};
    },
    [&](view<duration> x, const legacy_duration_type&) -> caf::error {
      out = x;
      return {};
    },
    [&](view<std::string> x, const legacy_string_type&) -> caf::error {
      out = std::string{x};
      return {};
    },
    [&](view<pattern> x, const legacy_pattern_type&) -> caf::error {
      out = std::string{x.string()};
      return {};
    },
    [&](view<address> x, const legacy_address_type&) -> caf::error {
      auto bytes = as_bytes(x);
      auto ptr = std::launder(reinterpret_cast<const uint32_t*>(bytes.data()));
      out = ::broker::address{ptr, ::broker::address::family::ipv6,
                              ::broker::address::byte_order::network};
      return {};
    },
    [&](view<subnet> x, const legacy_subnet_type&) -> caf::error {
      auto bytes = as_bytes(x.network());
      auto ptr = std::launder(reinterpret_cast<const uint32_t*>(bytes.data()));
      auto addr = ::broker::address{ptr, ::broker::address::family::ipv6,
                                    ::broker::address::byte_order::network};
      out = ::broker::subnet(addr, x.length());
      return {};
    },
    [&](view<enumeration> x, const legacy_enumeration_type& y) -> caf::error {
      if (x >= y.fields.size())
        return caf::make_error(ec::invalid_argument, "enum out of bounds");
      out = ::broker::enum_value{y.fields[x]};
      return {};
    },
    [&](view<list> xs, const legacy_list_type& l) -> caf::error {
      ::broker::vector result;
      result.resize(xs.size());
      for (size_t i = 0; i < result.size(); ++i)
        if (auto err = convert(xs->at(i), result[i], l.value_type))
          return err;
      out = std::move(result);
      return {};
    },
    [&](view<map> xs, const legacy_map_type& m) -> caf::error {
      ::broker::table result;
      for (size_t i = 0; i < result.size(); ++i) {
        ::broker::data key;
        ::broker::data value;
        auto [key_view, value_view] = xs->at(i);
        if (auto err = convert(key_view, key, m.key_type))
          return err;
        if (auto err = convert(value_view, value, m.value_type))
          return err;
        result.emplace(std::move(key), std::move(value));
      }
      out = std::move(result);
      return {};
    },
    [&](view<record>, const legacy_record_type&) -> caf::error {
      return caf::make_error(ec::invalid_argument, //
                             "records must be flattened");
    },
  };
  return caf::visit(f, in, field_type);
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
        return {};
      },
    };
    if (auto err = caf::visit(f, x))
      return err;
  }
  // Ship data to Zeek via Broker, one event per row.
  for (size_t row = 0; row < slice.rows(); ++row) {
    ::broker::vector xs;
    const auto& layout = slice.layout();
    auto columns = layout.fields.size();
    xs.reserve(columns);
    auto flat_layout = flatten(layout);
    for (size_t col = 0; col < columns; ++col) {
      ::broker::data x;
      auto& field_type = flat_layout.fields[col].type;
      if (auto err = convert(slice.at(row, col), x, field_type))
        return err;
      xs.push_back(std::move(x));
    }
    ::broker::vector args(2);
    args[0] = layout.name();
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
