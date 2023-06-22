//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/configuration.hpp"

#include <vast/concept/printable/to_string.hpp>
#include <vast/concept/printable/vast/data.hpp>
#include <vast/data.hpp>
#include <vast/detail/overload.hpp>
#include <vast/die.hpp>
#include <vast/error.hpp>

#include <fmt/format.h>

namespace vast::plugins::kafka {

auto configuration::make(const record& options)
  -> caf::expected<configuration> {
  configuration result;
  if (auto err = result.set(options))
    return err;
  return result;
}

auto configuration::get(std::string_view key) const
  -> caf::expected<std::string> {
  std::string value;
  auto result = conf_->get(std::string{key}, value);
  if (result != RdKafka::Conf::ConfResult::CONF_OK)
    return caf::make_error(ec::unspecified,
                           fmt::format("failed to get key: {}", key));
  return value;
}

auto configuration::set(const std::string& key, const std::string& value)
  -> caf::error {
  std::string error;
  auto result = conf_->set(key, value, error);
  switch (result) {
    case RdKafka::Conf::ConfResult::CONF_UNKNOWN:
      return caf::make_error(ec::unspecified,
                             fmt::format("unknown configuration property: {}",
                                         error));
    case RdKafka::Conf::ConfResult::CONF_INVALID:
      return caf::make_error(
        ec::unspecified, fmt::format("invalid configuration value: {}", error));
    case RdKafka::Conf::ConfResult::CONF_OK:
      break;
  }
  return {};
}

auto configuration::set(const record& options) -> caf::error {
  auto stringify = detail::overload{
    [](const auto& x) {
      return to_string(x);
    },
    [](const std::string& x) {
      return x;
    },
  };
  for (const auto& [key, value] : options)
    if (auto err = set(key, caf::visit(stringify, value)))
      return err;
  return {};
}

configuration::configuration() {
  conf_.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (!conf_)
    die("RdKafka::Conf::create");
}

} // namespace vast::plugins::kafka
