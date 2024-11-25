//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/configuration.hpp"

#include <tenzir/concept/printable/tenzir/data.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/die.hpp>
#include <tenzir/error.hpp>

#include <fmt/format.h>

namespace tenzir::plugins::kafka {

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
    if (auto err = set(key, tenzir::match(value, stringify))) {
      return err;
    }
  return {};
}

auto configuration::set_rebalance_cb(int64_t offset) -> caf::error {
  rebalance_callback_ = std::make_shared<rebalancer>(offset);
  std::string error;
  auto result = conf_->set("rebalance_cb", rebalance_callback_.get(), error);
  if (result != RdKafka::Conf::ConfResult::CONF_OK)
    return caf::make_error(ec::unspecified, "failed to set rebalance_cb: {}",
                           error);
  return {};
}

configuration::rebalancer::rebalancer(int offset) : offset_{offset} {
}

auto configuration::rebalancer::rebalance_cb(
  RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err,
  std::vector<RdKafka::TopicPartition*>& partitions) -> void {
  // This branching logic comes from the librdkafka consumer example. See the
  // implementation of ExampleRebalanceCb for details. The only thing we added
  // is the offset assignment at the beginning.
  if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
    if (offset_ != RdKafka::Topic::OFFSET_INVALID) {
      TENZIR_DEBUG("setting offset to {}", offset_);
      for (auto* partition : partitions)
        partition->set_offset(offset_);
    }
    if (consumer->rebalance_protocol() == "COOPERATIVE") {
      if (auto err = consumer->incremental_assign(partitions)) {
        TENZIR_ERROR("failed to assign incrementally: {}", err->str());
        delete err;
      };
    } else {
      auto err = consumer->assign(partitions);
      if (err != RdKafka::ERR_NO_ERROR)
        TENZIR_ERROR("failed to assign partitions: {}", RdKafka::err2str(err));
    }
  } else if (err == RdKafka::ERR__REVOKE_PARTITIONS) {
    // Application may commit offsets manually here
    // if auto.commit.enable=false
    if (consumer->rebalance_protocol() == "COOPERATIVE") {
      if (auto err = consumer->incremental_unassign(partitions)) {
        TENZIR_ERROR("failed to unassign incrementally: {}", err->str());
        delete err;
      };
    } else {
      auto err = consumer->unassign();
      if (err != RdKafka::ERR_NO_ERROR)
        TENZIR_ERROR("failed to unassign partitions: {}",
                     RdKafka::err2str(err));
    }
  } else {
    TENZIR_ERROR("rebalancing error: {}", RdKafka::err2str(err));
    auto err = consumer->unassign();
    if (err != RdKafka::ERR_NO_ERROR)
      TENZIR_ERROR("failed to unassign partitions: {}", RdKafka::err2str(err));
  }
}

configuration::configuration() {
  conf_.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (!conf_)
    die("RdKafka::Conf::create");
}

} // namespace tenzir::plugins::kafka
