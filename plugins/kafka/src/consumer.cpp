//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/consumer.hpp"

#include <tenzir/detail/narrow.hpp>
#include <tenzir/error.hpp>

#include <fmt/format.h>

namespace tenzir::plugins::kafka {

auto consumer::make(configuration config) -> caf::expected<consumer> {
  consumer result;
  std::string error;
  result.consumer_.reset(
    RdKafka::KafkaConsumer::create(config.conf_.get(), error));
  if (! result.consumer_) {
    return caf::make_error(ec::unspecified, error);
  }
  result.config_ = std::move(config);
  return result;
}

auto consumer::subscribe(const std::vector<std::string>& topics) -> caf::error {
  auto result = consumer_->subscribe(topics);
  if (result != RdKafka::ERR_NO_ERROR) {
    return caf::make_error(ec::unspecified, RdKafka::err2str(result));
  }
  return {};
}

auto consumer::consume_raw(std::chrono::milliseconds timeout)
  -> std::shared_ptr<RdKafka::Message> {
  auto ms = detail::narrow_cast<int>(timeout.count());
  return std::shared_ptr<RdKafka::Message>{consumer_->consume(ms)};
}

auto consumer::commit(RdKafka::Message* message) -> caf::error {
  auto result = consumer_->commitSync(message);
  if (result != RdKafka::ERR_NO_ERROR) {
    return caf::make_error(ec::unspecified,
                           fmt::format("failed to commit message: {}",
                                       RdKafka::err2str(result)));
  }
  return {};
}

auto consumer::commit_async(RdKafka::Message* message) -> caf::error {
  auto result = consumer_->commitAsync(message);
  if (result != RdKafka::ERR_NO_ERROR) {
    return caf::make_error(ec::unspecified,
                           fmt::format("failed to commit message async: {}",
                                       RdKafka::err2str(result)));
  }
  return {};
}

} // namespace tenzir::plugins::kafka
