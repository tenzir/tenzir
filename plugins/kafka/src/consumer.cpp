//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/consumer.hpp"

#include <vast/detail/narrow.hpp>
#include <vast/error.hpp>

#include <fmt/format.h>

namespace vast::plugins::kafka {

auto consumer::make(configuration config) -> caf::expected<consumer> {
  consumer result;
  std::string error;
  result.consumer_.reset(
    RdKafka::KafkaConsumer::create(config.conf_.get(), error));
  if (!result.consumer_)
    return caf::make_error(ec::unspecified, error);
  result.config_ = std::move(config);
  return result;
}

auto consumer::subscribe(const std::vector<std::string>& topics) -> caf::error {
  auto result = consumer_->subscribe(topics);
  if (result != RdKafka::ERR_NO_ERROR)
    return caf::make_error(ec::unspecified, RdKafka::err2str(result));
  return {};
}

auto consumer::consume(std::chrono::milliseconds timeout)
  -> caf::expected<chunk_ptr> {
  auto ms = detail::narrow_cast<int>(timeout.count());
  auto* ptr = consumer_->consume(ms);
  auto result = chunk::make(ptr->payload(), ptr->len(), [ptr]() noexcept {
    delete ptr; // NOLINT
  });
  switch (ptr->err()) {
    case RdKafka::ERR_NO_ERROR:
      break;
    case RdKafka::ERR__TIMED_OUT:
      return ec::timeout;
    case RdKafka::ERR__PARTITION_EOF:
      return ec::end_of_input;
    default:
      return caf::make_error(ec::unspecified,
                             fmt::format("failed to consume message: {} ({})",
                                         ptr->errstr(),
                                         static_cast<int>(ptr->err())));
  }
  return result;
}

} // namespace vast::plugins::kafka
