//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/error.hpp>
#include <caf/expected.hpp>

#include <memory>
#include <rdkafkacpp.h>
#include <string>
#include <vector>

namespace vast::plugins::kafka {

class configuration {
  friend class producer;
  friend class consumer;

public:
  static auto defaults() -> std::vector<std::pair<std::string, std::string>>;

  static auto
  make(const std::vector<std::pair<std::string, std::string>>& options
       = defaults()) -> caf::expected<configuration>;

  auto get(std::string_view key) const -> caf::expected<std::string>;

  auto set(const std::string& key, const std::string& value) -> caf::error;

  auto set(const std::vector<std::pair<std::string, std::string>>& options)
    -> caf::error;

private:
  configuration();

  std::shared_ptr<RdKafka::Conf> conf_{};
};

} // namespace vast::plugins::kafka
