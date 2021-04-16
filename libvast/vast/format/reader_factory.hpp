//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/factory.hpp"
#include "vast/format/reader.hpp"

#include <functional>
#include <string>

namespace vast {

template <>
struct factory_traits<format::reader> {
  using result_type = caf::expected<format::reader_ptr>;
  using key_type = std::string;
  using signature = std::function<result_type(const caf::settings&)>;

  static void initialize();
};

} // namespace vast
