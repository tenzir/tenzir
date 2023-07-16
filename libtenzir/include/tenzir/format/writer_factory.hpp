//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/factory.hpp"
#include "tenzir/format/writer.hpp"

#include <functional>
#include <string>

namespace tenzir {

template <>
struct factory_traits<format::writer> {
  using result_type = caf::expected<format::writer_ptr>;
  using key_type = std::string;
  using signature = std::function<result_type(const caf::settings&)>;

  static void initialize();
};

} // namespace tenzir
