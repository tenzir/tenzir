//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <boost/unordered/unordered_flat_map.hpp>

namespace tenzir {

extern boost::unordered_flat_map<
  std::string_view, boost::unordered_flat_map<int64_t, std::string_view>>
  ocsf_enums;

} // namespace tenzir
