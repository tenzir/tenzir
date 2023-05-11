//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <caf/expected.hpp>

#include <string_view>

namespace vast {

auto to_xsv_sep(std::string_view x) -> caf::expected<char>;

} // namespace vast
