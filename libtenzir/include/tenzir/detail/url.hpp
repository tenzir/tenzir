//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <boost/url/url.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

template <>
struct fmt::formatter<boost::urls::url> : fmt::ostream_formatter {};

template <>
struct fmt::formatter<boost::urls::url_view> : fmt::ostream_formatter {};
