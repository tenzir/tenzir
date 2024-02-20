//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/printable/tenzir/json_printer_options.hpp"

#include "tenzir/detail/env.hpp"

namespace tenzir {

auto jq_style() -> json_style {
  return {
    .null_ = fmt::emphasis::bold | fg(fmt::terminal_color::black),
    .false_ = fg(fmt::terminal_color::white),
    .true_ = fg(fmt::terminal_color::white),
    .number = fg(fmt::terminal_color::white),
    .string = fg(fmt::terminal_color::green),
    .array = fmt::emphasis::bold | fg(fmt::terminal_color::white),
    .object = fmt::emphasis::bold | fg(fmt::terminal_color::white),
    .field = fmt::emphasis::bold | fg(fmt::terminal_color::blue),
    .comma = fmt::emphasis::bold | fg(fmt::terminal_color::white),
  };
}

auto no_style() -> json_style {
  return {};
}

auto default_style() -> json_style {
  // See https://no-color.org.
  if (detail::getenv("NO_COLOR").value_or("").empty()) {
    return no_style();
  }
  // TODO: Let the saver detect a default style, depending on
  // whether we're outputting data in a TTY or not.
  return no_style();
}

} // namespace tenzir
