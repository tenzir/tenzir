//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <fmt/color.h>

#include <cstdint>
#include <optional>

namespace tenzir {

struct json_style {
  fmt::text_style null_;
  fmt::text_style false_;
  fmt::text_style true_;
  fmt::text_style number;
  fmt::text_style string;
  fmt::text_style array;
  fmt::text_style object;
  fmt::text_style field;
  fmt::text_style comma;
  fmt::text_style duration;
  fmt::text_style time;
  fmt::text_style subnet;
  fmt::text_style ip;
  fmt::text_style blob;
  fmt::text_style colon;
};

// Defined in
// https://github.com/jqlang/jq/blob/c99981c5b2e7e7d4d6d1463cf564bb99e9f18ed9/src/jv_print.c#L27
auto jq_style() -> json_style;

auto tql_style() -> json_style;

auto no_style() -> json_style;

auto default_style() -> json_style;

struct json_printer_options {
  /// The number of spaces used for indentation.
  uint8_t indentation = 2;

  /// Print TQL style
  /// * Duration and time are first class types
  /// * Keys are not quoted, unless they contain whitespace, keywords or other
  ///   special symbols
  /// * Trailing commas by default
  bool tql = false;

  /// Colorize the output like `jq`.
  json_style style = default_style();

  /// Print NDJSON rather than JSON.
  bool oneline = false;

  /// Disables trailing commas for TQL style output
  std::optional<bool> trailing_commas = {};

  /// Print numeric rather than human-readable durations.
  bool numeric_durations = false;

  /// Omit null null fields in records
  bool omit_null_fields = false;

  /// Omit null null values in lists
  bool omit_nulls_in_lists = false;

  /// Omit empty records when printing.
  bool omit_empty_records = false;

  /// Omit empty lists when printing.
  bool omit_empty_lists = false;

  /// Omit empty maps when printing.
  /// TODO: Remove this when removing the import command.
  bool omit_empty_maps = false;
};

} // namespace tenzir
