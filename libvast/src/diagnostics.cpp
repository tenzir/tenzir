//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/diagnostics.hpp"

#include "vast/detail/string.hpp"
#include "vast/logger.hpp"

#include <fmt/color.h>

#include <iostream>

namespace vast {

namespace {

struct colors {
  static auto make(bool color) -> colors {
    if (!color) {
      return colors{};
    }
    return colors{
      .blue = "\033[34m",
      .green = "\033[32m",
      .red = "\033[31m",
      .yellow = "\033[33m",

      .bold = "\033[1m",
      .reset = "\033[0m",
      .uncolor = "\033[39m",
    };
  }

  std::string_view blue;
  std::string_view green;
  std::string_view red;
  std::string_view yellow;

  std::string_view bold;
  std::string_view reset;
  std::string_view uncolor;
};

class diagnostic_printer final : public diagnostic_handler, private colors {
public:
  diagnostic_printer(std::string filename, std::string source, bool color,
                     std::ostream& stream)
    : colors{colors::make(color)},
      storage_{std::move(source)},
      lines_{detail::split(storage_, "\n")},
      stream_{stream},
      filename_{std::move(filename)} {
  }

  void emit(diagnostic diag) override {
    // TODO: Do not print color if not a console + provide option.
    // TODO: Do not print the same line multiple times. Merge annotations instead.
    fmt::print(stream_, "{}{}{}{}: {}{}\n", bold, color(diag.severity),
               diag.severity, uncolor, diag.message, reset);
    auto indent_width = size_t{0};
    for (auto& annotation : diag.annotations) {
      auto [line, col] = line_col_indices(annotation.source.begin);
      indent_width = std::max(indent_width, std::to_string(line).size());
    }
    auto indent = std::string(indent_width, ' ');
    for (auto& annotation : diag.annotations) {
      if (!annotation.source) {
        VAST_WARN("annotation does not have source: {}", annotation);
        continue;
      }
      auto [line, col] = line_col_indices(annotation.source.begin);
      if (&annotation == &diag.annotations.front()) {
        fmt::print(stream_, "{}{}{}-->{} {}:{}:{}\n", indent, bold, blue, reset,
                   filename_, line + 1, col + 1);
        fmt::print(stream_, "{} {}{}|{}\n", indent, bold, blue, reset);
      } else {
        fmt::print(stream_, "{} {}{}⋮{}\n", indent, bold, blue, reset);
      }
      fmt::print(stream_, "{}{}{}{} |{} {}\n",
                 std::string(indent_width - std::to_string(line).size(), ' '),
                 bold, blue, line + 1, reset, lines_[line]);
      // TODO: This doesn't respect multi-line spans.
      auto count
        = std::max(size_t{1}, annotation.source.end - annotation.source.begin);
      auto pseudo_severity
        = annotation.primary ? diag.severity : severity::note;
      fmt::print(stream_, "{} {}{}| {}{}{} {}{}\n", indent, bold, blue,
                 color(pseudo_severity), std::string(col, ' '),
                 std::string(count, symbol(pseudo_severity)), annotation.text,
                 reset);
      if (&annotation == &diag.annotations.back()) {
        fmt::print(stream_, "{} {}{}|{}\n", indent, bold, blue, reset);
      }
    }
    for (auto& note : diag.notes) {
      fmt::print(stream_, "{} {}{}={} {}:{} {}\n", indent, bold, blue, uncolor,
                 note.kind, reset, note.message);
    }
    if (diag.severity == severity::error) {
      error_ = true;
    }
  }

  auto has_seen_error() const -> bool override {
    return error_;
  }

private:
  static auto symbol(severity s) -> char {
    switch (s) {
      case severity::error:
        return '^';
      case severity::warning:
        return '~';
      case severity::note:
        return '-';
    }
    VAST_UNREACHABLE();
  }

  auto color(severity s) const -> std::string_view {
    switch (s) {
      case severity::error:
        return red;
      case severity::warning:
        return yellow;
      case severity::note:
        return blue;
    }
    VAST_UNREACHABLE();
  }

  /// Returned indices are zero-based.
  auto line_col_indices(size_t offset) -> std::pair<size_t, size_t> {
    auto line = size_t{0};
    auto col = offset;
    while (true) {
      VAST_ASSERT(line < lines_.size());
      if (col <= lines_[line].size()) {
        break;
      }
      col -= lines_[line].size() + 1;
      line += 1;
    }
    return {line, col};
  }

  bool error_ = false;
  std::string storage_;
  std::vector<std::string_view> lines_;
  std::ostream& stream_;
  std::string filename_;
};

} // namespace

auto make_diagnostic_printer(std::string filename, std::string source,
                             bool color, std::ostream& stream)
  -> std::unique_ptr<diagnostic_handler> {
  return std::make_unique<diagnostic_printer>(std::move(filename),
                                              std::move(source), color, stream);
}

} // namespace vast
