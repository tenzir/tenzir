//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"

#include "tenzir/detail/string.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/shared_diagnostic_handler.hpp"

#include <boost/algorithm/string.hpp>
#include <caf/message_handler.hpp>
#include <fmt/color.h>

#include <iostream>
#include <string_view>

namespace tenzir {

namespace {

void trim_and_truncate(std::string& str) {
  using namespace std::string_view_literals;
  boost::trim(str);
  if (str.size() > 2000) {
    auto prefix = std::string_view{str.begin(), str.begin() + 75};
    str = fmt::format("{} ... (truncated {} bytes)", prefix,
                      str.length() - prefix.length());
  }
}

struct colors {
  static auto make(color_diagnostics color) -> colors {
    if (color == color_diagnostics::no) {
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
  diagnostic_printer(std::optional<location_origin> origin,
                     color_diagnostics color, std::ostream& stream)
    : colors{colors::make(color)},
      storage_{origin ? std::move(origin->source) : ""},
      lines_{origin ? detail::split(storage_, "\n")
                    : std::vector<std::string_view>{}},
      stream_{stream},
      filename_{origin ? std::move(origin->filename) : ""} {
  }

  void emit(diagnostic diag) override {
    if (not std::exchange(first, false)) {
      fmt::print("\n");
    }
    // TODO: Do not print the same line multiple times. Merge annotations instead.
    fmt::print(stream_, "{}{}{}{}: {}{}\n", bold, color(diag.severity),
               diag.severity, uncolor, diag.message, reset);
    auto indent_width = size_t{0};
    for (auto& annotation : diag.annotations) {
      if (lines_.empty()) {
        // TODO: This is a hack for the case where we don't have the information.
        break;
      }
      auto [line, col] = line_col_indices(annotation.source.begin);
      indent_width = std::max(indent_width, std::to_string(line + 1).size());
    }
    auto indent = std::string(indent_width, ' ');
    for (auto& annotation : diag.annotations) {
      if (lines_.empty()) {
        // TODO: This is a hack for the case where we don't have the information.
        break;
      }
      if (!annotation.source) {
        TENZIR_VERBOSE("annotation does not have source: {:?}", annotation);
        continue;
      }
      auto [line_idx, col] = line_col_indices(annotation.source.begin);
      auto line = line_idx + 1;
      if (&annotation == &diag.annotations.front()) {
        fmt::print(stream_, "{}{}{}-->{} {}:{}:{}\n", indent, bold, blue, reset,
                   filename_, line, col + 1);
        fmt::print(stream_, "{} {}{}|{}\n", indent, bold, blue, reset);
      } else {
        fmt::print(stream_, "{} {}{}⋮{}\n", indent, bold, blue, reset);
      }
      fmt::print(stream_, "{}{}{}{} |{} {}\n",
                 std::string(indent_width - std::to_string(line).size(), ' '),
                 bold, blue, line, reset, lines_[line_idx]);
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
    TENZIR_UNREACHABLE();
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
    TENZIR_UNREACHABLE();
  }

  /// Returned indices are zero-based.
  auto line_col_indices(size_t offset) -> std::pair<size_t, size_t> {
    auto line = size_t{0};
    auto col = offset;
    while (true) {
      TENZIR_ASSERT(line < lines_.size());
      if (col <= lines_[line].size()) {
        break;
      }
      col -= lines_[line].size() + 1;
      line += 1;
    }
    return {line, col};
  }

  bool first = true;
  bool error_ = false;
  std::string storage_;
  std::vector<std::string_view> lines_;
  std::ostream& stream_;
  std::string filename_;
};

} // namespace

diagnostic_annotation::diagnostic_annotation(bool primary, std::string text,
                                             location source)
  : primary{primary}, text{std::move(text)}, source{std::move(source)} {
  trim_and_truncate(this->text);
}

diagnostic_note::diagnostic_note(diagnostic_note_kind kind, std::string message)
  : kind{kind}, message{std::move(message)} {
  trim_and_truncate(this->message);
}

auto make_diagnostic_printer(std::optional<location_origin> origin,
                             color_diagnostics color, std::ostream& stream)
  -> std::unique_ptr<diagnostic_handler> {
  return std::make_unique<diagnostic_printer>(std::move(origin), color, stream);
}

auto diagnostic::builder(enum severity s, caf::error err)
  -> diagnostic_builder {
  if (err.category() == caf::type_id_v<tenzir::ec>
      && static_cast<tenzir::ec>(err.code()) == ec::diagnostic) {
    auto ctx = err.context();
    auto* inner = static_cast<diagnostic*>(nullptr);
    caf::message_handler{
      [&](diagnostic& diag) {
        inner = &diag;
      },
      [](const caf::message&) {},
    }(ctx);
    if (inner) {
      return std::move(*inner).modify().severity(s);
    }
  }
  auto as_string = [&](size_t i) -> std::optional<std::string_view> {
    if (err.context().match_element<std::string>(i)) {
      return err.context().get_as<std::string>(i);
    }
    return std::nullopt;
  };
  auto eligible = err.context().size() != 0;
  for (auto i = size_t{0}; i < err.context().size(); ++i) {
    if (not as_string(i)) {
      eligible = false;
      break;
    }
  }
  if (not eligible) {
    return builder(s, "{}", err);
  }
  auto b = builder(s, "{}", *as_string(err.context().size() - 1));
  for (auto i = err.context().size() - 1; i > 0; --i) {
    b = std::move(b).note("{}", *as_string(i - 1));
  }
  return b;
}

void diagnostic_builder::emit(const shared_diagnostic_handler& diag) && {
  return diag.emit(std::move(result_));
}

} // namespace tenzir
