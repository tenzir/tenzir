//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"

#include "tenzir/detail/backtrace.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/option.hpp"
#include "tenzir/ref.hpp"
#include "tenzir/shared_diagnostic_handler.hpp"
#include "tenzir/source.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/functional/hash.hpp>
#include <caf/message_handler.hpp>
#include <fmt/color.h>

#include <iostream>
#include <span>
#include <string_view>
#include <unordered_map>

namespace tenzir {

namespace {

void trim_and_truncate(std::string& str) {
  using namespace std::string_view_literals;
  boost::trim(str);
  if (str.size() > 100000) {
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
  diagnostic_printer(SourceMap const& source_map, color_diagnostics color,
                     std::ostream& stream)
    : colors{colors::make(color)}, source_map_{source_map}, stream_{stream} {
  }

  void emit(diagnostic diag) override {
    if (not std::exchange(first, false)) {
      fmt::print(stream_, "\n");
    }
    // TODO: Do not print the same line multiple times. Merge annotations instead.
    fmt::print(stream_, "{}{}{}{}: {}{}\n", bold, color(diag.severity),
               diag.severity, uncolor, diag.message, reset);
    // Sources are resolved against the source map on every emit because the
    // map may change during the printer's lifetime. Sources without pre-split
    // lines are split here; the cache keeps those splits alive for the
    // duration of this call. The views point into the source text, which the
    // `Arc` in the source map keeps stable.
    auto split_cache
      = std::unordered_map<SourceId, std::vector<std::string_view>>{};
    auto resolve = [&](SourceId id) -> Option<ResolvedSource> {
      auto source = source_map_->source(id);
      if (not source) {
        return None{};
      }
      if (source->lines) {
        return ResolvedSource{&*source, *source->lines};
      }
      auto [it, inserted] = split_cache.try_emplace(id);
      if (inserted) {
        it->second = detail::split(source->text, "\n");
      }
      return ResolvedSource{&*source, it->second};
    };
    // Resolve the call-site chain of the primary location, so that we can
    // show where the failing operator was called from.
    auto call_chain = std::vector<location>{};
    for (auto& annotation : diag.annotations) {
      if (not annotation.primary or not annotation.source) {
        continue;
      }
      auto id = annotation.source.callsite_index;
      while (id != 0) {
        auto call_site = source_map_->call_site(id);
        if (not call_site) {
          break;
        }
        call_chain.push_back(*call_site);
        id = call_site->callsite_index;
        // Defensive limit in case of malformed (cyclic) call-site data.
        if (call_chain.size() > 100) {
          break;
        }
      }
      break;
    }
    auto indent_width = size_t{0};
    for (auto& annotation : diag.annotations) {
      if (not annotation.source) {
        continue;
      }
      auto src = resolve(annotation.source.source_index);
      if (not src) {
        continue;
      }
      if (auto lc = line_col_indices(src->lines, annotation.source.begin)) {
        auto [line, col] = *lc;
        indent_width = std::max(indent_width, std::to_string(line + 1).size());
      }
    }
    for (auto& call_site : call_chain) {
      auto src = resolve(call_site.source_index);
      if (not src) {
        continue;
      }
      if (auto lc = line_col_indices(src->lines, call_site.begin)) {
        auto [line, col] = *lc;
        indent_width = std::max(indent_width, std::to_string(line + 1).size());
      }
    }
    auto indent = std::string(indent_width, ' ');
    for (auto& annotation : diag.annotations) {
      if (not annotation.source) {
        TENZIR_VERBOSE("annotation does not have source: {:?}", annotation);
        continue;
      }
      auto src = resolve(annotation.source.source_index);
      if (not src) {
        continue;
      }
      auto lc = line_col_indices(src->lines, annotation.source.begin);
      if (not lc) {
        // Source offset is beyond the available source text. This can
        // happen when diagnostics reference a modified definition.
        continue;
      }
      auto [line_idx, col] = *lc;
      auto line = line_idx + 1;
      fmt::print(stream_, "{}{}{}-->{} {}:{}:{}\n", indent, bold, blue, reset,
                 src->source->origin, line, col + 1);
      fmt::print(stream_, "{} {}{}|{}\n", indent, bold, blue, reset);
      fmt::print(stream_, "{}{}{}{} |{} {}\n",
                 std::string(indent_width - std::to_string(line).size(), ' '),
                 bold, blue, line, reset, src->lines[line_idx]);
      // TODO: This doesn't respect multi-line spans.
      auto count = std::max(uint32_t{1},
                            annotation.source.end - annotation.source.begin);
      auto pseudo_severity
        = annotation.primary ? diag.severity : severity::note;
      fmt::print(stream_, "{} {}{}| {}{}{} {}{}\n", indent, bold, blue,
                 color(pseudo_severity), std::string(col, ' '),
                 std::string(count, symbol(pseudo_severity)), annotation.text,
                 reset);
      fmt::print(stream_, "{} {}{}|{}\n", indent, bold, blue, reset);
    }
    for (auto& call_site : call_chain) {
      auto src = resolve(call_site.source_index);
      if (not src) {
        continue;
      }
      auto lc = line_col_indices(src->lines, call_site.begin);
      if (not lc) {
        continue;
      }
      auto [line_idx, col] = *lc;
      auto line = line_idx + 1;
      fmt::print(stream_, "{}{}{}-->{} {}:{}:{}\n", indent, bold, blue, reset,
                 src->source->origin, line, col + 1);
      fmt::print(stream_, "{} {}{}|{}\n", indent, bold, blue, reset);
      fmt::print(stream_, "{}{}{}{} |{} {}\n",
                 std::string(indent_width - std::to_string(line).size(), ' '),
                 bold, blue, line, reset, src->lines[line_idx]);
      // TODO: This doesn't respect multi-line spans.
      auto count = std::max(uint32_t{1}, call_site.end - call_site.begin);
      fmt::print(stream_, "{} {}{}| {}{}{} called from here{}\n", indent, bold,
                 blue, color(severity::note), std::string(col, ' '),
                 std::string(count, symbol(severity::note)), reset);
      fmt::print(stream_, "{} {}{}|{}\n", indent, bold, blue, reset);
    }
    for (auto& note : diag.notes) {
      auto lines = detail::split(note.message, "\n");
      for (auto& line : lines) {
        if (&line == &lines.front()) {
          fmt::print(stream_, "{} {}{}={} {}:{} {}\n", indent, bold, blue,
                     uncolor, note.kind, reset, line);
        } else {
          auto kind_spaces = std::string(fmt::to_string(note.kind).size(), ' ');
          fmt::print(stream_, "{}   {}  {}\n", indent, kind_spaces, line);
        }
      }
    }
    if (diag.severity == severity::error) {
      error_ = true;
    }
  }

private:
  /// A source resolved for the duration of a single `emit()` call.
  struct ResolvedSource {
    const Source* source;
    std::span<const std::string_view> lines;
  };

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

  /// Returned indices are zero-based. Returns nullopt if the offset is
  /// beyond the end of the source text.
  static auto
  line_col_indices(std::span<const std::string_view> lines, size_t offset)
    -> std::optional<std::pair<size_t, size_t>> {
    auto line = size_t{0};
    auto col = offset;
    while (true) {
      if (line >= lines.size()) {
        return std::nullopt;
      }
      if (col <= lines[line].size()) {
        break;
      }
      col -= lines[line].size() + 1;
      line += 1;
    }
    return std::pair{line, col};
  }

  bool first = true;
  bool error_ = false;
  Ref<const SourceMap> source_map_;
  std::ostream& stream_;
};

} // namespace

diagnostic_annotation::diagnostic_annotation(bool primary, std::string text,
                                             location source)
  : primary{primary}, text{std::move(text)}, source{source} {
  trim_and_truncate(this->text);
}

diagnostic_note::diagnostic_note(diagnostic_note_kind kind, std::string message)
  : kind{kind}, message{std::move(message)} {
  trim_and_truncate(this->message);
}

auto make_diagnostic_printer(SourceMap const& source_map,
                             color_diagnostics color, std::ostream& stream)
  -> std::unique_ptr<diagnostic_handler> {
  return std::make_unique<diagnostic_printer>(source_map, color, stream);
}

auto make_diagnostic_printer(color_diagnostics color, std::ostream& stream)
  -> std::unique_ptr<diagnostic_handler> {
  static const auto empty = SourceMap{};
  return std::make_unique<diagnostic_printer>(empty, color, stream);
}

auto diagnostic::builder(enum severity s, caf::error err,
                         std::source_location location) -> diagnostic_builder {
  if (err.category() == caf::type_id_v<tenzir::ec>
      and static_cast<tenzir::ec>(err.code()) == ec::diagnostic) {
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
    return builder(s, "{}", err)
      .note("source: {}:{}", location.file_name(), location.line());
  }
  return builder(s, "{}", *as_string(err.context().size() - 1))
    .compose([&](auto b) {
      for (auto i = err.context().size() - 1; i > 0; --i) {
        b = std::move(b).note("{}", *as_string(i - 1));
      }
      return b;
    })
    .note("source: {}:{}", location.file_name(), location.line());
}

void diagnostic_builder::emit(const shared_diagnostic_handler& diag) and {
  return diag.emit(std::move(result_));
}

auto diagnostic_deduplicator::insert(const diagnostic& d) -> bool {
  // We remember whether we have seen a diagnostic by storing its main message
  // and the locations of its annotations.
  // TODO: Improve this.
  auto locations = std::vector<location>{};
  for (auto& annotation : d.annotations) {
    locations.push_back(annotation.source);
  }
  auto inserted
    = seen_.emplace(std::pair{d.message, std::move(locations)}).second;
  return inserted;
}

void diagnostic_deduplicator::clear() {
  seen_.clear();
}

auto diagnostic_deduplicator::hasher::operator()(const seen_t& x) const
  -> size_t {
  auto result = std::hash<std::string>{}(x.first);
  for (auto& loc : x.second) {
    boost::hash_combine(result, loc.begin);
    boost::hash_combine(result, loc.end);
  }
  return result;
}

auto to_diagnostic(const panic_exception& e) -> diagnostic {
  auto note = std::string{
    "this is a bug, we would appreciate a report - thank you!\n"
    "=> "
    "https://github.com/orgs/tenzir/discussions/new?category=bug-reports\n\n",
  };
  fmt::format_to(std::back_inserter(note), "version: v{}\n",
                 tenzir::version::version);
  fmt::format_to(std::back_inserter(note), "source: {}:{}\n\n",
                 e.location.file_name(), e.location.line());
  for (auto& frame : e.stacktrace) {
    note += detail::format_frame(frame) + "\n";
  }
  return diagnostic::error("unexpected internal error: {}", e.message)
    .primary(location{detail::narrow_cast<uint32_t>(e.trace.begin),
                      detail::narrow_cast<uint32_t>(e.trace.end)})
    .note(std::move(note))
    .done();
}
} // namespace tenzir
