//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"

#include <caf/scheduled_actor.hpp>

namespace tenzir {

/// An inspectable version of `std::source_location`.
struct source_location {
public:
  source_location() = default;

  explicit source_location(std::source_location loc)
    : file_{loc.file_name()},
      function_{loc.function_name()},
      line_{loc.line()} {
  }

  auto file() const -> std::string_view {
    return file_;
  }

  auto function() const -> std::string_view {
    return function_;
  }

  auto line() const -> uint64_t {
    return line_;
  }

  friend auto inspect(auto& f, source_location& x) -> bool {
    return f.object(x).fields(f.field("file", x.file_),
                              f.field("function", x.function_),
                              f.field("line", x.line_));
  }

private:
  // TODO: Most of the time this can just be a `char const*`.
  std::string file_;
  std::string function_;
  uint64_t line_{};
};

/// A report captures an unexpected CAF error with a backtrace.
class report {
public:
  report() = default;

  explicit report(caf::error error) : error{std::move(error)} {
  }

  friend auto inspect(auto& f, report& x) -> bool {
    return f.object(x).fields(f.field("error", x.error),
                              f.field("backtrace", x.backtrace));
  }

  // TODO: Public.
  caf::error error;
  /// Backtrace in reverse order (outermost last).
  std::vector<source_location> backtrace;
};

inline auto make_report(caf::error err, std::source_location location
                                        = std::source_location::current())
  -> report {
  auto is_report = err.category() == caf::type_id_v<tenzir::ec>
                   and static_cast<tenzir::ec>(err.code()) == ec::report;
  auto result = report{};
  if (is_report) {
    // Try to get unique ownership of the context.
    auto context = err.context();
    err.reset();
    result = context.get_mutable_as<report>(0);
  } else {
    result = report{std::move(err)};
  }
  result.backtrace.emplace_back(location);
  return result;
}

inline auto
make_report_error(caf::error err, std::source_location location
                                  = std::source_location::current()) {
  return caf::make_error(ec::report, make_report(std::move(err), location));
}

inline auto make_quit_with_report(caf::scheduled_actor* self,
                                  std::source_location location
                                  = std::source_location::current()) {
  return [self, location](caf::error& err) {
    TENZIR_ERROR("{} quits due to unexpected error at {}:{}", self->name(),
                 location.file_name(), location.line());
    self->quit(make_report_error(std::move(err), location));
  };
}

#define TENZIR_REPORT make_quit_with_report(self_)

} // namespace tenzir
