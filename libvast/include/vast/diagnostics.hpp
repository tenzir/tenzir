//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/assert.hpp"
#include "vast/detail/inspect_enum_str.hpp"
#include "vast/tql/basic.hpp"

#include <fmt/format.h>

#include <functional>

/// Similar to `VAST_ASSERT(...)`, but throws a `diagnostic` instead of
/// aborting. Unlike `VAST_ASSERT(...)`, this assertion is always checked, hence
/// the expression is allowed to have side-effects.
#define VAST_DIAG_ASSERT(x)                                                    \
  do {                                                                         \
    if (!(x)) {                                                                \
      ::vast::diagnostic::error("internal error: assertion `{}` failed at "    \
                                "{}:{}",                                       \
                                #x, __FILE__, __LINE__)                        \
        .throw_();                                                             \
    }                                                                          \
  } while (false)

namespace vast {

class diagnostic_handler {
public:
  virtual ~diagnostic_handler() = default;

  virtual void emit(diagnostic d) = 0;

  virtual auto has_seen_error() const -> bool = 0;
};

enum class severity { error, warning, note };

template <class Inspector>
auto inspect(Inspector& f, severity& x) -> bool {
  return detail::inspect_enum_str(f, x, {"error", "warning", "note"});
}

struct diagnostic_annotation {
  /// True if the source represents the underlying reason for the outer
  /// diagnostic, false if it is only related to it.
  bool primary{};

  /// An message for explanations, can be empty.
  std::string text;

  /// The location that this annotation is associated to, can be unknown.
  location source;

  friend auto inspect(auto& f, diagnostic_annotation& x) -> bool {
    return f.object(x)
      .pretty_name("diagnostic_span")
      .fields(f.field("primary", x.primary), f.field("text", x.text),
              f.field("source", x.source));
  }
};

enum class diagnostic_note_kind {
  /// Generic note, not further specified.
  note,
  /// The usage description for an operator.
  usage,
  /// Recommendation on how to solve the problem.
  hint,
  /// Link to the associated documentation.
  docs
};

template <class Inspector>
auto inspect(Inspector& f, diagnostic_note_kind& x) -> bool {
  return detail::inspect_enum_str(f, x, {"note", "usage", "hint", "docs"});
}

/// Additional information related to a parent diagnostic.
struct diagnostic_note {
  /// The type of this note.
  diagnostic_note_kind kind;

  /// The (required) message of this note.
  std::string message;

  // In the future, we could allow annotations here as well.
  // std::vector<diagnostic_annotation> annotations;

  friend auto inspect(auto& f, diagnostic_note& x) -> bool {
    return f.object(x)
      .pretty_name("diagnostic_note")
      .fields(f.field("kind", x.kind), f.field("message", x.message));
  }
};

/// A structured representation of a compiler diagnostic.
struct [[nodiscard]] diagnostic {
  /// The severity of the diagnostic.
  enum severity severity;

  /// Description of the diagnostic, should not be empty.
  std::string message;

  /// Annotations that are directly related to the message.
  std::vector<diagnostic_annotation> annotations;

  /// Additional notes which have their own message.
  std::vector<diagnostic_note> notes;

  template <class... Ts>
  static auto
  builder(enum severity s, fmt::format_string<Ts...> str, Ts&&... xs)
    -> diagnostic_builder;

  template <class... Ts>
  static auto error(fmt::format_string<Ts...> str, Ts&&... xs) {
    return builder(severity::error, std::move(str), std::forward<Ts>(xs)...);
  }

  template <class... Ts>
  static auto warning(fmt::format_string<Ts...> str, Ts&&... xs) {
    return builder(severity::warning, std::move(str), std::forward<Ts>(xs)...);
  }

  auto modify() && -> diagnostic_builder;

  template <class Inspector>
  friend auto inspect(Inspector& f, diagnostic& x) -> bool {
    return f.object(x)
      .pretty_name("diagnostic")
      .fields(f.field("severity", x.severity), f.field("message", x.message),
              f.field("annotations", x.annotations), f.field("notes", x.notes));
  }
};

/// Utility class to construct a `diagnostic`.
class [[nodiscard]] diagnostic_builder {
public:
  explicit diagnostic_builder(diagnostic start) : result_{std::move(start)} {
  }

  diagnostic_builder(enum severity severity, std::string message)
    : result_{severity, std::move(message), {}, {}} {
  }

  // -- annotations -----------------------------------------------------------

  auto
  primary(location source, std::string text = "") && -> diagnostic_builder {
    result_.annotations.push_back(
      diagnostic_annotation{true, std::move(text), source});
    return std::move(*this);
  }

  template <class... Ts>
    requires(sizeof...(Ts) > 0)
  auto primary(location source, fmt::format_string<Ts...> str,
               Ts&&... xs) && -> diagnostic_builder {
    return std::move(*this).primary(
      source, fmt::format(std::move(str), std::forward<Ts>(xs)...));
  }

  auto
  secondary(location source, std::string text = "") && -> diagnostic_builder {
    result_.annotations.push_back(
      diagnostic_annotation{false, std::move(text), source});
    return std::move(*this);
  }

  template <class... Ts>
    requires(sizeof...(Ts) > 0)
  auto secondary(location source, fmt::format_string<Ts...> str,
                 Ts&&... xs) && -> diagnostic_builder {
    return std::move(*this).secondary(
      source, fmt::format(std::move(str), std::forward<Ts>(xs)...));
  }

  // -- notes -----------------------------------------------------------------

  auto note(std::string str) && -> diagnostic_builder {
    result_.notes.push_back(
      diagnostic_note{diagnostic_note_kind::note, std::move(str)});
    return std::move(*this);
  }

  template <class... Ts>
    requires(sizeof...(Ts) > 0)
  auto
  note(fmt::format_string<Ts...> str, Ts&&... xs) && -> diagnostic_builder {
    return std::move(*this).note(
      fmt::format(std::move(str), std::forward<Ts>(xs)...));
  }

  auto docs(std::string str) && -> diagnostic_builder {
    result_.notes.push_back(
      diagnostic_note{diagnostic_note_kind::docs, std::move(str)});
    return std::move(*this);
  }

  template <class... Ts>
    requires(sizeof...(Ts) > 0)
  auto
  docs(fmt::format_string<Ts...> str, Ts&&... xs) && -> diagnostic_builder {
    return std::move(*this).docs(
      fmt::format(std::move(str), std::forward<Ts>(xs)...));
  }

  auto usage(std::string str) && -> diagnostic_builder {
    result_.notes.push_back(
      diagnostic_note{diagnostic_note_kind::usage, std::move(str)});
    return std::move(*this);
  }

  template <class... Ts>
    requires(sizeof...(Ts) > 0)
  auto
  usage(fmt::format_string<Ts...> str, Ts&&... xs) && -> diagnostic_builder {
    return std::move(*this).usage(
      fmt::format(std::move(str), std::forward<Ts>(xs)...));
  }

  auto hint(std::string str) && -> diagnostic_builder {
    result_.notes.push_back(
      diagnostic_note{diagnostic_note_kind::hint, std::move(str)});
    return std::move(*this);
  }

  template <class... Ts>
    requires(sizeof...(Ts) > 0)
  auto
  hint(fmt::format_string<Ts...> str, Ts&&... xs) && -> diagnostic_builder {
    return std::move(*this).hint(
      fmt::format(std::move(str), std::forward<Ts>(xs)...));
  }

  // -- finalizing ------------------------------------------------------------

  auto done() && -> diagnostic {
    return std::move(result_);
  }

  void emit(diagnostic_handler& diag) && {
    diag.emit(std::move(result_));
  }

  [[noreturn]] void throw_() && {
    throw std::move(result_);
  }

private:
  diagnostic result_;
};

template <class... Ts>
auto diagnostic::builder(enum severity s, fmt::format_string<Ts...> str,
                         Ts&&... xs) -> diagnostic_builder {
  return diagnostic_builder{s, fmt::format(std::move(str),
                                           std::forward<Ts>(xs)...)};
}

inline auto diagnostic::modify() && -> diagnostic_builder {
  return diagnostic_builder{std::move(*this)};
}

class null_diagnostic_handler final : public diagnostic_handler {
public:
  void emit(diagnostic diag) override {
    has_seen_error_ |= diag.severity == severity::error;
  }

  auto has_seen_error() const -> bool override {
    return has_seen_error_;
  }

private:
  bool has_seen_error_ = false;
};

class collecting_diagnostic_handler final : public diagnostic_handler {
public:
  void emit(diagnostic diag) override {
    has_seen_error_ |= diag.severity == severity::error;
    result.push_back(std::move(diag));
  }

  auto has_seen_error() const -> bool override {
    return has_seen_error_;
  }

  auto collect() && -> std::vector<diagnostic> {
    return std::move(result);
  }

private:
  std::vector<diagnostic> result;
  bool has_seen_error_ = false;
};

///
auto make_diagnostic_printer(std::string filename, std::string source,
                             bool color, std::ostream& stream)
  -> std::unique_ptr<diagnostic_handler>;

} // namespace vast

template <>
struct fmt::formatter<vast::severity> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::severity& x, FormatContext& ctx) const {
    using enum vast::severity;
    return fmt::format_to(ctx.out(), "{}", std::invoke([&] {
                            switch (x) {
                              case error:
                                return "error";
                              case warning:
                                return "warning";
                              case note:
                                return "note";
                            }
                            VAST_UNREACHABLE();
                          }));
  }
};

template <>
struct fmt::formatter<vast::diagnostic_note_kind> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::diagnostic_note_kind& x, FormatContext& ctx) const {
    using enum vast::diagnostic_note_kind;
    return fmt::format_to(ctx.out(), "{}", std::invoke([&] {
                            switch (x) {
                              case note:
                                return "note";
                              case usage:
                                return "usage";
                              case hint:
                                return "hint";
                              case docs:
                                return "docs";
                            }
                            VAST_UNREACHABLE();
                          }));
  }
};

template <>
struct fmt::formatter<vast::diagnostic_annotation> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::diagnostic_annotation& x, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{{primary: {}, text: {:?}, source: {}}}",
                          x.primary, x.text, x.source);
  }
};

template <>
struct fmt::formatter<vast::diagnostic_note> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::diagnostic_note& x, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{{kind: {}, message: {:?}}}", x.kind,
                          x.message);
  }
};

template <>
struct fmt::formatter<vast::diagnostic> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::diagnostic& x, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(),
                          "{{severity: {}, message: {:?}, annotations: [{}], "
                          "notes: [{}]}}",
                          x.severity, x.message, fmt::join(x.annotations, ", "),
                          fmt::join(x.notes, ", "));
  }
};
