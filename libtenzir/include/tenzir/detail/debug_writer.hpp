//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// This file is based upon the JSON writer of CAF, the C++ Actor Framework.

#pragma once

#include "tenzir/detail/assert.hpp"

#include <caf/serializer.hpp>
#include <fmt/format.h>

#include <iterator>
#include <type_traits>
#include <vector>

namespace tenzir {

class debug_writer : public caf::serializer {
private:
  using string_view = caf::string_view;
  using type_id_t = caf::type_id_t;
  using byte = caf::byte;
  template <class T>
  using span = caf::span<T>;

public:
  // -- member types -----------------------------------------------------------

  using super = serializer;

  /// Reflects the structure of JSON objects according to ECMA-404. This enum
  /// skips types such as `members` or `value` since they are not needed to
  /// generate JSON.
  enum class type : uint8_t {
    element, /// Can morph into any other type except `member`.
    object,  /// Contains any number of members.
    member,  /// A single key-value pair.
    key,     /// The key of a field.
    array,   /// Contains any number of elements.
    string,  /// A character sequence (terminal type).
    number,  /// An integer or floating point (terminal type).
    boolean, /// Either "true" or "false" (terminal type).
    null,    /// The literal "null" (terminal type).
  };

  // -- constants --------------------------------------------------------------

  /// The default value for `skip_empty_fields()`.
  static constexpr bool skip_empty_fields_default = true;

  /// The value value for `field_type_suffix()`.
  static constexpr string_view field_type_suffix_default = "-type";

  // -- constructors, destructors, and assignment operators --------------------

  debug_writer();

  // -- properties -------------------------------------------------------------

  /// Returns a string view into the internal buffer.
  /// @warning This view becomes invalid when calling any non-const member
  ///          function on the writer object.
  [[nodiscard]] std::string_view str() const noexcept {
    return {buf_.data(), buf_.size()};
  }

  /// Returns the current indentation factor.
  [[nodiscard]] size_t indentation() const noexcept {
    return indentation_factor_;
  }

  /// Sets the indentation level.
  /// @param factor The number of spaces to add to each level of indentation. A
  ///               value of 0 (the default) disables indentation, printing the
  ///               entire JSON output into a single line.
  void indentation(size_t factor) noexcept {
    indentation_factor_ = factor;
  }

  /// Returns whether the writer generates compact JSON output without any
  /// spaces or newlines to separate values.
  [[nodiscard]] bool compact() const noexcept {
    return indentation_factor_ == 0;
  }

  /// Returns whether the writer omits empty fields entirely (true) or renders
  /// empty fields as `$field: null` (false).
  [[nodiscard]] bool skip_empty_fields() const noexcept {
    return skip_empty_fields_;
  }

  /// Configures whether the writer omits empty fields.
  void skip_empty_fields(bool value) noexcept {
    skip_empty_fields_ = value;
  }

  /// Returns the suffix for generating type annotation fields for variant
  /// fields. For example, CAF inserts field called "@foo${field_type_suffix}"
  /// for a variant field called "foo".
  [[nodiscard]] string_view field_type_suffix() const noexcept {
    return field_type_suffix_;
  }

  /// Configures whether the writer omits empty fields.
  void field_type_suffix(string_view suffix) noexcept {
    field_type_suffix_ = suffix;
  }

  // -- modifiers --------------------------------------------------------------

  /// Removes all characters from the buffer and restores the writer to its
  /// initial state.
  /// @warning Invalidates all string views into the buffer.
  void reset();

  // -- overrides --------------------------------------------------------------

  bool begin_object(type_id_t type, string_view name) override;

  bool end_object() override;

  bool begin_field(string_view name) override;

  bool begin_field(string_view name, bool is_present) override;

  bool begin_field(string_view name, span<const type_id_t> types,
                   size_t index) override;

  bool begin_field(string_view name, bool is_present,
                   span<const type_id_t> types, size_t index) override;

  bool end_field() override;

  bool begin_tuple(size_t size) override;

  bool end_tuple() override;

  bool begin_key_value_pair() override;

  bool end_key_value_pair() override;

  bool begin_sequence(size_t size) override;

  bool end_sequence() override;

  bool begin_associative_array(size_t size) override;

  bool end_associative_array() override;

  bool value(byte x) override;

  bool value(bool x) override;

  bool value(int8_t x) override;

  bool value(uint8_t x) override;

  bool value(int16_t x) override;

  bool value(uint16_t x) override;

  bool value(int32_t x) override;

  bool value(uint32_t x) override;

  bool value(int64_t x) override;

  bool value(uint64_t x) override;

  bool value(float x) override;

  bool value(double x) override;

  bool value(long double x) override;

  bool value(string_view x) override;

  bool value(const std::u16string& x) override;

  bool value(const std::u32string& x) override;

  bool value(span<const byte> x) override;

  // Adds `c` to the output buffer.
  void add(char c) {
    buf_.push_back(c);
  }

  // Adds `str` to the output buffer.
  void add(string_view str) {
    buf_.insert(buf_.end(), str.begin(), str.end());
  }

  /// Writes a custom value based on a format string.
  template <class... Args>
  [[nodiscard]] auto fmt_value(fmt::format_string<Args...> fs, Args&&... args)
    -> bool {
    return fmt_value_impl(fs, fmt::make_format_args(args...));
  }

  /// Augments the following value with a string.
  ///
  /// This call does not replace the inspection of an actual value.
  template <class... Args>
  [[nodiscard]] auto
  prepend(fmt::format_string<Args...> fs, const Args&... args) -> bool {
    return prepend_impl(fs, fmt::make_format_args(args...));
  }

  /// Augments the preceding value with a string.
  ///
  /// This call does not replace the inspection of an actual value.
  template <class... Args>
  [[nodiscard]] auto append(fmt::format_string<Args...> fs, const Args&... args)
    -> bool {
    return append_impl(fs, fmt::make_format_args(args...));
  }

  // Enters a new level of nesting.
  void push(type = type::element);

private:
  // -- implementation details -------------------------------------------------

  auto out() -> std::back_insert_iterator<std::vector<char>> {
    return std::back_inserter(buf_);
  }

  [[nodiscard]] auto fmt_value_impl(fmt::string_view fs, fmt::format_args args)
    -> bool;

  [[nodiscard]] auto prepend_impl(fmt::string_view fs, fmt::format_args args)
    -> bool;

  [[nodiscard]] auto append_impl(fmt::string_view fs, fmt::format_args args)
    -> bool;

  template <class T>
  bool number(T);

  // -- state management -------------------------------------------------------

  void init();

  // Returns the current top of the stack or `null_literal` if empty.
  type top();

  // Backs up one level of nesting.
  bool pop();

  // Backs up one level of nesting but checks that current top is `t` before.
  bool pop_if(type t);

  // Backs up one level of nesting but checks that the top is `t` afterwards.
  bool pop_if_next(type t);

  // Tries to morph the current top of the stack to t.
  bool morph(type t);

  // Tries to morph the current top of the stack to t. Stores the previous value
  // to `prev`.
  bool morph(type t, type& prev);

  // Morphs the current top of the stack to t without performing *any* checks.
  void unsafe_morph(type t);

  // Sets an error reason that the inspector failed to write a t.
  void fail(type t);

  // Checks whether any element in the stack has the type `object`.
  bool inside_object() const noexcept;

  // -- printing ---------------------------------------------------------------

  // Adds a newline unless `compact() == true`.
  void nl();

  // Adds a separator to the output buffer unless the current entry is empty.
  // The separator is just a comma when in compact mode and otherwise a comma
  // followed by a newline.
  void sep();

  // -- member variables -------------------------------------------------------

  // The current level of indentation.
  size_t indentation_level_ = 0;

  // The number of whitespaces to add per indentation level.
  size_t indentation_factor_ = 0;

  // Buffer for producing the JSON output.
  std::vector<char> buf_;

  struct entry {
    type t;
    bool filled;
    friend bool operator==(entry x, type y) noexcept {
      return x.t == y;
    };
  };

  // Bookkeeping for where we are in the current object.
  std::vector<entry> stack_;

  // Configures whether we omit empty fields entirely (true) or render empty
  // fields as `$field: null` (false).
  bool skip_empty_fields_ = skip_empty_fields_default;

  string_view field_type_suffix_ = field_type_suffix_default;
};

template <class T>
auto as_debug_writer(T& x) -> debug_writer* {
  using U = std::remove_cvref_t<T>;
  if constexpr (std::is_same_v<U, debug_writer>) {
    return &x;
  } else if constexpr (std::is_base_of_v<caf::serializer, U>) {
    return dynamic_cast<debug_writer*>(&x);
  } else {
    return nullptr;
  }
}

} // namespace tenzir
