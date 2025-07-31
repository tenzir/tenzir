//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// This file is based upon the JSON writer of CAF, the C++ Actor Framework.

#include "tenzir/detail/debug_writer.hpp"

#include <caf/detail/append_hex.hpp>
#include <caf/detail/print.hpp>
#include <caf/type_id.hpp>

#include <string_view>

namespace tenzir {

using sec = caf::sec;
using caf::query_type_name;

namespace {

static constexpr const char class_name[] = "tenzir::debug_writer";

constexpr bool can_morph(debug_writer::type from, debug_writer::type to) {
  return from == debug_writer::type::element
         && to != debug_writer::type::member;
}

constexpr const char* json_type_names[]
  = {"element", "object", "member", "key", "array",
     "string",  "number", "bool",   "null"};

constexpr const char* json_type_name(debug_writer::type t) {
  return json_type_names[static_cast<uint8_t>(t)];
}

} // namespace

// -- implementation details ---------------------------------------------------

template <class T>
bool debug_writer::number(T x) {
  switch (top()) {
    case type::element:
      caf::detail::print(buf_, x);
      pop();
      return true;
    case type::key:
      add('"');
      caf::detail::print(buf_, x);
      add("\": ");
      return true;
    case type::array:
      sep();
      caf::detail::print(buf_, x);
      return true;
    default:
      fail(type::number);
      return false;
  }
}

// -- constructors, destructors, and assignment operators ----------------------

debug_writer::debug_writer() {
  init();
}

// -- modifiers ----------------------------------------------------------------

void debug_writer::reset() {
  buf_.clear();
  stack_.clear();
  push();
}

// -- overrides ----------------------------------------------------------------

void debug_writer::set_error(caf::error stop_reason) {
  err_ = std::move(stop_reason);
}

caf::error& debug_writer::get_error() noexcept {
  return err_;
}

caf::actor_system* debug_writer::sys() const noexcept {
  return nullptr;
}

bool debug_writer::has_human_readable_format() const noexcept {
  return true;
}

bool debug_writer::begin_object(type_id_t id, std::string_view name) {
  (void)id;
  (void)name;
  return begin_associative_array(0);
}

bool debug_writer::end_object() {
  return end_associative_array();
}

bool debug_writer::begin_field(std::string_view name) {
  if (begin_key_value_pair()) {
    CAF_ASSERT(top() == type::key);
    add(name);
    add(": ");
    pop();
    CAF_ASSERT(top() == type::element);
    return true;
  } else {
    return false;
  }
}

bool debug_writer::begin_field(std::string_view name, bool is_present) {
  if (skip_empty_fields_ && ! is_present) {
    auto t = top();
    switch (t) {
      case type::object:
        push(type::member);
        return true;
      default: {
        std::string str = "expected object, found ";
        str += json_type_name(t);
        emplace_error(sec::runtime_error, class_name, __func__, std::move(str));
        return false;
      }
    }
  } else if (begin_key_value_pair()) {
    CAF_ASSERT(top() == type::key);
    add(name);
    add(": ");
    pop();
    CAF_ASSERT(top() == type::element);
    if (! is_present) {
      add("null");
      pop();
    }
    return true;
  } else {
    return false;
  }
}

bool debug_writer::begin_field(std::string_view name,
                               span<const type_id_t> types, size_t index) {
  (void)types;
  (void)index;
  return begin_field(name);
}

bool debug_writer::begin_field(std::string_view name, bool is_present,
                               span<const type_id_t> types, size_t index) {
  if (is_present) {
    return begin_field(name, types, index);
  } else {
    return begin_field(name, is_present);
  }
}

bool debug_writer::end_field() {
  return end_key_value_pair();
}

bool debug_writer::begin_tuple(size_t size) {
  return begin_sequence(size);
}

bool debug_writer::end_tuple() {
  return end_sequence();
}

bool debug_writer::begin_key_value_pair() {
  sep();
  auto t = top();
  switch (t) {
    case type::object:
      push(type::member);
      push(type::element);
      push(type::key);
      return true;
    default: {
      std::string str = "expected object, found ";
      str += json_type_name(t);
      emplace_error(sec::runtime_error, class_name, __func__, std::move(str));
      return false;
    }
  }
}

bool debug_writer::end_key_value_pair() {
  return pop_if(type::member);
}

bool debug_writer::begin_sequence(size_t) {
  switch (top()) {
    default:
      emplace_error(sec::runtime_error, "unexpected begin_sequence");
      return false;
    case type::element:
      unsafe_morph(type::array);
      break;
    case type::array:
      push(type::array);
      break;
  }
  add('[');
  ++indentation_level_;
  nl();
  return true;
}

bool debug_writer::end_sequence() {
  if (pop_if(type::array)) {
    --indentation_level_;
    nl();
    add(']');
    return true;
  } else {
    return false;
  }
}

bool debug_writer::begin_associative_array(size_t) {
  switch (top()) {
    default:
      emplace_error(sec::runtime_error, class_name, __func__,
                    "unexpected begin_object or begin_associative_array");
      return false;
    case type::element:
      unsafe_morph(type::object);
      break;
    case type::array:
      sep();
      push(type::object);
      break;
  }
  add('{');
  ++indentation_level_;
  nl();
  return true;
}

bool debug_writer::end_associative_array() {
  if (pop_if(type::object)) {
    --indentation_level_;
    nl();
    add('}');
    if (! stack_.empty()) {
      stack_.back().filled = true;
    }
    return true;
  } else {
    return false;
  }
}

bool debug_writer::value(std::byte x) {
  return number(to_integer<uint8_t>(x));
}

bool debug_writer::value(bool x) {
  auto add_str = [this, x] {
    if (x) {
      add("true");
    } else {
      add("false");
    }
  };
  switch (top()) {
    case type::element:
      add_str();
      pop();
      return true;
    case type::key:
      add('"');
      add_str();
      add("\": ");
      return true;
    case type::array:
      sep();
      add_str();
      return true;
    default:
      fail(type::boolean);
      return false;
  }
}

bool debug_writer::value(int8_t x) {
  return number(x);
}

bool debug_writer::value(uint8_t x) {
  return number(x);
}

bool debug_writer::value(int16_t x) {
  return number(x);
}

bool debug_writer::value(uint16_t x) {
  return number(x);
}

bool debug_writer::value(int32_t x) {
  return number(x);
}

bool debug_writer::value(uint32_t x) {
  return number(x);
}

bool debug_writer::value(int64_t x) {
  return number(x);
}

bool debug_writer::value(uint64_t x) {
  return number(x);
}

bool debug_writer::value(float x) {
  return number(x);
}

bool debug_writer::value(double x) {
  return number(x);
}

bool debug_writer::value(long double x) {
  return number(x);
}

bool debug_writer::value(std::string_view x) {
  switch (top()) {
    case type::element:
      caf::detail::print_escaped(buf_, x);
      pop();
      return true;
    case type::key:
      caf::detail::print_escaped(buf_, x);
      add(": ");
      pop();
      return true;
    case type::array:
      sep();
      caf::detail::print_escaped(buf_, x);
      return true;
    default:
      fail(type::string);
      return false;
  }
}

bool debug_writer::value(const std::u16string&) {
  emplace_error(sec::unsupported_operation,
                "u16string not supported yet by caf::json_writer");
  return false;
}

bool debug_writer::value(const std::u32string&) {
  emplace_error(sec::unsupported_operation,
                "u32string not supported yet by caf::json_writer");
  return false;
}

bool debug_writer::value(span<const std::byte> x) {
  switch (top()) {
    case type::element:
      add('"');
      caf::detail::append_hex(buf_, reinterpret_cast<const void*>(x.data()),
                              x.size());
      add('"');
      pop();
      return true;
    case type::key:
      unsafe_morph(type::element);
      add('"');
      caf::detail::append_hex(buf_, reinterpret_cast<const void*>(x.data()),
                              x.size());
      add("\": ");
      pop();
      return true;
    case type::array:
      sep();
      add('"');
      caf::detail::append_hex(buf_, reinterpret_cast<const void*>(x.data()),
                              x.size());
      add('"');
      return true;
    default:
      fail(type::string);
      return false;
  }
}

// -- state management ---------------------------------------------------------

void debug_writer::init() {
  // Reserve some reasonable storage for the character buffer. JSON grows
  // quickly, so we can start at 1kb to avoid a couple of small allocations in
  // the beginning.
  buf_.reserve(1024);
  // Even heavily nested objects should fit into 32 levels of nesting.
  stack_.reserve(32);
  // Placeholder for what is to come.
  push();
}

debug_writer::type debug_writer::top() {
  if (! stack_.empty()) {
    return stack_.back().t;
  } else {
    return type::null;
  }
}

// Enters a new level of nesting.
void debug_writer::push(type t) {
  stack_.emplace_back(t, false);
}

// Backs up one level of nesting.
bool debug_writer::pop() {
  if (! stack_.empty()) {
    stack_.pop_back();
    return true;
  } else {
    std::string str = "pop() called with an empty stack: begin/end mismatch";
    emplace_error(sec::runtime_error, std::move(str));
    return false;
  }
}

bool debug_writer::pop_if(type t) {
  if (! stack_.empty() && stack_.back() == t) {
    stack_.pop_back();
    return true;
  } else {
    std::string str = "pop_if failed: expected ";
    str += json_type_name(t);
    if (stack_.empty()) {
      str += ", found an empty stack";
    } else {
      str += ", found ";
      str += json_type_name(stack_.back().t);
    }
    emplace_error(sec::runtime_error, std::move(str));
    return false;
  }
}

bool debug_writer::pop_if_next(type t) {
  if (stack_.size() > 1
      && (stack_[stack_.size() - 2] == t
          || can_morph(stack_[stack_.size() - 2].t, t))) {
    stack_.pop_back();
    return true;
  } else {
    std::string str = "pop_if_next failed: expected ";
    str += json_type_name(t);
    if (stack_.size() < 2) {
      str += ", found a stack of size ";
      caf::detail::print(str, stack_.size());
    } else {
      str += ", found ";
      str += json_type_name(stack_[stack_.size() - 2].t);
    }
    emplace_error(sec::runtime_error, std::move(str));
    return false;
  }
}

// Tries to morph the current top of the stack to t.
bool debug_writer::morph(type t) {
  type unused;
  return morph(t, unused);
}

bool debug_writer::morph(type t, type& prev) {
  if (! stack_.empty()) {
    if (can_morph(stack_.back().t, t)) {
      prev = stack_.back().t;
      stack_.back().t = t;
      return true;
    } else {
      std::string str = "cannot convert ";
      str += json_type_name(stack_.back().t);
      str += " to ";
      str += json_type_name(t);
      emplace_error(sec::runtime_error, std::move(str));
      return false;
    }
  } else {
    std::string str = "mismatched begin/end calls on the JSON inspector";
    emplace_error(sec::runtime_error, std::move(str));
    return false;
  }
}

void debug_writer::unsafe_morph(type t) {
  stack_.back().t = t;
}

void debug_writer::fail(type t) {
  std::string str = "failed to write a ";
  str += json_type_name(t);
  str += ": invalid position (begin/end mismatch?)";
  emplace_error(sec::runtime_error, std::move(str));
}

bool debug_writer::inside_object() const noexcept {
  auto is_object = [](const entry& x) {
    return x.t == type::object;
  };
  return std::any_of(stack_.begin(), stack_.end(), is_object);
}

// -- printing ---------------------------------------------------------------

void debug_writer::nl() {
  if (indentation_factor_ > 0) {
    buf_.push_back('\n');
    buf_.insert(buf_.end(), indentation_factor_ * indentation_level_, ' ');
  }
}

void debug_writer::sep() {
  CAF_ASSERT(top() == type::element || top() == type::object
             || top() == type::array);
  if (stack_.back().filled) {
    if (indentation_factor_ > 0) {
      add(",\n");
      buf_.insert(buf_.end(), indentation_factor_ * indentation_level_, ' ');
    } else {
      add(", ");
    }
  } else {
    stack_.back().filled = true;
  }
}

auto debug_writer::fmt_value_impl(fmt::string_view fs, fmt::format_args args)
  -> bool {
  switch (top()) {
    case type::array:
      sep();
      fmt::vformat_to(std::back_inserter(buf_), fs, args);
      return true;
    case type::element:
      fmt::vformat_to(std::back_inserter(buf_), fs, args);
      pop();
      return true;
    default:
      emplace_error(caf::sec::runtime_error, "expected array or element, got",
                    json_type_name(top()));
      return false;
  }
  TENZIR_UNREACHABLE();
}

auto debug_writer::prepend_impl(fmt::string_view fs, fmt::format_args args)
  -> bool {
  switch (top()) {
    case type::array:
      sep();
      fmt::vformat_to(std::back_inserter(buf_), fs, args);
      push();
      return true;
    case type::element:
      fmt::vformat_to(std::back_inserter(buf_), fs, args);
      return true;
    default:
      emplace_error(caf::sec::runtime_error, "expected array or element, got",
                    json_type_name(top()));
      return false;
  }
  TENZIR_UNREACHABLE();
}

[[nodiscard]] auto
debug_writer::append_impl(fmt::string_view fs, fmt::format_args args) -> bool {
  fmt::vformat_to(std::back_inserter(buf_), fs, args);
  return true;
}

} // namespace tenzir
