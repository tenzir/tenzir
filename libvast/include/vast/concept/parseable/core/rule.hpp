//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/type_traits.hpp"

#include <memory>
#include <utility>

namespace vast {
namespace detail {

template <class Iterator, class Attribute>
struct abstract_rule;

template <class Iterator>
struct abstract_rule<Iterator, unused_type> {
  virtual ~abstract_rule() = default;
  [[nodiscard]] virtual abstract_rule* clone() const = 0;
  virtual bool parse(Iterator& f, const Iterator& l, unused_type) const = 0;
};

template <class Iterator, class Attribute>
struct abstract_rule {
  virtual ~abstract_rule() = default;
  [[nodiscard]] virtual abstract_rule* clone() const = 0;
  virtual bool parse(Iterator& f, const Iterator& l, unused_type) const = 0;
  virtual bool parse(Iterator& f, const Iterator& l, Attribute& a) const = 0;
};

template <class Parser, class Iterator, class Attribute>
class rule_definition;

template <class Parser, class Iterator>
class rule_definition<Parser, Iterator, unused_type>
  : public abstract_rule<Iterator, unused_type> {
public:
  explicit rule_definition(Parser p) : parser_(std::move(p)) {
  }

  rule_definition(const rule_definition& rhs) : parser_{rhs.parser_} {
    // nop
  }

  [[nodiscard]] rule_definition* clone() const override {
    return new rule_definition(*this);
  };

  bool parse(Iterator& f, const Iterator& l, unused_type) const override {
    return parser_(f, l, unused);
  }

private:
  Parser parser_;
};

template <class Parser, class Iterator, class Attribute>
class rule_definition : public abstract_rule<Iterator, Attribute> {
public:
  explicit rule_definition(Parser p) : parser_(std::move(p)) {
  }

  rule_definition(const rule_definition& rhs) : parser_{rhs.parser_} {
    // nop
  }

  [[nodiscard]] rule_definition* clone() const override {
    return new rule_definition(*this);
  };

  bool parse(Iterator& f, const Iterator& l, unused_type) const override {
    return parser_(f, l, unused);
  }

  bool parse(Iterator& f, const Iterator& l, Attribute& a) const override {
    return parser_(f, l, a);
  }

private:
  Parser parser_;
};

} // namespace detail

/// A type-erased parser which can store any other parser. This type exhibits
/// value semantics and can therefore not be used to construct recursive
/// parsers.
template <class Iterator>
class type_erased_parser : public parser_base<type_erased_parser<Iterator>> {
public:
  using abstract_rule_type = detail::abstract_rule<Iterator, unused_type>;
  using rule_pointer = std::unique_ptr<abstract_rule_type>;
  using attribute = unused_type;

  template <class RHS>
  static auto make_parser(RHS&& rhs) {
    using rule_type = detail::rule_definition<RHS, Iterator, unused_type>;
    return std::make_unique<rule_type>(std::forward<RHS>(rhs));
  }

  type_erased_parser() = default;

  type_erased_parser(const type_erased_parser& rhs)
    : parser_{rhs.parser_->clone()} {
    // nop
  }

  template <class RHS>
  requires(!detail::is_same_or_derived_v<type_erased_parser, RHS>)
    type_erased_parser(RHS&& rhs)
    : parser_{make_parser<RHS>(std::forward<RHS>(rhs))} {
    static_assert(parser<std::decay_t<RHS>>);
  }

  type_erased_parser& operator=(const type_erased_parser& rhs) {
    parser_.reset(rhs.parser_->clone());
    return *this;
  }

  template <class RHS>
  requires(!detail::is_same_or_derived_v<type_erased_parser, RHS>)
    type_erased_parser&
    operator=(RHS&& rhs) {
    static_assert(parser<std::decay_t<RHS>>);
    parser_ = make_parser<RHS>(std::forward<RHS>(rhs));
    return *this;
  }

  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    return parser_->parse(f, l, unused);
  }

private:
  rule_pointer parser_;
};

/// A type-erased parser which can store any other parser.
template <class Iterator, class Attribute = unused_type>
class rule : public parser_base<rule<Iterator, Attribute>> {
  using abstract_rule_type = detail::abstract_rule<Iterator, Attribute>;
  using rule_pointer = std::unique_ptr<abstract_rule_type>;

  template <class RHS>
  void make_parser(RHS&& rhs) {
    // TODO:
    // static_assert(is_compatible_attribute<RHS, typename RHS::attribute>{},
    //              "incompatible parser attributes");
    using rule_type = detail::rule_definition<RHS, Iterator, Attribute>;
    *parser_ = std::make_unique<rule_type>(std::forward<RHS>(rhs));
  }

public:
  using attribute = Attribute;

  rule() : parser_{std::make_shared<rule_pointer>()} {
  }

  template <parser RHS>
  requires(!detail::is_same_or_derived_v<rule, RHS>) rule(RHS&& rhs) : rule{} {
    make_parser<RHS>(std::forward<RHS>(rhs));
  }

  template <parser RHS>
  requires(!detail::is_same_or_derived_v<rule, RHS>) auto operator=(RHS&& rhs) {
    make_parser<RHS>(std::forward<RHS>(rhs));
  }

  template <class T>
  bool parse(Iterator& f, const Iterator& l, T&& x) const {
    VAST_ASSERT(*parser_ != nullptr);
    return (*parser_)->parse(f, l, std::forward<T>(x));
  }

  [[nodiscard]] const std::shared_ptr<rule_pointer>& parser() const {
    return parser_;
  }

private:
  std::shared_ptr<rule_pointer> parser_;
};

/// A type-erased, non-owning reference to a parser.
template <class Iterator, class Attribute = unused_type>
class rule_ref : public parser_base<rule_ref<Iterator, Attribute>> {
  using abstract_rule_type = detail::abstract_rule<Iterator, Attribute>;
  using rule_pointer = std::unique_ptr<abstract_rule_type>;

  template <class RHS>
  void make_parser(RHS&& rhs) {
    // TODO:
    // static_assert(is_compatible_attribute<RHS, typename RHS::attribute>{},
    //              "incompatible parser attributes");
    using rule_type = detail::rule_definition<RHS, Iterator, Attribute>;
    *parser_ = std::make_unique<rule_type>(std::forward<RHS>(rhs));
  }

public:
  using attribute = Attribute;

  explicit rule_ref(const rule<Iterator, Attribute>& x) : parser_(x.parser()) {
    // nop
  }

  rule_ref(rule_ref&&) = default;

  rule_ref(const rule_ref&) = default;

  rule_ref& operator=(rule_ref&&) = default;

  rule_ref& operator=(const rule_ref&) = default;

  bool parse(Iterator& f, const Iterator& l, unused_type x) const {
    auto ptr = parser_.lock();
    VAST_ASSERT(ptr != nullptr);
    return (*ptr)->parse(f, l, x);
  }

  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    auto ptr = parser_.lock();
    VAST_ASSERT(ptr != nullptr);
    return (*ptr)->parse(f, l, x);
  }

private:
  std::weak_ptr<rule_pointer> parser_;
};

template <class Iterator, class Attribute>
auto ref(const rule<Iterator, Attribute>& x) {
  return rule_ref<Iterator, Attribute>{x};
}

template <class Iterator, class Attribute>
auto ref(rule<Iterator, Attribute>& x) {
  return rule_ref<Iterator, Attribute>{x};
}

} // namespace vast
