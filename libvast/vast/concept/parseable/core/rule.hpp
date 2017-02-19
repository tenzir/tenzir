#ifndef VAST_CONCEPT_PARSEABLE_CORE_RULE_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_RULE_HPP

#include <memory>
#include <utility>

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/type_traits.hpp"

namespace vast {
namespace detail {

template <typename Iterator, typename Attribute>
struct abstract_rule {
  ~abstract_rule() = default;
  virtual bool parse(Iterator& f, Iterator const& l, unused_type) const = 0;
  virtual bool parse(Iterator& f, Iterator const& l, Attribute& a) const = 0;
};

template <typename Parser, typename Iterator, typename Attribute>
class rule_definition : public abstract_rule<Iterator, Attribute> {
public:
  explicit rule_definition(Parser p) : parser_(std::move(p)) {
  }

  bool parse(Iterator& f, Iterator const& l, unused_type) const override {
    return parser_(f, l, unused);
  }

  bool parse(Iterator& f, Iterator const& l, Attribute& a) const override {
    return parser_(f, l, a);
  }

private:
  Parser parser_;
};

} // namespace detail

/// A type-erased parser which can store any other parser.
template <typename Iterator, typename Attribute = unused_type>
class rule : public parser<rule<Iterator, Attribute>> {
  using abstract_rule_type = detail::abstract_rule<Iterator, Attribute>;
  using rule_pointer = std::unique_ptr<abstract_rule_type>;

  template <typename RHS>
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

  template <
    typename RHS,
    typename = std::enable_if_t<
      is_parser<std::decay_t<RHS>>{} && ! detail::is_same_or_derived<rule, RHS>::value
    >
  >
  rule(RHS&& rhs)
    : rule{} {
    make_parser<RHS>(std::forward<RHS>(rhs));
  }

  template <typename RHS>
  auto operator=(RHS&& rhs)
    -> std::enable_if_t<is_parser<std::decay_t<RHS>>{}
                        && !detail::is_same_or_derived<rule, RHS>::value> {
    make_parser<RHS>(std::forward<RHS>(rhs));
  }

  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    VAST_ASSERT(*parser_ != nullptr);
    return (*parser_)->parse(f, l, unused);
  }

  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    VAST_ASSERT(*parser_ != nullptr);
    return (*parser_)->parse(f, l, a);
  }

private:
  std::shared_ptr<rule_pointer> parser_;
};

} // namespace vast

#endif
