#ifndef VAST_CONCEPT_PARSEABLE_VAST_DETAIL_BRO_PARSER_FACTORY_HPP
#define VAST_CONCEPT_PARSEABLE_VAST_DETAIL_BRO_PARSER_FACTORY_HPP

#include "vast/data.hpp"
#include "vast/type.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric.hpp"
#include "vast/concept/parseable/string/any.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/util/assert.hpp"
#include "vast/util/string.hpp"

namespace vast {
namespace detail {

/// Parses non-container types.
template <typename Iterator, typename Attribute>
struct bro_parser
{
  bro_parser(Iterator& f, Iterator const& l, Attribute& attr)
    : f_{f},
      l_{l},
      attr_{attr}
  {
  }

  template <typename Parser>
  bool parse(Parser const& p) const
  {
    return p.parse(f_, l_, attr_);
  }

  template <typename T>
  bool operator()(T const&) const
  {
    VAST_ASSERT("invalid type");
    return false;
  }

  bool operator()(type::boolean const&) const
  {
    return parse(parsers::tf);
  }

  bool operator()(type::integer const&) const
  {
    static auto p = parsers::i64 ->* [](integer x) { return x; };
    return parse(p);
  }

  bool operator()(type::count const&) const
  {
    static auto p = parsers::u64 ->* [](count x) { return x; };
    return parse(p);
  }

  bool operator()(type::time_point const&) const
  {
    static auto p = parsers::real
      ->* [](real x) { return time::point{time::fractional(x)}; };
    return parse(p);
  }

  bool operator()(type::time_duration const&) const
  {
    static auto p = parsers::real
      ->* [](real x) { return time::duration{time::fractional(x)}; };
    return parse(p);
  }

  bool operator()(type::string const&) const
  {
    static auto p = +parsers::any
      ->* [](std::string x) { return util::byte_unescape(x); };
    return parse(p);
  }

  bool operator()(type::pattern const&) const
  {
    static auto p = +parsers::any
      ->* [](std::string x) { return util::byte_unescape(x); };
    return parse(p);
  }

  bool operator()(type::address const&) const
  {
    static auto p = parsers::addr ->* [](address x) { return x; };
    return parse(p);
  }

  bool operator()(type::subnet const&) const
  {
    static auto p = parsers::net ->* [](subnet x) { return x; };
    return parse(p);
  }

  bool operator()(type::port const&) const
  {
    static auto p = parsers::u16
      ->* [](uint16_t x) { return port{x, port::unknown}; };
    return parse(p);
  }

  Iterator& f_;
  Iterator const& l_;
  Attribute& attr_;
};

/// Constructs a polymorphic Bro data parser.
template <typename Iterator, typename Attribute>
struct bro_parser_factory {
  using result_type = rule<Iterator, Attribute>;

  bro_parser_factory(std::string const& set_separator)
    : set_separator_{set_separator} {
  }

  template <typename T>
  result_type operator()(T const&) const {
    VAST_ASSERT("invalid type");
    return {};
  }

  result_type operator()(type::boolean const&) const {
    return parsers::tf;
  }

  result_type operator()(type::integer const&) const {
    return parsers::i64 ->* [](integer x) { return x; };
  }

  result_type operator()(type::count const&) const {
    return parsers::u64 ->* [](count x) { return x; };
  }

  result_type operator()(type::time_point const&) const {
    return parsers::real
      ->* [](real x) { return time::point{time::fractional(x)}; };
  }

  result_type operator()(type::time_duration const&) const {
    return parsers::real
      ->* [](real x) { return time::duration{time::fractional(x)}; };
  }

  result_type operator()(type::string const&) const {
    if (set_separator_.empty())
      return +parsers::any
        ->* [](std::string x) { return util::byte_unescape(x); };
    else
      return +(parsers::any - set_separator_)
               ->*[](std::string x) { return util::byte_unescape(x); };
  }

  result_type operator()(type::pattern const&) const {
    if (set_separator_.empty())
      return +parsers::any
        ->* [](std::string x) { return util::byte_unescape(x); };
    else
      return +(parsers::any - set_separator_)
        ->* [](std::string x) { return util::byte_unescape(x); };
  }

  result_type operator()(type::address const&) const {
    return parsers::addr ->* [](address x) { return x; };
  }

  result_type operator()(type::subnet const&) const {
    return parsers::net ->* [](subnet x) { return x; };
  }

  result_type operator()(type::port const&) const {
    return parsers::u16 ->* [](uint16_t x) { return port{x, port::unknown}; };
  }

  result_type operator()(type::set const& t) const {
    return (visit(*this, t.elem()) % set_separator_)
      ->* [](std::vector<Attribute> x) { return set(std::move(x)); };
  }

  result_type operator()(type::vector const& t) const {
    return (visit(*this, t.elem()) % set_separator_)
      ->* [](std::vector<Attribute> x) { return vector(std::move(x)); };
  }

  std::string const& set_separator_;
};

/// Constructs a Bro data parser from a type and set separator.
template <typename Iterator, typename Attribute = data>
rule<Iterator, Attribute>
make_bro_parser(type const& t, std::string const& set_separator = ",") {
  rule<Iterator, Attribute> r;
  auto sep = t.container() ? set_separator : "";
  return visit(bro_parser_factory<Iterator, Attribute>{sep}, t);
}

/// Parses non-container Bro data.
template <typename Iterator, typename Attribute = data>
bool bro_basic_parse(type const& t, Iterator& f, Iterator const& l,
                     Attribute& attr) {
  return visit(bro_parser<Iterator, Attribute>{f, l, attr}, t);
}

} // namespace detail
} // namespace vast

#endif
