#ifndef VAST_FORMAT_BRO_HPP
#define VAST_FORMAT_BRO_HPP

#include <chrono>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric.hpp"
#include "vast/concept/parseable/string/any.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/data.hpp"
#include "vast/detail/line_range.hpp"
#include "vast/expected.hpp"
#include "vast/filesystem.hpp"
#include "vast/maybe.hpp"
#include "vast/schema.hpp"

namespace vast {

class event;

namespace format {
namespace bro {

/// Parses non-container types.
template <class Iterator, class Attribute>
struct bro_parser {
  bro_parser(Iterator& f, Iterator const& l, Attribute& attr)
    : f_{f},
      l_{l},
      attr_{attr} {
  }

  template <class Parser>
  bool parse(Parser const& p) const {
    return p.parse(f_, l_, attr_);
  }

  template <class T>
  bool operator()(T const&) const {
    return false;
  }

  bool operator()(boolean_type const&) const {
    return parse(parsers::tf);
  }

  bool operator()(integer_type const&) const {
    static auto p = parsers::i64 ->* [](integer x) { return x; };
    return parse(p);
  }

  bool operator()(count_type const&) const {
    static auto p = parsers::u64 ->* [](count x) { return x; };
    return parse(p);
  }

  bool operator()(timestamp_type const&) const {
    static auto p = parsers::real ->* [](real x) {
      auto i = std::chrono::duration_cast<timespan>(double_seconds(x));
      return timestamp{i};
    };
    return parse(p);
  }

  bool operator()(timespan_type const&) const {
    static auto p = parsers::real ->* [](real x) {
      return std::chrono::duration_cast<timespan>(double_seconds(x));
    };
    return parse(p);
  }

  bool operator()(string_type const&) const {
    static auto p = +parsers::any
      ->* [](std::string x) { return detail::byte_unescape(x); };
    return parse(p);
  }

  bool operator()(pattern_type const&) const {
    static auto p = +parsers::any
      ->* [](std::string x) { return detail::byte_unescape(x); };
    return parse(p);
  }

  bool operator()(address_type const&) const {
    static auto p = parsers::addr ->* [](address x) { return x; };
    return parse(p);
  }

  bool operator()(subnet_type const&) const {
    static auto p = parsers::net ->* [](subnet x) { return x; };
    return parse(p);
  }

  bool operator()(port_type const&) const {
    static auto p = parsers::u16
      ->* [](uint16_t x) { return port{x, port::unknown}; };
    return parse(p);
  }

  Iterator& f_;
  Iterator const& l_;
  Attribute& attr_;
};

/// Constructs a polymorphic Bro data parser.
template <class Iterator, class Attribute>
struct bro_parser_factory {
  using result_type = rule<Iterator, Attribute>;

  bro_parser_factory(std::string const& set_separator)
    : set_separator_{set_separator} {
  }

  template <class T>
  result_type operator()(T const&) const {
    return {};
  }

  result_type operator()(boolean_type const&) const {
    return parsers::tf;
  }

  result_type operator()(integer_type const&) const {
    return parsers::i64 ->* [](integer x) { return x; };
  }

  result_type operator()(count_type const&) const {
    return parsers::u64 ->* [](count x) { return x; };
  }

  result_type operator()(timestamp_type const&) const {
    return parsers::real ->* [](real x) {
      auto i = std::chrono::duration_cast<timespan>(double_seconds(x));
      return timestamp{i};
    };
  }

  result_type operator()(timespan_type const&) const {
    return parsers::real ->* [](real x) {
      return std::chrono::duration_cast<timespan>(double_seconds(x));
    };
  }

  result_type operator()(string_type const&) const {
    if (set_separator_.empty())
      return +parsers::any
        ->* [](std::string x) { return detail::byte_unescape(x); };
    else
      return +(parsers::any - set_separator_)
               ->*[](std::string x) { return detail::byte_unescape(x); };
  }

  result_type operator()(pattern_type const&) const {
    if (set_separator_.empty())
      return +parsers::any
        ->* [](std::string x) { return detail::byte_unescape(x); };
    else
      return +(parsers::any - set_separator_)
        ->* [](std::string x) { return detail::byte_unescape(x); };
  }

  result_type operator()(address_type const&) const {
    return parsers::addr ->* [](address x) { return x; };
  }

  result_type operator()(subnet_type const&) const {
    return parsers::net ->* [](subnet x) { return x; };
  }

  result_type operator()(port_type const&) const {
    return parsers::u16 ->* [](uint16_t x) { return port{x, port::unknown}; };
  }

  result_type operator()(set_type const& t) const {
    auto set_insert = [](std::vector<Attribute> v) {
      set s;
      for (auto& x : v)
        s.insert(std::move(x));
      return s;
    };
    return (visit(*this, t.value_type) % set_separator_) ->* set_insert;
  }

  result_type operator()(vector_type const& t) const {
    return (visit(*this, t.value_type) % set_separator_)
      ->* [](std::vector<Attribute> x) { return vector(std::move(x)); };
  }

  std::string const& set_separator_;
};

/// Constructs a Bro data parser from a type and set separator.
template <class Iterator, class Attribute = data>
rule<Iterator, Attribute>
make_bro_parser(type const& t, std::string const& set_separator = ",") {
  rule<Iterator, Attribute> r;
  auto sep = is_container(t) ? set_separator : "";
  return visit(bro_parser_factory<Iterator, Attribute>{sep}, t);
}

/// Parses non-container Bro data.
template <class Iterator, class Attribute = data>
bool bro_basic_parse(type const& t, Iterator& f, Iterator const& l,
                     Attribute& attr) {
  return visit(bro_parser<Iterator, Attribute>{f, l, attr}, t);
}

/// A Bro reader.
class reader {
public:
  reader() = default;

  /// Constructs a Bro reader.
  /// @param input The stream of logs to read.
  explicit reader(std::unique_ptr<std::istream> input);

  maybe<event> read();

  expected<void> schema(vast::schema const& sch);

  expected<vast::schema> schema() const;

  const char* name() const;

private:
  expected<void> parse_header();

  std::unique_ptr<std::istream> input_;
  std::unique_ptr<detail::line_range> lines_;
  std::string separator_ = " ";
  std::string set_separator_;
  std::string empty_field_;
  std::string unset_field_;
  int timestamp_field_ = -1;
  vast::schema schema_;
  type type_;
  std::vector<rule<std::string::const_iterator, data>> parsers_;
};

/// A Bro writer.
class writer {
public:
  writer() = default;
  writer(writer&&) = default;
  writer& operator=(writer&&) = default;

  /// Constructs a Bro writer.
  /// @param dir The path where to write the log file(s) to.
  writer(path dir);

  ~writer();

  expected<void> write(event const& e);

  expected<void> flush();

  const char* name() const;

private:
  path dir_;
  std::unordered_map<std::string, std::unique_ptr<std::ostream>> streams_;
};

} // namespace bro
} // namespace format
} // namespace vast

#endif
