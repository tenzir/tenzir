#ifndef VAST_CONCEPT_PARSEABLE_CORE_MAYBE_H
#define VAST_CONCEPT_PARSEABLE_CORE_MAYBE_H

#include "vast/concept/parseable/core/parser.h"

namespace vast {

/// Like ::optional_parser, but exposes `T` instead of `optional<T>` as
/// attribute.
template <typename Parser>
class maybe_parser : public parser<maybe_parser<Parser>> {
public:
  using attribute = typename Parser::attribute;

  explicit maybe_parser(Parser p)
    : parser_{std::move(p)} {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    parser_.parse(f, l, a);
    return true;
  }

private:
  Parser parser_;
};

} // namespace vast

#endif
