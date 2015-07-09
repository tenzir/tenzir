#ifndef VAST_CONCEPT_PARSEABLE_CORE_KLEENE_H
#define VAST_CONCEPT_PARSEABLE_CORE_KLEENE_H

#include <vector>

#include "vast/concept/parseable/core/parser.h"

namespace vast {

template <typename Parser>
class kleene_parser : public parser<kleene_parser<Parser>>
{
public:
  using attribute = std::vector<typename Parser::attribute>;

  explicit kleene_parser(Parser p)
    : parser_{std::move(p)}
  {
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type&) const
  {
    auto save = f;
    while (parser_.parse(f, l, unused))
      save = f;
    f = save;
    return true;
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    auto save = f;
    typename Parser::attribute elem;
    while (parser_.parse(f, l, elem))
    {
      a.push_back(std::move(elem));
      save = f;
    }
    f = save;
    return true;
  }

private:
  Parser parser_;
};

} // namespace vast

#endif
