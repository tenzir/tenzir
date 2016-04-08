#ifndef VAST_CONCEPT_PARSEABLE_VAST_IDENTIFIER_HPP
#define VAST_CONCEPT_PARSEABLE_VAST_IDENTIFIER_HPP

#include "vast/concept/parseable/core/choice.hpp"
#include "vast/concept/parseable/core/plus.hpp"
#include "vast/concept/parseable/core/operators.hpp"
#include "vast/concept/parseable/string/char.hpp"
#include "vast/concept/parseable/string/char_class.hpp"

namespace vast {
namespace parsers {

auto const identifier = +(alnum | chr{'_'});

} // namespace parsers
} // namespace vast

#endif
