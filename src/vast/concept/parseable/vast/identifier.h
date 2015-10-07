#ifndef VAST_CONCEPT_PARSEABLE_VAST_IDENTIFIER_H
#define VAST_CONCEPT_PARSEABLE_VAST_IDENTIFIER_H

#include "vast/concept/parseable/core/choice.h"
#include "vast/concept/parseable/core/plus.h"
#include "vast/concept/parseable/core/operators.h"
#include "vast/concept/parseable/string/char.h"
#include "vast/concept/parseable/string/char_class.h"

namespace vast {
namespace parsers {

auto const identifier = +(alnum | chr{'_'});

} // namespace parsers
} // namespace vast

#endif
