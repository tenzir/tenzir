#include "vast/detail/parser/bro15/conn_definition.h"

#include <ze/parser/address_definition.h>
#include "vast/detail/parser/streamer.h"

typedef vast::detail::parser::multi_pass_iterator iterator_type;
template struct ze::parser::address<iterator_type>;
template struct vast::detail::parser::bro15::connection<iterator_type>;
