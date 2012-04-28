#include "vast/ingest/bro-1.5/conn_definition.h"

#include <ze/parser/address_definition.h>
#include "vast/util/parser/streamer.h"

typedef vast::util::parser::multi_pass_iterator iterator_type;
template struct ze::parser::address<iterator_type>;
template struct vast::ingest::bro15::parser::connection<iterator_type>;
