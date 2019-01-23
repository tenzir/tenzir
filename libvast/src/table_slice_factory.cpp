/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/table_slice_factory.hpp"

#include <caf/streambuf.hpp>
#include <caf/stream_deserializer.hpp>

#include "vast/chunk.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/detail/assert.hpp"
#include "vast/logger.hpp"

namespace vast {

void factory_traits<table_slice>::initialize() {
  factory<table_slice>::add<default_table_slice>();
}

table_slice_ptr factory_traits<table_slice>::make(chunk_ptr chunk) {
  if (chunk == nullptr)
    return nullptr;
  // Setup a CAF deserializer.
  auto data = const_cast<char*>(chunk->data()); // CAF won't touch it.
  caf::charbuf buf{data, chunk->size()};
  caf::stream_deserializer<caf::charbuf&> source{buf};
  // Deserialize the class ID and default-construct a table slice.
  caf::atom_value id;
  table_slice_header header;
  if (auto err = source(id, header)) {
    VAST_ERROR_ANON(__func__, "failed to deserialize table slice meta data");
    return nullptr;
  }
  auto result = factory<table_slice>::make(id, std::move(header));
  if (!result) {
    VAST_ERROR_ANON(__func__, "failed to make table slice for:", to_string(id));
    return nullptr;
  }
  // Skip table slice data already processed.
  auto bytes_read = static_cast<size_t>(buf.in_avail());
  VAST_ASSERT(chunk->size() > bytes_read);
  auto header_size = chunk->size() - bytes_read;
  if (auto err = result.unshared().load(chunk->slice(header_size))) {
    VAST_ERROR_ANON(__func__, "failed to load table slice from chunk");
    return nullptr;
  }
  return result;
}

} // namespace vast
