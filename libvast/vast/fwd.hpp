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

#pragma once

#include <caf/intrusive_ptr.hpp>

namespace vast {

// -- classes ------------------------------------------------------------------

class abstract_type;
class address;
class bitmap;
class chunk;
class column_index;
class const_table_slice_handle;
class data;
class default_table_slice;
class default_table_slice_builder;
class event;
class expression;
class json;
class meta_index;
class path;
class pattern;
class port;
class schema;
class segment;
class segment_builder;
class segment_store;
class store;
class subnet;
class table_index;
class table_slice;
class table_slice_builder;
class table_slice_handle;
class type;
class value;

// -- structs ------------------------------------------------------------------

struct address_type;
struct alias_type;
struct boolean_type;
struct count_type;
struct enumeration_type;
struct integer_type;
struct map_type;
struct none_type;
struct pattern_type;
struct port_type;
struct real_type;
struct record_type;
struct set_type;
struct string_type;
struct subnet_type;
struct timespan_type;
struct timestamp_type;
struct vector_type;

// -- smart pointers -----------------------------------------------------------

using chunk_ptr = caf::intrusive_ptr<chunk>;
using column_index_ptr = std::unique_ptr<column_index>;
using table_slice_ptr = caf::intrusive_ptr<table_slice>;
using const_table_slice_ptr = caf::intrusive_ptr<const table_slice>;
using table_slice_builder_ptr = caf::intrusive_ptr<table_slice_builder>;
using default_table_slice_ptr = caf::intrusive_ptr<default_table_slice>;

// -- miscellaneous ------------------------------------------------------------

using ids = bitmap; // temporary; until we have a real type for 'ids'

} // namespace vast
