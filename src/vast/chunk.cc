#include "vast/chunk.h"
#include "vast/event.h"
#include "vast/concept/serializable/state.h"
#include "vast/concept/serializable/std/chrono.h"
#include "vast/concept/serializable/std/array.h"
#include "vast/concept/serializable/std/string.h"
#include "vast/concept/serializable/vast/data.h"
#include "vast/concept/serializable/vast/type.h"
#include "vast/concept/state/event.h"
#include "vast/util/assert.h"

namespace vast {

bool operator==(chunk::meta_data const& x, chunk::meta_data const& y)
{
  return x.first == y.first
      && x.last == y.last
      && x.ids == y.ids
      && x.schema == y.schema;
}

chunk::writer::writer(chunk& chk)
  : meta_{&chk.get_meta()},
    block_writer_{std::make_unique<block::writer>(chk.block())}
{
}

chunk::writer::~writer()
{
  flush();
}

bool chunk::writer::write(event const& e)
{
  if (! block_writer_)
    return false;
  // Write meta data.
  if (meta_->first == time::duration{} || e.timestamp() < meta_->first)
    meta_->first = e.timestamp();
  if (meta_->last == time::duration{} || e.timestamp() > meta_->last)
    meta_->last = e.timestamp();
  if (e.id() != invalid_event_id || ! meta_->ids.empty())
  {
    if (e.id() < meta_->ids.size() || e.id() == invalid_event_id)
      return false;
    auto delta = e.id() - meta_->ids.size();
    meta_->ids.append(delta, false);
    meta_->ids.push_back(true);
  }
  // Write type.
  auto t = type_cache_.find(e.type());
  if (t == type_cache_.end())
  {
    VAST_ASSERT(! meta_->schema.find_type(e.type().name()));
    if (! meta_->schema.add(e.type()))
      return false;
    auto type_id = static_cast<uint32_t>(type_cache_.size());
    if (! block_writer_->write(type_id, 0))
      return false;
    t = type_cache_.emplace(e.type(), type_id).first;
    if (! block_writer_->write(e.type().name(), 0))
      return false;
  }
  else if (! block_writer_->write(t->second, 0))
  {
    return false;
  }
  // Write timestamp and data.
  return block_writer_->write(e.timestamp(), 0)
      && block_writer_->write(e.data());
}

void chunk::writer::flush()
{
  block_writer_.reset();
}


chunk::reader::reader(chunk const& chk)
  : chunk_{&chk},
    block_reader_{std::make_unique<block::reader>(chunk_->block())},
    ids_begin_{chunk_->meta().ids.begin()},
    ids_end_{chunk_->meta().ids.end()}
{
  if (ids_begin_ != ids_end_)
    first_ = *ids_begin_;
}

result<event> chunk::reader::read(event_id id)
{
  if (id != invalid_event_id)
  {
    if (first_ == invalid_event_id)
      return error{"chunk has no associated ids, cannot read event ", id};
    if (id < first_)
      return error{"chunk begins at id ", first_};
    if (ids_begin_ == ids_end_ || id < *ids_begin_)
    {
      block_reader_ = std::make_unique<block::reader>(chunk_->block());
      ids_begin_ = chunk_->meta().ids.begin();
      type_cache_.clear();
    }
    while (ids_begin_ != ids_end_ && *ids_begin_ < id)
    {
      auto e = materialize(true);
      if (e.failed())
        return e.error();
      ++ids_begin_;
    }
    if (ids_begin_ == ids_end_ || *ids_begin_ != id)
      return error{"no event with id ", id};
  }
  auto e = materialize(false);
  if (e && ids_begin_ != ids_end_)
    e->id(*ids_begin_++);
  return e;
}

result<event> chunk::reader::materialize(bool discard)
{
  if (block_reader_->available() == 0)
    return {};
  // Read type.
  uint32_t type_id;
  if (! block_reader_->read(type_id, 0))
    return error{"failed to read type id from block"};
  auto t = type_cache_.find(type_id);
  if (t == type_cache_.end())
  {
    std::string type_name;
    if (! block_reader_->read(type_name, 0))
      return error{"failed to read type name from block"};
    auto st = chunk_->meta().schema.find_type(type_name);
    if (! st)
      return error{"schema inconsistency, missing type: ", type_name};
    t = type_cache_.emplace(type_id, *st).first;
  }
  // Read timstamp and data
  time::point ts;
  if (! block_reader_->read(ts, 0))
    return error{"failed to read event timestamp from block"};
  data d;
  if (! block_reader_->read(d))
    return error{"failed to read event data from block"};
  // Bail out early if requested.
  if (discard)
    return {};
  event e{{std::move(d), t->second}};
  e.timestamp(ts);
  return std::move(e);
}

chunk::chunk(io::compression method)
  : msg_{caf::make_message(meta_data{}, vast::block{method})}
{
}

chunk::chunk(std::vector<event> const& es, io::compression method)
{
  compress(es, method);
}

bool chunk::ids(default_bitstream ids)
{
  if (ids.count() != events())
    return false;
  get_meta().ids = std::move(ids);
  return true;
}

bool chunk::compress(std::vector<event> const& events, io::compression method)
{
  msg_ = caf::make_message(meta_data{}, vast::block{method});
  writer w{*this};
  for (auto& e : events)
    if (! w.write(e))
      return false;
  return true;
}

std::vector<event> chunk::uncompress() const
{
  std::vector<event> result(events());
  reader r{*this};
  for (uint64_t i = 0; i < events(); ++i)
  {
    auto e = r.read();
    VAST_ASSERT(e);
    result[i] = std::move(*e);
  }
  return result;
}

chunk::meta_data const& chunk::meta() const
{
  return msg_.get_as<meta_data>(0);
}

uint64_t chunk::bytes() const
{
  return block().compressed_bytes();
}

uint64_t chunk::events() const
{
  return block().elements();
}

event_id chunk::base() const
{
  auto i = meta().ids.find_first();
  return i == default_bitstream::npos ? invalid_event_id : i;
}

chunk::meta_data& chunk::get_meta()
{
  return msg_.get_as_mutable<meta_data>(0);
}

block& chunk::block()
{
  return msg_.get_as_mutable<vast::block>(1);
}

block const& chunk::block() const
{
  return msg_.get_as<vast::block>(1);
}

bool operator==(chunk const& x, chunk const& y)
{
  return x.meta() == y.meta() && x.block() == y.block();
}

} // namespace vast
