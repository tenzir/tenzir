//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/segment_store.hpp"

#include "vast/bitmap_algorithms.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/fbs/segment.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/system/status_verbosity.hpp"
#include "vast/table_slice.hpp"

#include <caf/config_value.hpp>
#include <caf/dictionary.hpp>
#include <caf/settings.hpp>

#include <filesystem>
#include <system_error>

namespace vast {

segment_store::lookup::lookup(const segment_store& store, ids xs,
                              std::vector<uuid>&& candidates)
  : store_{store}, xs_{std::move(xs)}, candidates_{std::move(candidates)} {
  // nop
}

caf::expected<table_slice> segment_store::lookup::next() {
  // Update the buffer if it has been consumed or the previous
  // refresh return an error.
  while (!buffer_ || it_ == buffer_->end()) {
    buffer_ = handle_segment();
    if (!buffer_)
      // Either an error occurred, or the list of candidates is exhausted.
      return buffer_.error();
    it_ = buffer_->begin();
  }
  return *it_++;
}

caf::expected<std::vector<table_slice>>
segment_store::lookup::handle_segment() {
  if (first_ == candidates_.end())
    return caf::no_error;
  auto& cand = *first_++;
  if (cand == store_.builder_.id()) {
    VAST_DEBUG("{} looks into the active segment {}",
               detail::pretty_type_name(this), cand);
    return store_.builder_.lookup(xs_);
  }
  auto i = store_.cache_.find(cand);
  if (i != store_.cache_.end()) {
    VAST_DEBUG("{} got cache hit for segment {}",
               detail::pretty_type_name(this), cand);
    return i->second.lookup(xs_);
  }
  VAST_DEBUG("{} got cache miss for segment {}", detail::pretty_type_name(this),
             cand);
  auto s = store_.load_segment(cand);
  if (!s)
    return s.error();
  store_.cache_.emplace(cand, *s);
  return s->lookup(xs_);
}

// TODO: return expected<segment_store_ptr> for better error propagation.
segment_store_ptr
segment_store::make(std::filesystem::path dir, size_t max_segment_size,
                    size_t in_memory_segments) {
  VAST_TRACE_SCOPE("{} {} {}", VAST_ARG(dir), VAST_ARG(max_segment_size),
                   VAST_ARG(in_memory_segments));
  VAST_ASSERT(max_segment_size > 0);
  auto result = segment_store_ptr{
    new segment_store{std::move(dir), max_segment_size, in_memory_segments}};
  if (auto err = result->register_segments())
    return nullptr;
  return result;
}

segment_store::segment_store(std::filesystem::path dir,
                             uint64_t max_segment_size,
                             size_t in_memory_segments)
  : dir_{std::move(dir)},
    max_segment_size_{max_segment_size},
    cache_{in_memory_segments},
    // TODO: Make vast.max-segment-size a hard instead of a soft limit, such
    // that we do not need to multiplay with an arbitrary value above 1 here.
    builder_{detail::narrow_cast<size_t>(max_segment_size * 1.1)} {
  // nop
}

caf::error segment_store::put(table_slice xs) {
  VAST_TRACE_SCOPE("{}", VAST_ARG(xs));
  if (!segments_.inject(xs.offset(), xs.offset() + xs.rows(), builder_.id()))
    return caf::make_error(ec::unspecified, "failed to update range_map");
  num_events_ += xs.rows();
  if (auto error = builder_.add(std::move(xs)))
    return error;
  if (builder_.table_slice_bytes() < max_segment_size_)
    return caf::none;
  // We have exceeded our maximum segment size and now finish.
  return flush();
}

std::unique_ptr<segment_store::lookup>
segment_store::extract(const ids& xs) const {
  VAST_TRACE_SCOPE("{}", VAST_ARG(xs));
  // Collect candidate segments by seeking through the ID set and
  // probing each ID interval.
  std::vector<uuid> candidates;
  if (auto err = select_segments(xs, candidates)) {
    VAST_WARN("{} failed to get candidates for ids {}",
              detail::pretty_type_name(this), xs);
    return nullptr;
  }
  VAST_DEBUG("{} processes {} candidates", detail::pretty_type_name(this),
             candidates.size());
  std::partition(candidates.begin(), candidates.end(), [&](const auto& id) {
    return id == builder_.id() || cache_.find(id) != cache_.end();
  });
  return std::make_unique<lookup>(*this, std::move(xs), std::move(candidates));
}

caf::error segment_store::erase(const ids& xs) {
  VAST_TRACE_SCOPE("{}", VAST_ARG(xs));
  VAST_VERBOSE("erasing {} ids from store", rank(xs));
  // Get affected segments.
  std::vector<uuid> candidates;
  if (auto err = select_segments(xs, candidates))
    return err;
  if (candidates.empty())
    return caf::none;
  auto is_subset_of_xs = [&](const ids& ys) { return is_subset(ys, xs); };
  // Counts number of total erased events for user-facing output.
  uint64_t erased_events = 0;
  // Implements the body of the for-loop below. This lambda must be generic,
  // because the argument is either a `segment` or a `segment_builder`. This
  // algorithm removes all events with IDs in `xs` from a segment. For existing
  // segments, we create a new segment that contains all table slices that
  // remain after erasing `xs` from the input segment. For builders, we update
  // the builder directly by replacing the set of table slices. In any case, we
  // have to update `segments_` to point to the new segment ID.
  auto impl = [&](auto& seg) {
    auto segment_id = seg.id();
    // Get all slices in the segment and generate a new segment that contains
    // only what's left after dropping the selection.
    auto segment_ids = seg.ids();
    // Check whether we can drop the entire segment.
    if (is_subset_of_xs(segment_ids)) {
      erased_events += drop(seg);
      return;
    }
    std::vector<table_slice> slices;
    if (auto maybe_slices = seg.lookup(segment_ids)) {
      slices = std::move(*maybe_slices);
      if (slices.empty()) {
        VAST_WARN("{} got no slices after lookup for segment {} => "
                  "erases entire segment!",
                  detail::pretty_type_name(this), segment_id);
        erased_events += drop(seg);
        return;
      }
    } else {
      VAST_WARN("{} was unable to get table slice for segment {} => "
                "erases entire segment!",
                detail::pretty_type_name(this), segment_id);
      erased_events += drop(seg);
      return;
    }
    VAST_ASSERT(slices.size() > 0);
    // We have IDs we wish to delete in `xs`, but we need a bitmap of what to
    // keep for `select` in order to fill `new_slices` with the table slices
    // that remain after dropping all deleted IDs from the segment.
    auto keep_mask = ~xs;
    std::vector<table_slice> new_slices;
    for (auto& slice : slices) {
      // Expand keep_mask on-the-fly if needed.
      auto max_id = slice.offset() + slice.rows();
      if (keep_mask.size() < max_id)
        keep_mask.append_bits(true, max_id - keep_mask.size());
      size_t new_slices_size_before = new_slices.size();
      select(new_slices, slice, keep_mask);
      size_t remaining_rows = 0;
      for (size_t i = new_slices_size_before; i < new_slices.size(); ++i)
        remaining_rows += new_slices[i].rows();
      erased_events += slice.rows() - remaining_rows;
    }
    if (new_slices.empty()) {
      VAST_WARN("{} was unable to generate any new slice for segment "
                "{} => erases entire segment!",
                detail::pretty_type_name(this), segment_id);
      erased_events += drop(seg);
      return;
    }
    VAST_VERBOSE("{} shrinks segment {} from {} to {} slices",
                 detail::pretty_type_name(this), segment_id, slices.size(),
                 new_slices.size());
    // Remove stale state.
    segments_.erase_value(segment_id);
    // Estimate the size of the new segment.
    auto size_estimate = size_t{};
    for (const auto& slice : new_slices)
      size_estimate += as_bytes(slice).size();
    size_estimate *= 1.1;
    // Create a new segment from the remaining slices.
    segment_builder tmp_builder{size_estimate};
    segment_builder* builder = &tmp_builder;
    if constexpr (std::is_same_v<decltype(seg), segment_builder&>) {
      // If `update` got called with a builder then we simply use that by
      // resetting it and filling it with new content. Otherwise, we fill
      // `tmp_builder` instead and replace the the segment `seg` in the next
      // `if constexpr` block.
      seg.reset();
      builder = &seg;
    }
    for (auto& slice : new_slices) {
      if (auto err = builder->add(slice)) {
        VAST_ERROR("{} failed to add slice to builder: {}",
                   detail::pretty_type_name(this), err);
      } else if (!segments_.inject(slice.offset(),
                                   slice.offset() + slice.rows(),
                                   builder->id()))
        VAST_ERROR("{} failed to update range_map",
                   detail::pretty_type_name(this));
    }
    // Flush the new segment and remove the previous segment.
    if constexpr (std::is_same_v<decltype(seg), segment&>) {
      auto new_segment = builder->finish();
      auto filename = segment_path() / to_string(new_segment.id());
      if (auto err = write(filename, new_segment.chunk()))
        VAST_ERROR("{} failed to persist the new segment",
                   detail::pretty_type_name(this));
      auto stale_filename = segment_path() / to_string(segment_id);
      // Schedule deletion of the segment file when releasing the chunk.
      seg.chunk()->add_deletion_step([=]() noexcept {
        std::error_code err{};
        std::filesystem::remove(stale_filename, err);
      });
    }
    // else: nothing to do, since we can continue filling the active segment.
  };
  // Iterate affected segments.
  for (auto& candidate : candidates) {
    auto j = cache_.find(candidate);
    if (j != cache_.end()) {
      VAST_DEBUG("{} erases from the cached segment {}",
                 detail::pretty_type_name(this), candidate);
      impl(j->second);
      cache_.erase(j);
    } else if (candidate == builder_.id()) {
      VAST_DEBUG("{} erases from the active segment {}",
                 detail::pretty_type_name(this), candidate);
      impl(builder_);
    } else if (auto s = load_segment(candidate)) {
      VAST_DEBUG("{} erases from the segment {}",
                 detail::pretty_type_name(this), candidate);
      impl(*s);
    }
  }
  if (erased_events > 0) {
    VAST_ASSERT(erased_events <= num_events_);
    num_events_ -= erased_events;
    VAST_INFO("{} erased {} events", detail::pretty_type_name(this),
              erased_events);
  }
  return caf::none;
}

caf::expected<std::vector<table_slice>> segment_store::get(const ids& xs) {
  VAST_TRACE_SCOPE("{}", VAST_ARG(xs));
  // Collect candidate segments by seeking through the ID set and
  // probing each ID interval.
  std::vector<uuid> candidates;
  if (auto err = select_segments(xs, candidates))
    return err;
  // Process candidates in reverse order for maximum LRU cache hits.
  std::vector<table_slice> result;
  VAST_DEBUG("{} processes {} candidates", detail::pretty_type_name(this),
             candidates.size());
  std::partition(candidates.begin(), candidates.end(), [&](const auto& id) {
    return id == builder_.id() || cache_.find(id) != cache_.end();
  });
  for (auto cand = candidates.begin(); cand != candidates.end(); ++cand) {
    auto& id = *cand;
    caf::expected<std::vector<table_slice>> slices{caf::no_error};
    if (id == builder_.id()) {
      VAST_DEBUG("{} looks into the active segment {}",
                 detail::pretty_type_name(this), id);
      slices = builder_.lookup(xs);
    } else {
      auto i = cache_.find(id);
      if (i == cache_.end()) {
        VAST_DEBUG("{} got cache miss for segment {}",
                   detail::pretty_type_name(this), id);
        auto x = load_segment(id);
        if (!x)
          return x.error();
        i = cache_.emplace(id, std::move(*x)).first;
      } else {
        VAST_DEBUG("{} got cache hit for segment {}",
                   detail::pretty_type_name(this), id);
      }
      VAST_DEBUG("{} looks into segment {}", detail::pretty_type_name(this),
                 id);
      slices = i->second.lookup(xs);
    }
    if (!slices)
      return slices.error();
    result.reserve(result.size() + slices->size());
    result.insert(result.end(), slices->begin(), slices->end());
  }
  return result;
}

caf::error segment_store::flush() {
  if (!dirty())
    return caf::none;
  VAST_DEBUG("{} finishes current builder", detail::pretty_type_name(this));
  auto seg = builder_.finish();
  auto filename = segment_path() / to_string(seg.id());
  if (auto err = write(filename, seg.chunk()))
    return err;
  // Keep new segment in the cache.
  cache_.emplace(seg.id(), seg);
  VAST_DEBUG("{} wrote new segment to {}", detail::pretty_type_name(this),
             filename.parent_path());
  return caf::none;
}

void segment_store::inspect_status(caf::settings& xs,
                                   system::status_verbosity v) {
  using caf::put;
  if (v >= system::status_verbosity::info) {
    put(xs, "events", num_events_);
    auto mem = builder_.table_slice_bytes();
    for (auto& segment : cache_)
      mem += segment.second.chunk()->size();
    put(xs, "memory-usage", mem);
  }
  if (v >= system::status_verbosity::detailed) {
    auto& segments = put_dictionary(xs, "segments");
    auto& cached = put_list(segments, "cached");
    for (auto& kvp : cache_)
      cached.emplace_back(to_string(kvp.first));
    auto& current = put_dictionary(segments, "current");
    put(current, "uuid", to_string(builder_.id()));
    put(current, "size", builder_.table_slice_bytes());
  }
}

caf::error segment_store::register_segments() {
  if (!std::filesystem::exists(segment_path()))
    return caf::none;
  std::error_code err{};
  std::filesystem::directory_iterator dir{segment_path(), err};
  if (err)
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to find segment path {} : {}",
                                       dir->path(), err.message()));
  for (const auto& entry : dir)
    if (entry.exists())
      if (auto err = register_segment(entry.path()))
        return err;
  return caf::none;
}

caf::error
segment_store::register_segment(const std::filesystem::path& filename) {
  auto chk = chunk::mmap(filename);
  if (!chk)
    return std::move(chk.error());
  // We don't verify the segment here, since doing that would access
  // most of the pages of the mapping and effectively cause us to
  // read of the whole archive contents from disk. When the database
  // approaches the terabyte range, this becomes prohibitively expensive.
  // (see also tdhtf/ch1935)
  // TODO: Create a library function that performs verification on a
  // subset of the fields of a flatbuffer table.
  auto s = fbs::GetSegment(chk->get()->data());
  if (s == nullptr)
    return caf::make_error(ec::format_error, "segment integrity check failed");
  auto s0 = s->segment_as_v0();
  if (!s0)
    return caf::make_error(ec::format_error, "unknown segment version");
  num_events_ += s0->events();
  uuid segment_uuid;
  if (auto error = unpack(*s0->uuid(), segment_uuid))
    return error;
  VAST_DEBUG("{} found segment {}", detail::pretty_type_name(this),
             segment_uuid);
  for (auto interval : *s0->ids())
    if (!segments_.inject(interval->begin(), interval->end(), segment_uuid))
      return caf::make_error(ec::unspecified, "failed to update range_map");
  return caf::none;
}

caf::expected<segment> segment_store::load_segment(uuid id) const {
  auto filename = segment_path() / to_string(id);
  VAST_DEBUG("{} mmaps segment from {}", detail::pretty_type_name(this),
             filename);
  auto chk = chunk::mmap(filename);
  if (!chk)
    return std::move(chk.error());
  if (auto segment = segment::make(std::move(*chk))) {
    return segment;
  } else {
    VAST_ERROR("{} failed to load segment at {} with error: {}",
               detail::pretty_type_name(this), filename,
               render(segment.error()));
    return std::move(segment.error());
  }
}

caf::error segment_store::select_segments(const ids& selection,
                                          std::vector<uuid>& candidates) const {
  VAST_DEBUG("{} retrieves table slices with requested ids",
             detail::pretty_type_name(this));
  auto f = [](auto x) { return std::pair{x.left, x.right}; };
  auto g = [&](auto x) {
    auto id = x.value;
    if (candidates.empty() || candidates.back() != id)
      candidates.push_back(id);
    return caf::none;
  };
  auto begin = segments_.begin();
  auto end = segments_.end();
  return select_with(selection, begin, end, f, g);
}

uint64_t segment_store::drop(segment& x) {
  uint64_t erased_events = 0;
  auto segment_id = x.id();
  // TODO: discuss whether we want to allow accessing a segment through the
  // flatbuffers API. The (heavy-weight) altnerative here would be to create a
  // custom iterator so that a segment can be iterated as a list of table_slice
  // instances.
  auto s = fbs::GetSegment(x.chunk()->data());
  auto s0 = s->segment_as_v0();
  for (auto flat_slice : *s0->slices()) {
    auto slice = table_slice{*flat_slice, x.chunk(), table_slice::verify::no};
    erased_events += slice.rows();
  }
  VAST_INFO("{} erases entire segment {}", detail::pretty_type_name(this),
            segment_id);
  // Schedule deletion of the segment file when releasing the chunk.
  auto filename = segment_path() / to_string(segment_id);
  x.chunk()->add_deletion_step([=]() noexcept {
    std::error_code err{};
    std::filesystem::remove(filename, err);
  });
  segments_.erase_value(segment_id);
  return erased_events;
}

uint64_t segment_store::drop(segment_builder& x) {
  uint64_t erased_events = 0;
  auto segment_id = x.id();
  for (auto& slice : x.table_slices())
    erased_events += slice.rows();
  VAST_INFO("{} erases segment under construction {}",
            detail::pretty_type_name(this), segment_id);
  x.reset();
  segments_.erase_value(segment_id);
  return erased_events;
}

} // namespace vast
