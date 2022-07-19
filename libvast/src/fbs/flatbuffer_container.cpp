#include "vast/fbs/flatbuffer_container.hpp"

#include "vast/chunk.hpp"
#include "vast/table_slice.hpp"

namespace vast::fbs {

flatbuffer_container::flatbuffer_container(vast::chunk_ptr chunk) {
  if (!chunk || chunk->size() < 4)
    return;
  auto const* toc = vast::fbs::GetSegmentedFileHeader(chunk->data());
  if (toc->toc_type() != vast::fbs::segmented_file::SegmentedFileHeader::v0)
    return;
  toc_ = toc->toc_as_v0();
  chunk_ = std::move(chunk);
}

chunk_ptr flatbuffer_container::get_raw(size_t idx) {
  VAST_ASSERT_CHEAP(chunk_);
  VAST_ASSERT_CHEAP(toc_);
  auto const& segments = *toc_->file_segments();
  VAST_ASSERT_CHEAP(idx <= segments.size());
  auto const* range = segments.Get(idx);
  return chunk_->slice(range->offset(), range->size());
}

const std::byte* flatbuffer_container::get(size_t idx) const {
  VAST_ASSERT_CHEAP(chunk_);
  VAST_ASSERT_CHEAP(toc_);
  auto const& segments = *toc_->file_segments();
  VAST_ASSERT_CHEAP(idx <= segments.size());
  auto offset = segments.Get(idx)->offset();
  return chunk_->data() + offset;
}

size_t flatbuffer_container::size() const {
  VAST_ASSERT_CHEAP(chunk_);
  VAST_ASSERT_CHEAP(toc_);
  return toc_->file_segments()->size();
}

flatbuffer_container::operator bool() const {
  return toc_ != nullptr;
}

flatbuffer_container_builder::flatbuffer_container_builder(
  size_t expected_size) {
  file_contents_.reserve(expected_size);
  file_contents_.resize(PROBABLY_ENOUGH_BYTES_FOR_TOC_FLATBUFFER);
}

void flatbuffer_container_builder::add(std::span<const std::byte> bytes) {
  segments_.emplace_back(
    fbs::segmented_file::FileSegment{file_contents_.size(), bytes.size()});
  file_contents_.insert(file_contents_.end(), bytes.begin(), bytes.end());
}

flatbuffer_container flatbuffer_container_builder::finish() && {
  auto builder = flatbuffers::FlatBufferBuilder{};
  auto segments_offset = builder.CreateVectorOfStructs(segments_);
  auto v0_builder = fbs::segmented_file::v0Builder(builder);
  v0_builder.add_file_segments(segments_offset);
  auto v0_offset = v0_builder.Finish();
  auto toc_builder = fbs::SegmentedFileHeaderBuilder(builder);
  toc_builder.add_toc_type(vast::fbs::segmented_file::SegmentedFileHeader::v0);
  toc_builder.add_toc(v0_offset.Union());
  auto toc_offset = toc_builder.Finish();
  FinishSegmentedFileHeaderBuffer(builder, toc_offset);
  auto toc_buffer = builder.Release();
  // If the table of contents fits into the reserved space we copy it there,
  // otherwise we have no choice but to copy the whole contents.
  if (toc_buffer.size() <= PROBABLY_ENOUGH_BYTES_FOR_TOC_FLATBUFFER)
    [[likely]] {
    ::memcpy(file_contents_.data(), toc_buffer.data(), toc_buffer.size());
  } else {
    auto old = std::exchange(file_contents_, {});
    file_contents_.reserve(toc_buffer.size() + old.size());
    file_contents_.resize(toc_buffer.size());
    ::memcpy(file_contents_.data(), toc_buffer.data(), toc_buffer.size());
    file_contents_.insert(file_contents_.end(), old.begin(), old.end());
    auto offset_adjustment
      = toc_buffer.size() - PROBABLY_ENOUGH_BYTES_FOR_TOC_FLATBUFFER;
    auto* toc = GetMutableSegmentedFileHeader(file_contents_.data());
    // We just created this so we know the type.
    VAST_ASSERT(toc->toc_type()
                == vast::fbs::segmented_file::SegmentedFileHeader::v0);
    auto* v0 = static_cast<fbs::segmented_file::v0*>(toc->mutable_toc());
    auto* segments = v0->mutable_file_segments();
    for (size_t i = 0; i < segments->size(); ++i) {
      auto* segment = segments->GetMutableObject(i);
      segment->mutate_offset(segment->offset() + offset_adjustment);
    }
  }
  auto chunk = chunk::make(std::move(file_contents_));
  return flatbuffer_container{std::move(chunk)};
}

} // namespace vast::fbs
