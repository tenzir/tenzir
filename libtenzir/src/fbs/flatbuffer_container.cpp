#include "tenzir/fbs/flatbuffer_container.hpp"

#include "tenzir/chunk.hpp"
#include "tenzir/table_slice.hpp"

namespace tenzir::fbs {

flatbuffer_container::flatbuffer_container(tenzir::chunk_ptr chunk) {
  using flatbuffers::soffset_t;
  using flatbuffers::uoffset_t;
  if (!chunk || chunk->size() < FLATBUFFERS_MIN_BUFFER_SIZE) {
    return;
  }
  auto const* header = tenzir::fbs::GetSegmentedFileHeader(chunk->data());
  if (header->header_type()
      != tenzir::fbs::segmented_file::SegmentedFileHeader::v0)
    return;
  header_ = header->header_as_v0();
  chunk_ = std::move(chunk);
}

tenzir::chunk_ptr flatbuffer_container::chunk() const {
  return chunk_;
}

tenzir::chunk_ptr flatbuffer_container::dissolve() && {
  header_ = nullptr;
  return std::exchange(chunk_, {});
}

chunk_ptr flatbuffer_container::get_raw(size_t idx) const {
  TENZIR_ASSERT(chunk_);
  TENZIR_ASSERT(header_);
  auto const& segments = *header_->file_segments();
  TENZIR_ASSERT(idx <= segments.size());
  auto const* range = segments.Get(idx);
  return chunk_->slice(range->offset(), range->size());
}

const std::byte* flatbuffer_container::get(size_t idx) const {
  TENZIR_ASSERT(chunk_);
  TENZIR_ASSERT(header_);
  auto const& segments = *header_->file_segments();
  TENZIR_ASSERT(idx <= segments.size());
  auto offset = segments.Get(idx)->offset();
  return chunk_->data() + offset;
}

size_t flatbuffer_container::size() const {
  TENZIR_ASSERT(chunk_);
  TENZIR_ASSERT(header_);
  return header_->file_segments()->size();
}

flatbuffer_container::operator bool() const {
  return header_ != nullptr;
}

flatbuffer_container_builder::flatbuffer_container_builder(
  size_t expected_size) {
  file_contents_.reserve(expected_size + PROBABLY_ENOUGH_BYTES_FOR_HEADER);
  file_contents_.resize(PROBABLY_ENOUGH_BYTES_FOR_HEADER);
}

void flatbuffer_container_builder::add(std::span<const std::byte> bytes) {
  segments_.emplace_back(
    fbs::segmented_file::FileSegment{file_contents_.size(), bytes.size()});
  file_contents_.insert(file_contents_.end(), bytes.begin(), bytes.end());
}

flatbuffer_container
flatbuffer_container_builder::finish(const char* identifier) && {
  auto builder = flatbuffers::FlatBufferBuilder{};
  auto segments_offset = builder.CreateVectorOfStructs(segments_);
  auto v0_builder = fbs::segmented_file::v0Builder(builder);
  auto n = ::strnlen(identifier, 4);
  fbs::segmented_file::FileIdentifier ident;
  std::memset(ident.mutable_data()->Data(), '\0', 4);
  std::memcpy(ident.mutable_data()->Data(), identifier, n);
  v0_builder.add_inner_identifier(&ident);
  v0_builder.add_file_segments(segments_offset);
  auto v0_offset = v0_builder.Finish();
  auto header_builder = fbs::SegmentedFileHeaderBuilder(builder);
  header_builder.add_header_type(
    tenzir::fbs::segmented_file::SegmentedFileHeader::v0);
  header_builder.add_header(v0_offset.Union());
  auto header_offset = header_builder.Finish();
  FinishSegmentedFileHeaderBuffer(builder, header_offset);
  auto header_buffer = builder.Release();
  // If the table of contents fits into the reserved space we copy it there,
  // otherwise we have no choice but to copy the whole contents.
  if (header_buffer.size() <= PROBABLY_ENOUGH_BYTES_FOR_HEADER) [[likely]] {
    ::memcpy(file_contents_.data(), header_buffer.data(), header_buffer.size());
  } else {
    auto old = std::exchange(file_contents_, {});
    file_contents_.reserve(header_buffer.size() + old.size()
                           - PROBABLY_ENOUGH_BYTES_FOR_HEADER);
    file_contents_.resize(header_buffer.size());
    ::memcpy(file_contents_.data(), header_buffer.data(), header_buffer.size());
    file_contents_.insert(file_contents_.end(),
                          old.begin() + PROBABLY_ENOUGH_BYTES_FOR_HEADER,
                          old.end());
    auto offset_adjustment
      = header_buffer.size() - PROBABLY_ENOUGH_BYTES_FOR_HEADER;
    auto* header = GetMutableSegmentedFileHeader(file_contents_.data());
    // We just created this so we know the type.
    TENZIR_ASSERT(header->header_type()
                  == tenzir::fbs::segmented_file::SegmentedFileHeader::v0);
    auto* v0 = static_cast<fbs::segmented_file::v0*>(header->mutable_header());
    auto* segments = v0->mutable_file_segments();
    for (size_t i = 0; i < segments->size(); ++i) {
      auto* segment = segments->GetMutableObject(i);
      // FIXME: This `const_cast` is a hack to work around `GetMutableObject`
      // returning a `const` pointer on newer Flatbuffer versions.
      const_cast<tenzir::fbs::segmented_file::FileSegment*>(segment)
        ->mutate_offset(segment->offset() + offset_adjustment);
    }
  }
  auto chunk = chunk::make(std::move(file_contents_));
  return flatbuffer_container{std::move(chunk)};
}

} // namespace tenzir::fbs
