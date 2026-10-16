// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/concatenate.hpp"

#include "tenzir/detail/narrow.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/broadcast.hpp"

#include <limits>

namespace tenzir::nova {
namespace {

using Chunk = MaskedArray<Array<Data>>;

auto total_length(std::span<Chunk const> chunks) -> storage::Index {
  auto length = storage::Index{0};
  for (auto const& chunk : chunks) {
    TENZIR_ASSERT_EQ(chunk.data.length(), chunk.present.length());
    TENZIR_ASSERT_LEQ(chunk.data.length(),
                      std::numeric_limits<storage::Index>::max() - length);
    length += chunk.data.length();
  }
  return length;
}

template <data_type Tag>
auto concatenate_values(std::span<MaskedArray<Array<Tag>> const> chunks,
                        storage::Index length) -> Array<Tag> {
  if constexpr (fundamental_type<Tag>) {
    // Keep repeated metadata and other constant columns compact. Copying the
    // owned constant also releases any unrelated source payload.
    if constexpr (not std::same_as<Tag, Null> and not std::same_as<Tag, Bool>
                  and not std::same_as<Tag, Secret>) {
      auto constant = Option<typename Type<Tag>::DataType>{};
      auto all_constant = true;
      for (auto const& chunk : chunks) {
        if (not chunk.present.any()) {
          continue;
        }
        match(chunk.data.storage(), [&](auto const& physical) {
          if constexpr (requires { physical.value(); }) {
            if (not constant) {
              constant = physical.value();
            } else if (*constant != physical.value()) {
              all_constant = false;
            }
          } else {
            all_constant = false;
          }
        });
      }
      if (all_constant and constant) {
        return Array<Tag>{
          storage::ConstantStorage<Tag, typename Type<Tag>::ViewType>{
            length, std::move(*constant)}};
      }
    }
    auto builder = ArrayBuilder<Tag>{};
    // Append each fundamental column directly, without erased row dispatch.
    auto offset = storage::Index{0};
    for (auto const& chunk : chunks) {
      for (auto row : storage::true_bits(chunk.present)) {
        builder.skip_n(offset + row - builder.length());
        if constexpr (std::same_as<Tag, Null>) {
          builder.null();
        } else {
          builder.data(*chunk.data.get(row));
        }
      }
      offset += chunk.data.length();
    }
    builder.skip_n(length - builder.length());
    return builder.finish();
  } else if constexpr (std::same_as<Tag, List>) {
    auto spans = storage::DataOwner<storage::Span[]>::Builder{};
    auto children = std::vector<Chunk>{};
    auto offset = storage::Index{0};
    for (auto const& chunk : chunks) {
      auto primary = chunk.data.to_primary();
      auto const& source = as<storage::ListStorage>(primary.storage());
      auto ranges = std::vector<storage::Span>{};
      for (auto row = storage::Index{0}; row < primary.length(); ++row) {
        auto begin = offset;
        if (chunk.present.get(row)) {
          auto range = source.spans()[row];
          TENZIR_ASSERT_LEQ(range.end - range.begin,
                            std::numeric_limits<storage::Index>::max()
                              - offset);
          offset += range.end - range.begin;
          ranges.push_back(range);
        }
        spans.emplace_back(storage::Span{begin, offset});
      }
      auto values = concatenate_ranges(source.values(), ranges);
      auto present = storage::BitMap{values.length(), true};
      children.push_back({std::move(values), std::move(present)});
    }
    return Array<List>{spans.finish(), concatenate(children)};
  } else {
    auto names = Array<Record>::Names{};
    auto shapes = ShapeTable{};
    auto indices = Array<Record>::IndicesStorage::Mutable{length};
    auto sources = std::vector<Array<Record>>{};
    for (auto const& chunk : chunks) {
      sources.push_back(chunk.data.to_primary());
      auto const& source
        = *as<storage::RecordStorage>(sources.back().storage());
      for (auto const& name : source.names_by_index) {
        if (not names.contains(name)) {
          auto index = names.size();
          names.try_emplace(storage::String<>{name}, index);
        }
      }
    }
    auto columns = Array<Record>::MaskedArrays{};
    columns.resize(names.size(),
                   {Array<Data>{Array<Null>{storage::NullStorage{length}}},
                    storage::BitMap{length, false}});
    for (auto const& [name, index] : names) {
      auto fields = std::vector<Chunk>{};
      auto present = storage::BitMap::Builder{};
      for (auto i = size_t{0}; i < chunks.size(); ++i) {
        auto field = sources[i].field(name);
        auto mask = field ? field->present & chunks[i].present
                          : storage::BitMap{sources[i].length(), false};
        if (auto constant = mask.as_constant()) {
          present.append_n(*constant, mask.length());
        } else {
          for (auto row = storage::Index{0}; row < mask.length(); ++row) {
            present.emplace_back(mask.get(row));
          }
        }
        fields.push_back({field ? field->data
                                : Array<Data>{Array<Null>{
                                    storage::NullStorage{mask.length()}}},
                          std::move(mask)});
      }
      columns[index] = {concatenate(fields), present.finish()};
    }
    auto offset = storage::Index{0};
    for (auto i = size_t{0}; i < chunks.size(); ++i) {
      auto const& source = *as<storage::RecordStorage>(sources[i].storage());
      auto remap = storage::Vector<storage::Index>{};
      for (auto const& name : source.names_by_index) {
        remap.push_back(
          detail::narrow<storage::Index>(names.find(name)->second));
      }
      auto shape_remap = ShapeTable::ShapeRemap{};
      for (auto row = storage::Index{0}; row < sources[i].length(); ++row) {
        if (not chunks[i].present.get(row)) {
          indices.set(offset + row, -1);
          continue;
        }
        auto shape = source.shape_indices.get(row);
        auto found = shape_remap.find(shape);
        if (found == shape_remap.end()) {
          auto imported = shapes.import_shapes(source.shape_table, remap,
                                               std::span{&shape, size_t{1}});
          found = shape_remap.emplace(shape, imported.at(shape)).first;
        }
        indices.set(offset + row, found->second);
      }
      offset += sources[i].length();
    }
    return Array<Record>{std::move(indices).finish(), std::move(shapes),
                         std::move(names), std::move(columns)};
  }
}

} // namespace

auto concatenate(std::span<Chunk const> chunks) -> Array<Data> {
  auto length = total_length(chunks);
  auto alternatives = storage::Vector<UnionArray::MaskedArray>{};
  auto indices = storage::SparseStorage<storage::Index>::Mutable{length};
  for (auto row = storage::Index{0}; row < length; ++row) {
    indices.set(row, -1);
  }
  auto append = [&]<data_type Tag>() {
    auto selected = std::vector<Option<MaskedArray<Array<Tag>>>>{};
    auto any = false;
    for (auto const& chunk : chunks) {
      auto alternative = chunk.data.template get_alternative<Tag>();
      if (alternative) {
        alternative->present = std::move(alternative->present) & chunk.present;
        any = any or alternative->present.any();
      }
      selected.push_back(std::move(alternative));
    }
    if (not any) {
      return;
    }
    auto typed = std::vector<MaskedArray<Array<Tag>>>{};
    auto present = storage::BitMap::Builder{};
    auto offset = storage::Index{0};
    for (auto i = size_t{0}; i < chunks.size(); ++i) {
      auto count = chunks[i].data.length();
      auto mask
        = selected[i] ? selected[i]->present : storage::BitMap{count, false};
      if (auto constant = mask.as_constant()) {
        present.append_n(*constant, count);
      } else {
        for (auto row = storage::Index{0}; row < count; ++row) {
          present.emplace_back(mask.get(row));
        }
      }
      for (auto row : storage::true_bits(mask)) {
        indices.set(offset + row,
                    detail::narrow<storage::Index>(alternatives.size()));
      }
      auto placeholder = [&]() -> Array<Tag> {
        if constexpr (std::same_as<Tag, Null>) {
          return Array<Null>{storage::NullStorage{count}};
        } else if constexpr (std::same_as<Tag, Bool>) {
          return Array<Bool>{storage::BitMap{count, false}};
        } else {
          return Array<Tag>{
            storage::ConstantStorage<Tag, typename Type<Tag>::ViewType>{count,
                                                                        {}}};
        }
      };
      typed.push_back(
        {selected[i] ? std::move(selected[i]->data) : placeholder(),
         std::move(mask)});
      offset += count;
    }
    alternatives.push_back(
      {ErasedArray{concatenate_values<Tag>(typed, length)}, present.finish()});
  };
  [&]<size_t... Is>(std::index_sequence<Is...>) {
    (append.template operator()<data_type_list::at<Is>>(), ...);
  }(data_type_list::index_sequence);
  if (alternatives.empty()) {
    auto builder = ArrayBuilder<Data>{};
    builder.skip_n(length);
    return builder.finish();
  }
  return Array<Data>{
    UnionArray{std::move(indices).finish(), std::move(alternatives)}};
}

auto concatenate(std::span<Events const> events) -> Events {
  auto data = std::vector<Chunk>{};
  auto names = std::vector<Chunk>{};
  auto times = std::vector<Chunk>{};
  auto internal = std::vector<Chunk>{};
  for (auto const& event : events) {
    TENZIR_ASSERT_EQ(event.active_count(), event.length());
    data.push_back({Array<Data>{event.data}, event.mask});
    names.push_back({Array<Data>{event.meta.name}, event.mask});
    times.push_back({Array<Data>{event.meta.import_time}, event.mask});
    internal.push_back({Array<Data>{event.meta.internal}, event.mask});
  }
  if (total_length(data) == 0) {
    return Events{};
  }
  auto records = concatenate(data).get_alternative<Record>();
  TENZIR_ASSERT(records);
  auto length = records->data.length();
  return {std::move(records->data),
          storage::BitMap{length, true},
          {concatenate(names).get_alternative<String>()->data,
           concatenate(times).get_alternative<Time>()->data,
           concatenate(internal).get_alternative<Bool>()->data}};
}

auto EventsAssembler::append(Events const& input, storage::Index row) -> void {
  TENZIR_ASSERT(input.mask.get(row));
  TENZIR_ASSERT_LT(length_, std::numeric_limits<storage::Index>::max());
  if (sources_.empty() or &sources_.back().input.get() != &input) {
    sources_.push_back({input, {}});
  }
  auto& ranges = sources_.back().ranges;
  if (not ranges.empty() and ranges.back().end == row) {
    ++ranges.back().end;
  } else {
    ranges.push_back({row, row + 1});
  }
  ++length_;
}

auto EventsAssembler::length() const -> storage::Index {
  return length_;
}

auto EventsAssembler::finish() && -> Events {
  auto events = std::vector<Events>{};
  for (auto const& source : sources_) {
    auto data
      = concatenate_ranges(Array<Data>{source.input->data}, source.ranges)
          .get_alternative<Record>()
          ->data;
    auto length = data.length();
    auto select = [&]<data_type Tag>(Array<Tag> const& array) {
      return concatenate_ranges(Array<Data>{array}, source.ranges)
        .template get_alternative<Tag>()
        ->data;
    };
    events.emplace_back(std::move(data), storage::BitMap{length, true},
                        Events::Meta{select(source.input->meta.name),
                                     select(source.input->meta.import_time),
                                     select(source.input->meta.internal)});
  }
  return concatenate(events);
}

} // namespace tenzir::nova
