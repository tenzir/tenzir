#include "tenzir/nova/array_merge.hpp"

#include "tenzir/bitmap.hpp"
#include "tenzir/checked_math.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/shape_table.hpp"
#include "tenzir/nova/storage.hpp"
#include "tenzir/variant_traits.hpp"

#include <cstddef>
#include <ranges>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

namespace tenzir::nova {

namespace {

template <data_type Tag>
struct Alternatives {
  using TagType = Tag;

  Option<MaskedArray<Array<Tag>>> old = None{};
  Option<MaskedArray<Array<Tag>>> new_ = None{};
};

using AlternativeMatches
  = data_type_list::wrap<Alternatives>::apply<std::tuple>;

template <data_type Tag>
auto alternatives(AlternativeMatches& matches) -> Alternatives<Tag>& {
  constexpr auto index = data_type_list::unique_index_of<Tag>;
  return std::get<index>(matches);
}

template <data_type Tag>
auto erase(MaskedArray<Array<Tag>> result) -> MaskedArray<Array<Data>> {
  return {
    .data = Array<Data>{ErasedArray{std::move(result.data)}},
    .present = std::move(result.present),
  };
}

/// The tags whose payload lives in one flat buffer addressed by per-row spans.
template <typename Tag>
concept dense_offset_type = concepts::one_of<Tag, String, Blob>;

template <dense_offset_type Tag>
using DenseOffsetStorageFor = Type<Tag>::PrimaryPhysicalStorage;

template <dense_offset_type Tag>
using ConstantStorageFor
  = storage::ConstantStorage<Tag, typename Type<Tag>::ViewType>;

template <dense_offset_type Tag, class Storage>
auto dense_storage_size(Storage const& storage) -> storage::Index {
  if constexpr (std::same_as<Storage, DenseOffsetStorageFor<Tag>>) {
    return storage.data().length();
  } else {
    static_assert(std::same_as<Storage, ConstantStorageFor<Tag>>);
    if (storage.length() == 0) {
      return 0;
    }
    return detail::narrow<storage::Index>(storage.get(0).size());
  }
}

auto make_union(storage::Vector<UnionArray::MaskedArray> alternatives,
                storage::Index length) -> MaskedArray<Array<Data>> {
  if (alternatives.empty()) {
    return {
      .data = Array<Data>{storage::NullStorage{length}},
      .present = storage::BitMap{length, false},
    };
  }
  if (alternatives.size() == 1) {
    auto alternative = std::move(alternatives.front());
    return {
      .data = Array<Data>{std::move(alternative.data)},
      .present = std::move(alternative.present),
    };
  }
  auto indices = storage::SparseStorage<storage::Index>::Mutable{length};
  auto present = storage::BitMap::Mutable{length};
  for (auto row = storage::Index{0}; row < length; ++row) {
    auto selected = storage::Index{-1};
    for (auto alternative = std::size_t{0}; alternative < alternatives.size();
         ++alternative) {
      if (alternatives[alternative].present.get(row)) {
        TENZIR_ASSERT_EQ(selected, -1);
        selected = static_cast<storage::Index>(alternative);
      }
    }
    indices.set(row, selected);
    present.set(row, selected >= 0);
  }
  return {
    .data = Array<Data>{UnionArray{std::move(indices).finish(),
                                   std::move(alternatives)}},
    .present = std::move(present).finish(),
  };
}

} // namespace

class ArrayMerger {
public:
  auto merge(MaskedArray<Array<Data>> old, MaskedArray<Array<Data>> new_)
    -> MaskedArray<Array<Data>>;

private:
  template <fundamental_type Tag>
  auto
  merge_fundamental(MaskedArray<Array<Tag>> old, MaskedArray<Array<Tag>> new_)
    -> MaskedArray<Array<Tag>>;

  template <data_type Tag>
  auto merge_same(MaskedArray<Array<Tag>> old, MaskedArray<Array<Tag>> new_)
    -> MaskedArray<Array<Tag>>;

  template <data_type OldTag, data_type NewTag>
  auto merge_different(MaskedArray<Array<OldTag>> old,
                       MaskedArray<Array<NewTag>> new_)
    -> MaskedArray<Array<Data>>;

  template <bool IsNew>
  auto collect(MaskedArray<Array<Data>> input, AlternativeMatches& matches)
    -> void;

  auto merge_unions(MaskedArray<Array<Data>> old, MaskedArray<Array<Data>> new_)
    -> MaskedArray<Array<Data>>;

  auto
  merge_records(MaskedArray<Array<Record>> old, MaskedArray<Array<Record>> new_)
    -> MaskedArray<Array<Record>>;

  auto merge_lists(MaskedArray<Array<List>> old, MaskedArray<Array<List>> new_)
    -> MaskedArray<Array<List>>;
};

auto ArrayMerger::merge(MaskedArray<Array<Data>> old,
                        MaskedArray<Array<Data>> new_)
  -> MaskedArray<Array<Data>> {
  TENZIR_ASSERT_EQ(old.data.length(), new_.data.length());
  TENZIR_ASSERT_EQ(old.present.length(), old.data.length());
  TENZIR_ASSERT_EQ(new_.present.length(), new_.data.length());
  old.present = std::move(old.present).and_not(new_.present);
  if (not old.present.any()) {
    return new_;
  }
  if (not new_.present.any()) {
    return old;
  }
  return match(
    std::tie(old.data, new_.data),
    [&]<data_type OldTag, data_type NewTag>(
      Array<OldTag> const& old_data,
      Array<NewTag> const& new_data) -> MaskedArray<Array<Data>> {
      if constexpr (std::same_as<OldTag, NewTag>) {
        return erase(merge_same<OldTag>(
          MaskedArray<Array<OldTag>>{old_data, std::move(old.present)},
          MaskedArray<Array<NewTag>>{new_data, std::move(new_.present)}));
      } else {
        return merge_different(
          MaskedArray<Array<OldTag>>{old_data, std::move(old.present)},
          MaskedArray<Array<NewTag>>{new_data, std::move(new_.present)});
      }
    },
    [&](auto const&, auto const&) {
      return merge_unions(std::move(old), std::move(new_));
    });
}

template <fundamental_type Tag>
auto ArrayMerger::merge_fundamental(MaskedArray<Array<Tag>> old,
                                    MaskedArray<Array<Tag>> new_)
  -> MaskedArray<Array<Tag>> {
  TENZIR_ASSERT_EQ(old.data.length(), new_.data.length());
  auto const length = old.data.length();
  auto present = old.present | new_.present;
  if constexpr (std::same_as<Tag, Null>) {
    return {
      .data = Array<Null>{storage::NullStorage{length}},
      .present = std::move(present),
    };
  } else if constexpr (dense_offset_type<Tag>) {
    auto output = typename DenseOffsetStorageFor<Tag>::Mutable{length};
    match(
      std::tie(old.data.storage(), new_.data.storage()),
      [&](auto const& old_storage, auto const& new_storage) {
        auto const reserved_size
          = checked_add(dense_storage_size<Tag>(old_storage),
                        dense_storage_size<Tag>(new_storage));
        TENZIR_ASSERT(reserved_size);
        output.reserve_data(*reserved_size);
        using OldStorage = std::remove_cvref_t<decltype(old_storage)>;
        if constexpr (std::same_as<OldStorage, DenseOffsetStorageFor<Tag>>) {
          output.copy_from(old_storage);
          auto new_same_as = Option<storage::Index>{None{}};
          for (auto new_row : storage::true_bits(new_.present)) {
            using NewStorage = std::remove_cvref_t<decltype(new_storage)>;
            if constexpr (std::same_as<NewStorage, ConstantStorageFor<Tag>>) {
              if (new_same_as) {
                output.set_same_as(new_row, *new_same_as);
              } else {
                output.set(new_row, new_storage.get(new_row));
                new_same_as.emplace(new_row);
              }
            } else {
              output.set(new_row, new_storage.get(new_row));
            }
          }
        } else {
          auto old_same_as = Option<storage::Index>{None{}};
          auto new_same_as = Option<storage::Index>{None{}};
          auto set_from = [&](storage::Index row, auto const& source,
                              Option<storage::Index>& same_as) {
            using Storage = std::remove_cvref_t<decltype(source)>;
            if constexpr (std::same_as<Storage, ConstantStorageFor<Tag>>) {
              if (same_as) {
                output.set_same_as(row, *same_as);
              } else {
                output.set(row, source.get(row));
                same_as.emplace(row);
              }
            } else {
              output.set(row, source.get(row));
            }
          };
          for (auto [old_row, new_row] :
               std::views::zip(storage::bitmap_iteration(old.present),
                               storage::bitmap_iteration(new_.present))) {
            if (new_row) {
              set_from(*new_row, new_storage, new_same_as);
            } else if (old_row) {
              set_from(*old_row, old_storage, old_same_as);
            }
          }
        }
      });
    return {
      .data = Array<Tag>{std::move(output).finish(true)},
      .present = std::move(present),
    };
  } else {
    auto output = typename Type<Tag>::PrimaryPhysicalStorage::Mutable{length};
    match(
      std::tie(old.data.storage(), new_.data.storage()),
      [&](auto const& old_storage, auto const& new_storage) {
        using OldStorage = std::remove_cvref_t<decltype(old_storage)>;
        using PrimaryStorage = Type<Tag>::PrimaryPhysicalStorage;
        if constexpr (std::same_as<OldStorage, PrimaryStorage>) {
          output.copy_from(old_storage);
          for (auto new_row : storage::true_bits(new_.present)) {
            output.set(new_row, static_cast<typename Type<Tag>::ViewType>(
                                  new_storage.get(new_row)));
          }
        } else {
          for (auto [old_row, new_row] :
               std::views::zip(storage::bitmap_iteration(old.present),
                               storage::bitmap_iteration(new_.present))) {
            if (new_row) {
              output.set(*new_row, static_cast<typename Type<Tag>::ViewType>(
                                     new_storage.get(*new_row)));
            } else if (old_row) {
              output.set(*old_row, static_cast<typename Type<Tag>::ViewType>(
                                     old_storage.get(*old_row)));
            }
          }
        }
      });
    return {
      .data = Array<Tag>{std::move(output).finish()},
      .present = std::move(present),
    };
  }
}

template <data_type Tag>
auto ArrayMerger::merge_same(MaskedArray<Array<Tag>> old,
                             MaskedArray<Array<Tag>> new_)
  -> MaskedArray<Array<Tag>> {
  TENZIR_ASSERT_EQ(old.data.length(), new_.data.length());
  if (not old.present.any()) {
    return new_;
  }
  if (not new_.present.any()) {
    return old;
  }
  if constexpr (fundamental_type<Tag>) {
    return merge_fundamental(std::move(old), std::move(new_));
  } else if constexpr (std::same_as<Tag, Record>) {
    return merge_records(std::move(old), std::move(new_));
  } else {
    static_assert(std::same_as<Tag, List>);
    return merge_lists(std::move(old), std::move(new_));
  }
}

template <data_type OldTag, data_type NewTag>
auto ArrayMerger::merge_different(MaskedArray<Array<OldTag>> old,
                                  MaskedArray<Array<NewTag>> new_)
  -> MaskedArray<Array<Data>> {
  static_assert(not std::same_as<OldTag, NewTag>);
  TENZIR_ASSERT_EQ(old.data.length(), new_.data.length());
  auto const length = old.data.length();
  auto alternatives = storage::Vector<UnionArray::MaskedArray>{};
  alternatives.reserve(2);
  alternatives.push_back(UnionArray::MaskedArray{
    .data = ErasedArray{std::move(old.data)},
    .present = std::move(old.present),
  });
  alternatives.push_back(UnionArray::MaskedArray{
    .data = ErasedArray{std::move(new_.data)},
    .present = std::move(new_.present),
  });
  return make_union(std::move(alternatives), length);
}

template <bool IsNew>
auto ArrayMerger::collect(MaskedArray<Array<Data>> input,
                          AlternativeMatches& matches) -> void {
  if (not input.present.any()) {
    return;
  }
  match(
    std::move(input.data),
    [&](UnionArray array) {
      for (auto const& alternative : array.fields()) {
        auto present = input.present & alternative.present;
        if (not present.any()) {
          continue;
        }
        match(alternative.data, [&]<data_type Tag>(Array<Tag> data) {
          auto& destination = alternatives<Tag>(matches);
          if constexpr (IsNew) {
            TENZIR_ASSERT(not destination.new_);
            destination.new_.emplace(
              MaskedArray<Array<Tag>>{std::move(data), std::move(present)});
          } else {
            TENZIR_ASSERT(not destination.old);
            destination.old.emplace(
              MaskedArray<Array<Tag>>{std::move(data), std::move(present)});
          }
        });
      }
    },
    [&]<data_type Tag>(Array<Tag> data) {
      auto& destination = alternatives<Tag>(matches);
      if constexpr (IsNew) {
        TENZIR_ASSERT(not destination.new_);
        destination.new_.emplace(
          MaskedArray<Array<Tag>>{std::move(data), std::move(input.present)});
      } else {
        TENZIR_ASSERT(not destination.old);
        destination.old.emplace(
          MaskedArray<Array<Tag>>{std::move(data), std::move(input.present)});
      }
    });
}

auto ArrayMerger::merge_unions(MaskedArray<Array<Data>> old,
                               MaskedArray<Array<Data>> new_)
  -> MaskedArray<Array<Data>> {
  auto const length = old.data.length();
  auto matches = AlternativeMatches{};
  collect<false>(std::move(old), matches);
  collect<true>(std::move(new_), matches);
  auto result = storage::Vector<UnionArray::MaskedArray>{};
  result.reserve(data_type_list::size);
  std::apply(
    [&](auto&... matched) {
      auto finish = [&]<class Match>(Match& entry) {
        using Tag = typename Match::TagType;
        if (not entry.old and not entry.new_) {
          return;
        }
        auto merged = [&]() -> MaskedArray<Array<Tag>> {
          if (entry.old and entry.new_) {
            return merge_same<Tag>(std::move(*entry.old),
                                   std::move(*entry.new_));
          }
          if (entry.old) {
            return std::move(*entry.old);
          }
          TENZIR_ASSERT(entry.new_);
          return std::move(*entry.new_);
        }();
        if (not merged.present.any()) {
          return;
        }
        result.push_back(UnionArray::MaskedArray{
          .data = ErasedArray{std::move(merged.data)},
          .present = std::move(merged.present),
        });
      };
      (finish(matched), ...);
    },
    matches);
  return make_union(std::move(result), length);
}

auto ArrayMerger::merge_records(MaskedArray<Array<Record>> old,
                                MaskedArray<Array<Record>> new_)
  -> MaskedArray<Array<Record>> {
  TENZIR_ASSERT_EQ(old.data.length(), new_.data.length());
  auto const length = old.data.length();
  auto present = old.present | new_.present;
  if (&old.data.storage() == &new_.data.storage()) {
    return {
      .data = std::move(old.data),
      .present = std::move(present),
    };
  }
  old.data = old.data.to_primary();
  new_.data = new_.data.to_primary();
  auto const& old_storage
    = as<storage::RecordStorage>(old.data.storage()).data();
  auto const& new_storage
    = as<storage::RecordStorage>(new_.data.storage()).data();
  auto names = old_storage.names;
  auto shape_table = old_storage.shape_table;
  auto arrays = old_storage.arrays;
  for (auto& field : arrays) {
    field.present = std::move(field.present) & old.present;
  }
  auto new_field_remap = storage::Vector<storage::Index>{};
  new_field_remap.resize(new_storage.arrays.size(), -1);
  for (auto i = std::size_t{0}; i < new_storage.arrays.size(); ++i) {
    auto const name = new_storage.names_by_index[i];
    auto field = new_storage.arrays[i];
    field.present = std::move(field.present) & new_.present;
    auto const found = names.find(name);
    if (found != names.end()) {
      auto const destination = static_cast<storage::Index>(found->second);
      new_field_remap[i] = destination;
      arrays[found->second]
        = merge(std::move(arrays[found->second]), std::move(field));
      continue;
    }
    auto const destination = static_cast<storage::Index>(arrays.size());
    new_field_remap[i] = destination;
    arrays.push_back(std::move(field));
    names.try_emplace(storage::String<>{name},
                      static_cast<std::size_t>(destination));
  }
  auto selected_shapes = storage::Vector<ShapeTable::ShapeId>{};
  auto seen_shapes = storage::UnorderedMap<ShapeTable::ShapeId, bool>{};
  for (auto row = storage::Index{0}; row < length; ++row) {
    if (not new_.present.get(row)) {
      continue;
    }
    auto const shape = new_storage.shape_indices.get(row);
    TENZIR_ASSERT_LEQ(0, shape);
    if (seen_shapes.try_emplace(shape, true).second) {
      selected_shapes.push_back(shape);
    }
  }
  auto const shape_remap = shape_table.import_shapes(
    new_storage.shape_table, new_field_remap, selected_shapes);
  auto shape_indices = Array<Record>::IndicesStorage::Mutable{length};
  for (auto row = storage::Index{0}; row < length; ++row) {
    if (new_.present.get(row)) {
      auto const shape = new_storage.shape_indices.get(row);
      TENZIR_ASSERT_LEQ(0, shape);
      auto const found = shape_remap.find(shape);
      TENZIR_ASSERT(found != shape_remap.end());
      shape_indices.set(row, found->second);
    } else if (old.present.get(row)) {
      auto const shape = old_storage.shape_indices.get(row);
      TENZIR_ASSERT_LEQ(0, shape);
      shape_indices.set(row, shape);
    } else {
      shape_indices.set(row, -1);
    }
  }
  return {
    .data
    = Array<Record>{std::move(shape_indices).finish(), std::move(shape_table),
                    std::move(names), std::move(arrays)},
    .present = std::move(present),
  };
}

auto ArrayMerger::merge_lists(MaskedArray<Array<List>> old,
                              MaskedArray<Array<List>> new_)
  -> MaskedArray<Array<List>> {
  TENZIR_ASSERT_EQ(old.data.length(), new_.data.length());
  auto const length = old.data.length();
  auto present = old.present | new_.present;
  old.data = old.data.to_primary();
  new_.data = new_.data.to_primary();
  auto const& old_storage = as<storage::ListStorage>(old.data.storage());
  auto const& new_storage = as<storage::ListStorage>(new_.data.storage());
  auto aligned = true;
  for (auto row = storage::Index{0}; row < length; ++row) {
    if (old.data.get(row).length() != new_.data.get(row).length()) {
      aligned = false;
      break;
    }
  }
  if (aligned) {
    // TODO: This will explode if it ever encounters a constant repr
    auto const values_length = old_storage.values().length();
    TENZIR_ASSERT_EQ(values_length, new_storage.values().length());
    auto old_values_present = storage::BitMap::Mutable{values_length};
    auto new_values_present = storage::BitMap::Mutable{values_length};
    auto const& spans = old_storage.spans();
    for (auto row = storage::Index{0}; row < length; ++row) {
      auto const& [begin, end] = spans[row];
      for (auto value = begin; value < end; ++value) {
        old_values_present.set(value, old.present.get(row));
        new_values_present.set(value, new_.present.get(row));
      }
    }
    auto values
      = merge(MaskedArray<Array<Data>>{old_storage.values(),
                                       std::move(old_values_present).finish()},
              MaskedArray<Array<Data>>{new_storage.values(),
                                       std::move(new_values_present).finish()});
    return {
      .data = Array<List>{old_storage.spans(), std::move(values.data)},
      .present = std::move(present),
    };
  }
  auto builder = ArrayBuilder<List>{};
  auto append = [&](RowView<List> source) {
    auto destination = builder.list();
    for (auto value : source) {
      append_row(destination, value);
    }
  };
  for (auto row = storage::Index{0}; row < length; ++row) {
    if (new_.present.get(row)) {
      append(new_.data.get(row));
    } else if (old.present.get(row)) {
      append(old.data.get(row));
    } else {
      builder.skip();
    }
  }
  return {
    .data = builder.finish(),
    .present = std::move(present),
  };
}

auto with_merged(MaskedArray<Array<Data>> const& old,
                 MaskedArray<Array<Data>> const& new_) -> Array<Data> {
  TENZIR_ASSERT_EQ(old.data.length(), new_.data.length());
  TENZIR_ASSERT_EQ(old.present.length(), old.data.length());
  TENZIR_ASSERT_EQ(new_.present.length(), new_.data.length());
  if (auto constant = new_.present.as_constant(); constant and not *constant) {
    return old.data;
  }
  if (auto constant = new_.present.as_constant(); constant and *constant) {
    return new_.data;
  }
  return ArrayMerger{}.merge(old, new_).data;
}

} // namespace tenzir::nova
