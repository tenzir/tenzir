//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/event_builder.hpp"

#include "tenzir/cast.hpp"
#include "tenzir/data.hpp"
#include "tenzir/data_builder.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/array_merge.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/shape_table.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <concepts>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tenzir::nova {

namespace {

/// Returns the seed that values of a field typed `seed` are held to, where a
/// null type leaves them unconstrained, or `None` if they must be discarded.
/// With `schema_only`, the values of a null-typed field are discarded, which
/// leaves the field null, and those of a map- or secret-typed field become
/// null as mismatches.
auto effective(type seed, bool schema_only) -> Option<type> {
  if (is<null_type>(seed)) {
    if (schema_only) {
      return None{};
    }
    return type{};
  }
  if (not schema_only and (is<map_type>(seed) or is<secret_type>(seed))) {
    return type{};
  }
  return seed;
}

template <class V>
auto kind_of() -> type_kind {
  if constexpr (std::same_as<V, std::string_view>) {
    return type_kind::of<string_type>;
  } else if constexpr (std::same_as<V, blob_view>) {
    return type_kind::of<blob_type>;
  } else if constexpr (std::same_as<V, SecretView>) {
    return type_kind::of<secret_type>;
  } else {
    return type_kind::of<data_to_type_t<V>>;
  }
}

/// Returns whether a value viewed as `V` already conforms to `seed`.
template <class V>
auto conforms(type const& seed) -> bool {
  if constexpr (std::same_as<V, std::string_view>) {
    return is<string_type>(seed);
  } else if constexpr (std::same_as<V, blob_view>) {
    return is<blob_type>(seed);
  } else if constexpr (std::same_as<V, SecretView>) {
    return false;
  } else {
    return is<data_to_type_t<V>>(seed);
  }
}

auto emit_mismatch(diagnostic_handler& dh, value_path const& path,
                   type_kind value, type const& seed) -> void {
  diagnostic::warning("parsed field `{}` contains `{}`, but the schema "
                      "expects `{}`",
                      path, value, seed.kind())
    .emit(dh);
}

/// Writes a value produced by the legacy parsers. Nova has no enumerations,
/// so they become their name.
template <class Out>
auto append_parsed(Out& out, data const& value, type const& seed) -> void {
  match(
    value,
    [&](caf::none_t) {
      out.null();
    },
    [&](std::string const& x) {
      out.data(std::string_view{x});
    },
    [&](blob const& x) {
      out.data(blob_view{x});
    },
    [&](enumeration x) {
      out.data(as<enumeration_type>(seed).field(x));
    },
    [&]<class T>(T const& x)
      requires concepts::one_of<T, bool, int64_t, uint64_t, double, duration,
                                time, ip, subnet>
    {
      out.data(x);
    },
    [&](auto const&) {
      out.null();
    });
}

/// Writes a string whose type the input did not specify, inferring bool,
/// time, duration, subnet and ip unless `raw` is set.
template <class Out>
auto append_inferred(Out& out, std::string_view value, bool raw) -> void {
  if (raw) {
    out.data(value);
    return;
  }
  auto parsed
    = detail::data_builder::non_number_parser(value, nullptr, value_path{});
  if (parsed.data) {
    append_parsed(out, *parsed.data, type{});
    return;
  }
  out.data(value);
}

/// Writes `value` converted to `seed`, which must not be a null type. Strings
/// are parsed according to `seed`. Values that cannot be converted become
/// null with a warning.
template <class Out, class V>
auto append_coerced(Out& out, V value, type const& seed, value_path const& path,
                    diagnostic_handler& dh, bool schema_only) -> void {
  if (conforms<V>(seed)) {
    out.data(value);
    return;
  }
  if constexpr (std::same_as<V, std::string_view>) {
    auto parsed = detail::data_builder::non_number_parser(value, &seed, path);
    if (parsed.diagnostic) {
      dh.emit(std::move(*parsed.diagnostic));
    }
    if (parsed.data) {
      append_parsed(out, *parsed.data, seed);
    } else if (parsed.diagnostic) {
      schema_only ? out.null() : out.data(value);
    } else {
      out.data(value);
    }
  } else {
    auto mismatch = [&] {
      emit_mismatch(dh, path, kind_of<V>(), seed);
      schema_only ? out.null() : out.data(value);
    };
    constexpr auto is_number = concepts::one_of<V, int64_t, uint64_t, double>;
    match(
      seed,
      [&](int64_type const&) {
        if constexpr (std::same_as<V, uint64_t>) {
          if (value
              > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            mismatch();
          } else {
            out.data(static_cast<int64_t>(value));
          }
        } else {
          mismatch();
        }
      },
      [&](uint64_type const&) {
        if constexpr (std::same_as<V, int64_t>) {
          if (value < 0) {
            mismatch();
          } else {
            out.data(static_cast<uint64_t>(value));
          }
        } else {
          mismatch();
        }
      },
      [&](double_type const&) {
        if constexpr (concepts::one_of<V, int64_t, uint64_t>) {
          out.data(static_cast<double>(value));
        } else {
          mismatch();
        }
      },
      [&](duration_type const&) {
        if constexpr (is_number) {
          auto unit = seed.attribute("unit").value_or("s");
          auto result
            = cast_value(data_to_type_t<V>{}, value, duration_type{}, unit);
          if (result) {
            out.data(*result);
            return;
          }
        }
        mismatch();
      },
      [&](time_type const&) {
        if constexpr (is_number) {
          auto unit = seed.attribute("unit");
          if (not unit) {
            diagnostic::warning("could not parse value as `{}`", time_type{})
              .note("the value was read as a number, but the schema does not "
                    "specify a unit")
              .note("field `{}`", path)
              .emit(dh);
            schema_only ? out.null() : out.data(value);
            return;
          }
          auto result
            = cast_value(data_to_type_t<V>{}, value, duration_type{}, *unit);
          if (result) {
            out.data(time{} + *result);
            return;
          }
        }
        mismatch();
      },
      [&](string_type const&) {
        if constexpr (not std::same_as<V, SecretView>
                      and fmt::is_formattable<V>::value) {
          out.data(std::string_view{fmt::format("{}", value)});
        } else {
          mismatch();
        }
      },
      [&](enumeration_type const& e) {
        if constexpr (concepts::one_of<V, int64_t, uint64_t>) {
          auto out_of_range
            = value > static_cast<V>(std::numeric_limits<enumeration>::max());
          if constexpr (std::same_as<V, int64_t>) {
            out_of_range = out_of_range or value < 0;
          }
          if (out_of_range) {
            diagnostic::warning("value is out of range for expected type")
              .note("value `{}` does not fit into `{}`", value,
                    type_kind::of<enumeration_type>)
              .note("field `{}`", path)
              .emit(dh);
            schema_only ? out.null() : out.data(value);
            return;
          }
          auto name = e.field(static_cast<uint32_t>(value));
          if (name.empty()) {
            diagnostic::warning("unknown integral enumeration value")
              .note("value `{}` is not defined for `{}`", value, e)
              .note("field `{}`", path)
              .emit(dh);
            schema_only ? out.null() : out.data(value);
            return;
          }
          out.data(name);
        } else {
          mismatch();
        }
      },
      [&](auto const&) {
        mismatch();
      });
  }
}

/// Converts the fields of already built records to a schema. Only the rows
/// that are selected change, and columns whose values already conform to the
/// schema are not rebuilt.
class Retyper {
public:
  enum class Mode { full, common_fields_only };

  Retyper(bool schema_only, bool infer, diagnostic_handler& dh,
          Mode mode = Mode::full)
    : schema_only_{schema_only}, infer_{infer}, dh_{dh}, mode_{mode} {
  }

  /// Converts the records in `rows` to `seed`, which is a `record_type` or a
  /// null type. The records must be present in `rows`. With a schema, fields
  /// of the schema that a record lacks become null, the fields of the schema
  /// come first, and with `schema_only` other fields are dropped.
  auto record(Array<Record> array, storage::BitMap const& rows,
              type const& seed, value_path const& path) -> Array<Record> {
    if (not rows.any() or (mode_ == Mode::common_fields_only and not seed)) {
      return array;
    }
    array = std::move(array).to_primary();
    auto const& storage = *as<storage::RecordStorage>(array.storage());
    auto const* schema = seed ? &as<record_type>(seed) : nullptr;
    auto const length = array.length();
    auto names = storage.names;
    auto arrays = storage.arrays;
    auto changed = false;
    auto dropped = std::vector<bool>(arrays.size(), false);
    // Convert the existing fields in order of their first appearance, so that
    // diagnostics are in that order, too.
    for (auto index = std::size_t{0}; index < storage.arrays.size(); ++index) {
      auto const name = storage.names_by_index[index];
      if (auto it = storage.names.find(name);
          it == storage.names.end() or it->second != index) {
        // The field was removed.
        continue;
      }
      auto& column = arrays[index];
      auto selected = column.present & rows;
      if (not selected.any()) {
        continue;
      }
      auto field_seed = Option<type>{type{}};
      if (schema) {
        if (auto field = schema->resolve_field(name)) {
          field_seed = effective(schema->field(*field).type, schema_only_);
        } else if (mode_ == Mode::common_fields_only) {
          continue;
        } else if (schema_only_) {
          field_seed = None{};
        }
      }
      if (not field_seed) {
        column.present = std::move(column.present).and_not(rows);
        dropped[index] = true;
        changed = true;
        continue;
      }
      if (auto result
          = this->column(column, selected, *field_seed, path.field(name))) {
        column = std::move(*result);
        changed = true;
      }
    }
    if (not schema or mode_ == Mode::common_fields_only) {
      if (not changed) {
        return array;
      }
      return Array<Record>{storage.shape_indices,
                           ShapeTable{storage.shape_table}, std::move(names),
                           std::move(arrays)};
    }
    // Add the fields of the schema that records lack as null.
    auto schema_indices = std::vector<storage::Index>{};
    schema_indices.reserve(schema->num_fields());
    for (auto&& field : schema->fields()) {
      auto nulls = [&](storage::BitMap present) {
        return MaskedArray<Array<Data>>{
          Array<Data>{Array<Null>{storage::NullStorage{length}}},
          std::move(present)};
      };
      auto it = names.find(field.name);
      if (it == names.end()) {
        arrays.push_back(nulls(rows));
        dropped.push_back(false);
        it = names.try_emplace(storage::String<>{field.name}, arrays.size() - 1)
               .first;
      } else if (auto missing = rows.and_not(arrays[it->second].present);
                 missing.any()) {
        auto& column = arrays[it->second];
        column.data = with_merged(column, nulls(missing));
        column.present = column.present | missing;
      }
      schema_indices.push_back(static_cast<storage::Index>(it->second));
    }
    // Put the fields of the schema first, which only needs to be computed
    // once per distinct shape.
    auto in_schema = std::vector<bool>(arrays.size(), false);
    for (auto index : schema_indices) {
      in_schema[static_cast<std::size_t>(index)] = true;
    }
    auto shape_table = ShapeTable{storage.shape_table};
    auto shapes
      = std::unordered_map<ShapeTable::ShapeId, ShapeTable::ShapeId>{};
    auto shape_indices
      = Array<Record>::IndicesStorage::Mutable{storage.shape_indices};
    for (auto row : storage::true_bits(rows)) {
      auto const old_shape = storage.shape_indices.get(row);
      TENZIR_ASSERT_LEQ(0, old_shape);
      auto [it, inserted] = shapes.try_emplace(old_shape, old_shape);
      if (inserted) {
        auto const old_fields = shape_table.fields(old_shape);
        auto const rest
          = std::vector<storage::Index>{old_fields.begin(), old_fields.end()};
        auto shape = ShapeTable::empty_shape;
        for (auto index : schema_indices) {
          shape = shape_table.with_field(shape, index);
        }
        for (auto index : rest) {
          auto const i = static_cast<std::size_t>(index);
          if (not in_schema[i] and not dropped[i]) {
            shape = shape_table.with_field(shape, index);
          }
        }
        it->second = shape;
      }
      shape_indices.set(row, it->second);
    }
    return Array<Record>{std::move(shape_indices).finish(),
                         std::move(shape_table), std::move(names),
                         std::move(arrays)};
  }

private:
  /// Converts the values of `column` in `selected` to `seed`, which may be a
  /// null type. Returns nothing if the column does not change.
  auto column(MaskedArray<Array<Data>> const& column,
              storage::BitMap const& selected, type const& seed,
              value_path const& path) -> Option<MaskedArray<Array<Data>>> {
    auto const length = column.data.length();
    auto data = column.data;
    auto changed = false;
    // Descend into records and lists that may conform to the seed.
    if (not seed or is<record_type>(seed)) {
      auto records = data.get_alternative<Record>();
      if (records and (records->present & selected).any()) {
        data = std::move(data).map_alternative<Record>(
          [&](MaskedArray<Array<Record>> alternative) {
            return record(std::move(alternative.data),
                          alternative.present & selected, seed, path);
          });
        changed = true;
      }
    }
    if (not seed or is<list_type>(seed)) {
      auto lists = data.get_alternative<List>();
      if (lists and (lists->present & selected).any()) {
        auto element
          = seed ? effective(as<list_type>(seed).value_type(), schema_only_)
                     .value_or(type{})
                 : type{};
        data = std::move(data).map_alternative<List>(
          [&](MaskedArray<Array<List>> alternative) {
            return list(std::move(alternative.data),
                        alternative.present & selected, element, path);
          });
        changed = true;
      }
    }
    // Rebuild the values whose type changes.
    auto replace = storage::BitMap{length, false};
    [&]<std::size_t... Is>(std::index_sequence<Is...>) {
      auto check = [&]<class Tag>() {
        if (not needs_replacement<Tag>(seed)) {
          return;
        }
        if (auto alternative = data.template get_alternative<Tag>()) {
          replace = replace | (alternative->present & selected);
        }
      };
      (check.template operator()<data_type_list::at<Is>>(), ...);
    }(data_type_list::index_sequence);
    if (replace.any()) {
      auto builder = ArrayBuilder<Data>{};
      for (auto row : storage::true_bits(replace)) {
        builder.skip_n(row - builder.length());
        append(builder, data.get(row), seed, path);
      }
      builder.skip_n(length - builder.length());
      data = with_merged(MaskedArray<Array<Data>>{data, column.present},
                         MaskedArray<Array<Data>>{builder.finish(), replace});
      changed = true;
    }
    if (not changed) {
      return None{};
    }
    return MaskedArray<Array<Data>>{std::move(data), column.present};
  }

  auto list(Array<List> array, storage::BitMap const& rows, type const& element,
            value_path const& path) -> Array<List> {
    array = array.to_primary();
    auto const& storage = as<storage::ListStorage>(array.storage());
    auto const& values = storage.values();
    auto const& spans = storage.spans();
    auto elements = storage::BitMap::Mutable{values.length()};
    for (auto row : storage::true_bits(rows)) {
      auto const& [begin, end] = spans[row];
      for (auto i = begin; i < end; ++i) {
        elements.set(i, true);
      }
    }
    auto const selected = std::move(elements).finish();
    auto result = column(
      MaskedArray<Array<Data>>{values, storage::BitMap{values.length(), true}},
      selected, element, path.list());
    if (not result) {
      return array;
    }
    return Array<List>{spans, std::move(result->data)};
  }

  template <class Tag>
  auto needs_replacement(type const& seed) const -> bool {
    if constexpr (std::same_as<Tag, Null>) {
      return false;
    } else if constexpr (std::same_as<Tag, Record>) {
      return seed and not is<record_type>(seed);
    } else if constexpr (std::same_as<Tag, List>) {
      return seed and not is<list_type>(seed);
    } else {
      if (not seed) {
        return std::same_as<Tag, String> and infer_;
      }
      return not conforms<typename Type<Tag>::ViewType>(seed);
    }
  }

  auto append(ArrayBuilder<Data>& builder, RowView<Data> const& row,
              type const& seed, value_path const& path) -> void {
    match(row, [&]<class Tag>(RowView<Tag> view) {
      if constexpr (std::same_as<Tag, Null>) {
        builder.null();
      } else if constexpr (std::same_as<Tag, Record>) {
        emit_mismatch(*dh_, path, type_kind::of<record_type>, seed);
        if (schema_only_) {
          builder.null();
        } else {
          append_row(builder, row);
        }
      } else if constexpr (std::same_as<Tag, List>) {
        emit_mismatch(*dh_, path, type_kind::of<list_type>, seed);
        if (schema_only_) {
          builder.null();
        } else {
          append_row(builder, row);
        }
      } else if constexpr (std::same_as<Tag, String>) {
        if (seed) {
          append_coerced(builder, *view, seed, path, *dh_, schema_only_);
        } else {
          append_inferred(builder, *view, false);
        }
      } else {
        append_coerced(builder, *view, seed, path, *dh_, schema_only_);
      }
    });
  }

  bool schema_only_;
  bool infer_;
  Ref<diagnostic_handler> dh_;
  Mode mode_;
};

/// Returns the seed of the field `name` of a record seeded with `seed`, or
/// `None` if the field must be discarded.
auto field_seed(type const& seed, std::string_view name, bool schema_only)
  -> Option<type> {
  if (not seed) {
    return type{};
  }
  auto const& schema = as<record_type>(seed);
  if (auto index = schema.resolve_field(name)) {
    return effective(schema.field(*index).type, schema_only);
  }
  if (schema_only) {
    return None{};
  }
  return type{};
}

} // namespace

// -- Record ------------------------------------------------------------------

EventBuilder::Record::Record(
  EventBuilder& parent, Option<ArrayBuilder<nova::Record>::RecordBuilder> inner,
  type seed, value_path path, State state)
  : parent_{parent},
    inner_{std::move(inner)},
    seed_{std::move(seed)},
    path_{path} {
  if (state == State::reopened or not inner_ or not seed_) {
    return;
  }
  for (auto field : as<record_type>(seed_).fields()) {
    inner_->field(field.name).null();
  }
}

auto EventBuilder::Record::field(std::string_view name) -> Field {
  auto const path = path_.field(name);
  if (parent_->settings_.unflatten_separator.empty()) {
    return lookup(name, path);
  }
  return descend(name, path);
}

auto EventBuilder::Record::descend(std::string_view key, value_path const& path)
  -> Field {
  auto const& separator = parent_->settings_.unflatten_separator;
  auto const index = key.find(separator);
  if (index == key.npos) {
    return lookup(key, path);
  }
  return lookup_record(key.substr(0, index), path)
    .descend(key.substr(index + separator.size()), path);
}

auto EventBuilder::Record::lookup_record(std::string_view name,
                                         value_path const& path) -> Record {
  if (not inner_) {
    return Record{*parent_, None{}, type{}, path};
  }
  auto seed = field_seed(seed_, name, parent_->settings_.schema_only);
  if (not seed) {
    return Record{*parent_, None{}, type{}, path};
  }
  // Keys that share a prefix, such as `a.b` and `a.c`, continue the record
  // that is still open instead of repeating the key.
  if (not *seed or is<record_type>(*seed)) {
    if (auto record = parent_->open_record(*inner_, name)) {
      return Record{*parent_, std::move(*record), std::move(*seed), path,
                    State::reopened};
    }
  }
  return lookup(name, path).record();
}

auto EventBuilder::Record::lookup(std::string_view name, value_path const& path)
  -> Field {
  if (not inner_) {
    return Field{*parent_, None{}, type{}, path};
  }
  auto seed = field_seed(seed_, name, parent_->settings_.schema_only);
  if (not seed) {
    return Field{*parent_, None{}, type{}, path};
  }
  auto previous = Option<Data>{};
  auto extend = false;
  auto inner = parent_->take(*inner_, name, previous, extend);
  return Field{*parent_, std::move(inner),    std::move(*seed),
               path,     std::move(previous), extend};
}

// -- Field -------------------------------------------------------------------

EventBuilder::Field::Field(EventBuilder& parent, Option<FieldBuilder> inner,
                           type seed, value_path path, Option<Data> previous,
                           bool extend)
  : parent_{parent},
    inner_{std::move(inner)},
    seed_{std::move(seed)},
    path_{path},
    previous_{std::move(previous)},
    extend_{extend} {
}

auto EventBuilder::Field::repeat() -> List {
  TENZIR_ASSERT(inner_);
  TENZIR_ASSERT(previous_);
  auto previous = std::move(*previous_);
  previous_ = None{};
  auto element = seed_;
  if (auto const* list = try_as<list_type>(seed_)) {
    element = effective(list->value_type(), parent_->settings_.schema_only)
                .value_or(type{});
  }
  auto list = inner_->list();
  if (extend_) {
    for (auto const& value : as<nova::List>(previous)) {
      append_data(list, value);
    }
  } else {
    append_data(list, previous);
  }
  return List{*parent_, std::move(list), std::move(element), path_};
}

template <fundamental_view_type V>
auto EventBuilder::Field::data(V v) -> void {
  if (not inner_) {
    return;
  }
  if (previous_) {
    repeat().data(v);
    return;
  }
  if (not seed_) {
    inner_->data(v);
    return;
  }
  append_coerced(*inner_, v, seed_, path_, *parent_->dh_,
                 parent_->settings_.schema_only);
}

auto EventBuilder::Field::data_unparsed(std::string_view v) -> void {
  if (not inner_) {
    return;
  }
  if (previous_) {
    repeat().data_unparsed(v);
    return;
  }
  if (not seed_) {
    append_inferred(*inner_, v, parent_->raw_);
    return;
  }
  append_coerced(*inner_, v, seed_, path_, *parent_->dh_,
                 parent_->settings_.schema_only);
}

auto EventBuilder::Field::null() -> void {
  if (not inner_) {
    return;
  }
  if (previous_) {
    repeat().null();
    return;
  }
  inner_->null();
}

auto EventBuilder::Field::record() -> Record {
  if (not inner_) {
    return Record{*parent_, None{}, type{}, path_};
  }
  if (previous_) {
    return repeat().record();
  }
  if (seed_ and not is<record_type>(seed_)) {
    emit_mismatch(*parent_->dh_, path_, type_kind::of<record_type>, seed_);
    if (not parent_->settings_.schema_only) {
      return Record{*parent_, inner_->record(), type{}, path_};
    }
    inner_->null();
    return Record{*parent_, None{}, type{}, path_};
  }
  return Record{*parent_, inner_->record(), seed_, path_};
}

auto EventBuilder::Field::list() -> List {
  if (not inner_) {
    return List{*parent_, None{}, type{}, path_};
  }
  if (previous_) {
    return repeat().list();
  }
  if (seed_ and not is<list_type>(seed_)) {
    emit_mismatch(*parent_->dh_, path_, type_kind::of<list_type>, seed_);
    if (not parent_->settings_.schema_only) {
      return List{*parent_, inner_->list(), type{}, path_};
    }
    inner_->null();
    return List{*parent_, None{}, type{}, path_};
  }
  auto element = seed_ ? effective(as<list_type>(seed_).value_type(),
                                   parent_->settings_.schema_only)
                           .value_or(type{})
                       : type{};
  return List{*parent_, inner_->list(), std::move(element), path_};
}

template auto EventBuilder::Field::data(bool) -> void;
template auto EventBuilder::Field::data(int64_t) -> void;
template auto EventBuilder::Field::data(uint64_t) -> void;
template auto EventBuilder::Field::data(double) -> void;
template auto EventBuilder::Field::data<std::string_view>(std::string_view)
  -> void;
template auto EventBuilder::Field::data(blob_view) -> void;
template auto EventBuilder::Field::data(SecretView) -> void;
template auto EventBuilder::Field::data(ip) -> void;
template auto EventBuilder::Field::data(subnet) -> void;
template auto EventBuilder::Field::data<time>(time) -> void;
template auto EventBuilder::Field::data(duration) -> void;

// -- List --------------------------------------------------------------------

EventBuilder::List::List(EventBuilder& parent,
                         Option<ArrayBuilder<nova::List>::ListBuilder> inner,
                         type seed, value_path path)
  : parent_{parent},
    inner_{std::move(inner)},
    seed_{std::move(seed)},
    path_{path} {
}

template <fundamental_view_type V>
auto EventBuilder::List::data(V v) -> void {
  if (not inner_) {
    return;
  }
  if (not seed_) {
    inner_->data(v);
    return;
  }
  append_coerced(*inner_, v, seed_, path_.list(), *parent_->dh_,
                 parent_->settings_.schema_only);
}

auto EventBuilder::List::data_unparsed(std::string_view v) -> void {
  if (not inner_) {
    return;
  }
  if (not seed_) {
    append_inferred(*inner_, v, parent_->raw_);
    return;
  }
  append_coerced(*inner_, v, seed_, path_.list(), *parent_->dh_,
                 parent_->settings_.schema_only);
}

auto EventBuilder::List::null() -> void {
  if (inner_) {
    inner_->null();
  }
}

auto EventBuilder::List::record() -> Record {
  if (not inner_) {
    return Record{*parent_, None{}, type{}, path_.list()};
  }
  if (seed_ and not is<record_type>(seed_)) {
    emit_mismatch(*parent_->dh_, path_.list(), type_kind::of<record_type>,
                  seed_);
    if (not parent_->settings_.schema_only) {
      return Record{*parent_, inner_->record(), type{}, path_.list()};
    }
    inner_->null();
    return Record{*parent_, None{}, type{}, path_.list()};
  }
  return Record{*parent_, inner_->record(), seed_, path_.list()};
}

auto EventBuilder::List::list() -> List {
  if (not inner_) {
    return List{*parent_, None{}, type{}, path_.list()};
  }
  if (seed_ and not is<list_type>(seed_)) {
    emit_mismatch(*parent_->dh_, path_.list(), type_kind::of<list_type>, seed_);
    if (not parent_->settings_.schema_only) {
      return List{*parent_, inner_->list(), type{}, path_.list()};
    }
    inner_->null();
    return List{*parent_, None{}, type{}, path_.list()};
  }
  auto element = seed_ ? effective(as<list_type>(seed_).value_type(),
                                   parent_->settings_.schema_only)
                           .value_or(type{})
                       : type{};
  return List{*parent_, inner_->list(), std::move(element), path_.list()};
}

template auto EventBuilder::List::data(bool) -> void;
template auto EventBuilder::List::data(int64_t) -> void;
template auto EventBuilder::List::data(uint64_t) -> void;
template auto EventBuilder::List::data(double) -> void;
template auto EventBuilder::List::data<std::string_view>(std::string_view)
  -> void;
template auto EventBuilder::List::data(blob_view) -> void;
template auto EventBuilder::List::data(SecretView) -> void;
template auto EventBuilder::List::data(ip) -> void;
template auto EventBuilder::List::data(subnet) -> void;
template auto EventBuilder::List::data<time>(time) -> void;
template auto EventBuilder::List::data(duration) -> void;

// -- EventBuilder ------------------------------------------------------------

EventBuilder::EventBuilder(Settings settings, diagnostic_handler& dh)
  : settings_{std::move(settings)}, dh_{dh} {
}

EventBuilder::EventBuilder(EventBuilder&&) noexcept = default;
auto EventBuilder::operator=(EventBuilder&&) noexcept
  -> EventBuilder& = default;
EventBuilder::~EventBuilder() = default;

auto EventBuilder::make(Settings settings, diagnostic_handler& dh)
  -> failure_or<EventBuilder> {
  auto result = EventBuilder{std::move(settings), dh};
  auto const& s = result.settings_;
  result.raw_ = s.raw;
  result.name_ = s.default_schema_name;
  if (auto const* policy = try_as<SchemaPolicy>(s.policy)) {
    result.name_ = policy->name;
    auto schema = modules::get_schema(policy->name);
    if (not schema) {
      if (s.schema_only) {
        diagnostic::error("schema `{}` does not exist, but `schema_only` was "
                          "specified",
                          policy->name)
          .emit(dh);
        return failure::promise();
      }
      diagnostic::warning("schema `{}` does not exist", policy->name)
        .hint("if you know the input's shape, define the schema")
        .emit(dh);
    } else if (not is<record_type>(*schema)) {
      diagnostic::error("schema `{}` is not a record", policy->name).emit(dh);
      return failure::promise();
    } else {
      result.seed_ = std::move(*schema);
    }
  } else if (is<SelectorPolicy>(s.policy)) {
    // Strings stay unparsed until the selected schema is known.
    result.raw_ = true;
  }
  return result;
}

auto event_builder_settings(multi_series_builder::options const& options)
  -> EventBuilder::Settings {
  auto policy = match(
    options.policy,
    [](multi_series_builder::policy_default const&) -> EventBuilder::Policy {
      return EventBuilder::NoPolicy{};
    },
    [](multi_series_builder::policy_schema const& policy)
      -> EventBuilder::Policy {
      return EventBuilder::SchemaPolicy{policy.seed_schema};
    },
    [](multi_series_builder::policy_selector const& policy)
      -> EventBuilder::Policy {
      return EventBuilder::SelectorPolicy{policy.field_name,
                                          policy.naming_prefix};
    });
  return {
    .policy = std::move(policy),
    .schema_only = options.settings.schema_only,
    .raw = options.settings.raw,
    .unflatten_separator = options.settings.unnest_separator,
    .default_schema_name = options.settings.default_schema_name,
  };
}

auto EventBuilder::event() -> Record {
  repeated_.clear();
  return Record{*this, builder_.record(), seed_, value_path{}};
}

auto EventBuilder::length() const -> storage::Index {
  return builder_.length();
}

auto EventBuilder::finish() -> Events {
  auto array = std::exchange(builder_, ArrayBuilder<nova::Record>{}).finish();
  if (is<SelectorPolicy>(settings_.policy)) {
    return finish_selected(std::move(array));
  }
  auto const length = array.length();
  auto rows = storage::BitMap{length, true};
  return Events{std::move(array), std::move(rows),
                Events::Meta::make_empty(length, name_)};
}

auto EventBuilder::schema(std::string const& name) -> Option<type> const& {
  auto it = schemas_.find(name);
  if (it == schemas_.end()) {
    auto schema = modules::get_schema(name);
    if (schema and not is<record_type>(*schema)) {
      schema = None{};
    }
    it = schemas_.emplace(name, std::move(schema)).first;
  }
  return it->second;
}

auto EventBuilder::take(ArrayBuilder<nova::Record>::RecordBuilder& record,
                        std::string_view name, Option<Data>& previous,
                        bool& extend) -> FieldBuilder {
  auto slot = record.take_field(name, previous);
  if (not is<NoPolicy>(settings_.policy)) {
    // The input follows a schema, which does not expect repeated keys.
    previous = None{};
    return slot;
  }
  if (previous) {
    auto key = RepeatedKey{record.parent_, record.parent_->length() - 1,
                           std::string{name}};
    extend = std::ranges::find(repeated_, key) != repeated_.end();
    if (not extend) {
      repeated_.push_back(std::move(key));
    }
  }
  return slot;
}

auto EventBuilder::open_record(
  ArrayBuilder<nova::Record>::RecordBuilder& record, std::string_view name)
  -> Option<ArrayBuilder<nova::Record>::RecordBuilder> {
  return record.open_record_field(name);
}

auto EventBuilder::finish_selected(Array<nova::Record> array) -> Events {
  auto const& selector = as<SelectorPolicy>(settings_.policy);
  auto const length = array.length();
  // Look up the selector field for all events at once.
  auto column = MaskedArray<Array<Data>>{Array<Data>{array},
                                         storage::BitMap{length, true}};
  auto rest = std::string_view{selector.field};
  while (true) {
    auto const dot = rest.find('.');
    auto const name = rest.substr(0, dot);
    auto records = column.data.get_alternative<nova::Record>();
    auto field = Option<MaskedArray<Array<Data>>>{};
    if (records) {
      field = records->data.field(name);
    }
    if (not field) {
      column.present = storage::BitMap{length, false};
      break;
    }
    column = MaskedArray<Array<Data>>{
      field->data, column.present & records->present & field->present};
    if (dot == rest.npos) {
      break;
    }
    rest = rest.substr(dot + 1);
  }
  // Name the events and group them by name.
  struct Group {
    std::string name;
    storage::BitMap::Mutable rows;
    bool from_string = false;
    Option<storage::BitMap> finished_rows;
  };
  // Groups are kept in order of appearance, so that diagnostics are too.
  auto groups = std::vector<Group>{};
  auto group_indices = std::unordered_map<std::string, std::size_t>{};
  auto names = ArrayBuilder<String>{};
  auto missing = false;
  auto invalid = Option<std::string_view>{};
  for (auto row = storage::Index{0}; row < length; ++row) {
    auto name = std::string{};
    auto from_string = false;
    auto prefixed = [&](auto const& value) {
      if (selector.prefix) {
        return fmt::format("{}.{}", *selector.prefix, value);
      }
      return fmt::format("{}", value);
    };
    if (not column.present.get(row)) {
      missing = true;
    } else {
      match(column.data.get(row), [&]<class Tag>(RowView<Tag> view) {
        if constexpr (std::same_as<Tag, Null>) {
          // A null selector falls back to the default name.
        } else if constexpr (std::same_as<Tag, Blob>) {
          invalid = "contains `blob` data";
        } else if constexpr (std::same_as<Tag, Secret>) {
          invalid = "contains `secret`";
        } else if constexpr (std::same_as<Tag, nova::Record>
                             or std::same_as<Tag, nova::List>) {
          invalid = "contains structural type";
        } else {
          from_string = std::same_as<Tag, String>;
          name = prefixed(*view);
        }
      });
    }
    if (name.empty()) {
      names.data(std::string_view{settings_.default_schema_name});
    } else {
      names.data(std::string_view{name});
    }
    auto [it, inserted] = group_indices.try_emplace(name, groups.size());
    if (inserted) {
      groups.push_back(Group{.name = std::move(name),
                             .rows = storage::BitMap::Mutable{length},
                             .finished_rows = None{}});
    }
    auto& group = groups[it->second];
    group.rows.set(row, true);
    group.from_string |= from_string;
  }
  if (missing) {
    diagnostic::warning("event did not contain selector field")
      .note("selector field `{}` was not found", selector.field)
      .emit(*dh_);
  }
  if (invalid) {
    diagnostic::warning("selector field {}, which cannot be used as a "
                        "selector",
                        *invalid)
      .emit(*dh_);
  }
  auto common_fields = std::vector<record_type::field_view>{};
  auto common_initialized = false;
  auto selected_rows = storage::BitMap::Mutable{length};
  for (auto& group : groups) {
    group.finished_rows = std::move(group.rows).finish();
    auto selected = group.name.empty() ? Option<type>{} : schema(group.name);
    if (not selected) {
      continue;
    }
    selected_rows |= *group.finished_rows;
    auto const& record = as<record_type>(*selected);
    if (not common_initialized) {
      for (auto field : record.fields()) {
        common_fields.push_back(field);
      }
      common_initialized = true;
      continue;
    }
    std::erase_if(common_fields, [&](auto const& field) {
      auto other = record.resolve_field(field.name);
      // Only identical types convert alike, e.g., with the same `unit`.
      return not other or field.type != record.field(*other).type;
    });
  }
  if (common_initialized and not common_fields.empty()) {
    auto common = type{record_type{std::move(common_fields)}};
    array
      = Retyper{false, false, *dh_, Retyper::Mode::common_fields_only}.record(
        std::move(array), std::move(selected_rows).finish(), common,
        value_path{});
  }
  // Convert every group to its schema.
  auto const infer = not settings_.raw;
  for (auto& group : groups) {
    auto const& name = group.name;
    auto const rows = std::move(*group.finished_rows);
    auto selected = Option<type>{};
    if (not name.empty()) {
      selected = schema(name);
    }

    if (not selected) {
      if (not name.empty() and group.from_string) {
        diagnostic::warning("selected schema not found")
          .note("`{}` does not refer to a known schema", name)
          .emit(*dh_);
      }
      if (infer) {
        array = Retyper{false, true, *dh_}.record(std::move(array), rows,
                                                  type{}, value_path{});
      }
      continue;
    }
    array = Retyper{settings_.schema_only, infer, *dh_}.record(
      std::move(array), rows, *selected, value_path{});
  }
  auto meta = Events::Meta::make_empty(length);
  meta.name = names.finish();
  return Events{std::move(array), storage::BitMap{length, true},
                std::move(meta)};
}
} // namespace tenzir::nova
