//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ocsf.hpp"

#include "tenzir/collect.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/ocsf_enums.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/value_path.hpp"
#include "tenzir/view3.hpp"

#include <arrow/type_fwd.h>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include <limits>

namespace tenzir::plugins::ocsf {
namespace {

class string_list {
public:
  string_list() = default;

  string_list(const arrow::StringArray* array, int64_t begin, int64_t length)
    : array_{array}, begin_{begin}, length_{length} {
    if (length > 0) {
      TENZIR_ASSERT(array_);
    }
  }

  auto operator==(const string_list& other) const -> bool {
    if (length_ != other.length_) {
      return false;
    }
    for (auto i = int64_t{0}; i < length_; ++i) {
      if (view_at(*array_, begin_ + i)
          != view_at(*other.array_, other.begin_ + i)) {
        return false;
      }
    }
    return true;
  }

  auto contains(std::string_view name) const -> bool {
    for (auto i = int64_t{0}; i < length_; ++i) {
      if (view_at(*array_, begin_ + i) == name) {
        return true;
      }
    }
    return false;
  }

private:
  const arrow::StringArray* array_{nullptr};
  int64_t begin_{0};
  int64_t length_{0};
};

/// Returns a callable `int64_t -> string_list` for the given array.
auto make_string_list_function(std::shared_ptr<arrow::ListArray> list) -> auto {
  if (list) {
    TENZIR_ASSERT(is<arrow::StringArray>(*list->values()));
  }
  return [list = std::move(list)](int64_t i) {
    if (not list or list->IsNull(i)) {
      return string_list{};
    }
    auto offset = list->value_offset(i);
    auto length = list->value_length(i);
    return string_list{
      static_cast<arrow::StringArray*>(&*list->values()),
      offset,
      length,
    };
  };
}

class caster {
public:
  caster(location self, diagnostic_handler& dh, string_list profiles,
         string_list extensions, bool preserve_variants, bool null_fill,
         bool timestamp_to_ms)
    : self_{self},
      dh_{dh},
      profiles_{profiles},
      extensions_{extensions},
      preserve_variants_{preserve_variants},
      null_fill_{null_fill},
      timestamp_to_ms_{timestamp_to_ms} {
  }

  auto cast(const table_slice& slice, const type& ty, std::string_view name)
    -> table_slice {
    auto array = check(to_record_batch(slice)->ToStructArray());
    TENZIR_ASSERT(array);
    auto result
      = cast(series{slice.schema(),
                    std::static_pointer_cast<arrow::Array>(std::move(array))},
             ty, value_path{});
    auto schema = type{name, result.type};
    auto arrow_schema = schema.to_arrow_schema();
    return table_slice{
      arrow::RecordBatch::Make(std::move(arrow_schema), result.length(),
                               as<arrow::StructArray>(*result.array).fields()),
      std::move(schema),
    };
  }

private:
  template <basic_type Type>
  auto cast_type(const Type& ty) -> Type {
    return ty;
  }

  auto cast_type(const record_type& ty) -> record_type {
    auto fields = std::vector<record_type::field_view>{};
    for (const auto& [field_name, field_ty] : ty.fields()) {
      if (is_enabled(field_ty)) {
        fields.emplace_back(field_name, cast_type(field_ty));
      }
    }
    return record_type{fields};
  }

  auto cast_type(const list_type& ty) -> list_type {
    return list_type{cast_type(ty.value_type())};
  }

  auto cast_type(const enumeration_type&) -> enumeration_type {
    TENZIR_UNREACHABLE();
  }

  auto cast_type(const map_type&) -> map_type {
    TENZIR_UNREACHABLE();
  }

  auto cast_type(const type& ty) -> type {
    if (ty.attribute("variant")) {
      if (not preserve_variants_) {
        return type{string_type{}};
      }
      // We don't know the actual type, so we just use `null`.
      return type{null_type{}};
    }
    if (timestamp_to_ms_ and ty.attribute("epochtime")) {
      TENZIR_ASSERT(ty.kind().is<time_type>());
      return type{int64_type{}};
    }
    return match(ty, [&](const auto& ty) {
      return type{cast_type(ty)};
    });
  }

  template <basic_type Type>
  auto cast(basic_series<Type> input, const Type&, value_path path)
    -> basic_series<Type> {
    TENZIR_UNUSED(path);
    return input;
  }

  auto cast(series input, const type& ty, value_path path) -> series {
    auto nullify_empty_records
      = ty.attribute("nullify_empty_records").has_value();
    if (ty.attribute("variant")) {
      TENZIR_ASSERT(is<null_type>(ty));
      if (ty.attribute("must_be_record")
          and not input.type.kind().is_any<null_type, record_type>()
          // Strings are also allowed so that `ocsf::apply` is idempotent.
          and (preserve_variants_ or not input.type.kind().is<string_type>())) {
        diagnostic::warning("expected type `record` for `{}`, but got `{}`",
                            path, input.type.kind())
          .primary(self_)
          .emit(dh_);
        auto result_ty
          = preserve_variants_ ? type{null_type{}} : type{string_type{}};
        return series{result_ty, check(arrow::MakeArrayOfNull(
                                   result_ty.to_arrow_type(), input.length()))};
      }
      if (not preserve_variants_) {
        return print_json(input, nullify_empty_records);
      }
      if (nullify_empty_records) {
        if (auto* record_ty = try_as<record_type>(input.type)) {
          if (record_ty->num_fields() == 0) {
            return series::null(record_type{}, input.length());
          }
        }
      }
      return input;
    }
    if (ty.attribute("epochtime")) {
      TENZIR_ASSERT(is<time_type>(ty));
      if (timestamp_to_ms_) {
        const auto& array = as<arrow::TimestampArray>(*input.array);
        auto b = arrow::Int64Builder{};
        check(b.Reserve(array.length()));
        for (auto val : values(time_type{}, array)) {
          b.UnsafeAppendOrNull(val.transform([](time x) {
            return time_point_cast<std::chrono::milliseconds>(x)
              .time_since_epoch()
              .count();
          }));
        }
        return series{int64_type{}, finish(b)};
      }
    }
    auto result = match(
      std::tie(*input.array, ty),
      [&]<class Type>(const type_to_arrow_array_t<Type>&,
                      const Type& ty) -> series {
        return cast(
          basic_series<Type>{
            as<Type>(input.type),
            std::static_pointer_cast<type_to_arrow_array_t<Type>>(input.array),
          },
          ty, path);
      },
      [&](const arrow::UInt64Array& array, const int64_type&) -> series {
        auto int_builder = arrow::Int64Builder{};
        check(int_builder.Reserve(array.length()));
        auto warned = false;
        for (auto i = int64_t{0}; i < array.length(); ++i) {
          if (array.IsNull(i)) {
            check(int_builder.AppendNull());
          } else {
            auto value = array.Value(i);
            if (not std::in_range<int64_t>(value)) {
              if (not warned) {
                diagnostic::warning("integer in `{}` exceeds maximum", path)
                  .note("found {}", value)
                  .primary(self_)
                  .emit(dh_);
                warned = true;
              }
              check(int_builder.AppendNull());
            } else {
              check(int_builder.Append(static_cast<int64_t>(value)));
            }
          }
        }
        return series{int64_type{}, finish(int_builder)};
      },
      [&]<class Array, class Type>(const Array& array, const Type& ty) -> series
        requires(not std::same_as<Array, type_to_arrow_array_t<Type>>)
      {
        if constexpr (not std::same_as<Array, arrow::NullArray>) {
          diagnostic::warning("expected type `{}` for `{}`, but got `{}`",
                              type_kind::of<Type>, path,
                              type_kind::of<type_from_arrow_t<Array>>)
            .primary(self_)
            .emit(dh_);
        }
        auto cast_ty = cast_type(ty);
        return series{cast_ty, check(arrow::MakeArrayOfNull(
                                 cast_ty.to_arrow_type(), array.length()))};
      });
    return result;
  }

  auto cast(basic_series<enumeration_type>, const enumeration_type&, value_path)
    -> basic_series<enumeration_type> {
    TENZIR_UNREACHABLE();
  }

  auto cast(basic_series<map_type>, const map_type&, value_path)
    -> basic_series<map_type> {
    TENZIR_UNREACHABLE();
  }

  auto cast(basic_series<list_type> input, const list_type& ty, value_path path)
    -> basic_series<list_type> {
    auto values = cast(series{input.type.value_type(), input.array->values()},
                       ty.value_type(), path.list());
    return make_list_series(values, *input.array);
  }

  auto is_profile_enabled(const type& ty) -> bool {
    auto profile = ty.attribute("profile");
    return not profile or profiles_.contains(*profile);
  }

  auto is_extension_enabled(const type& ty) -> bool {
    auto extension = ty.attribute("extension");
    return not extension or extensions_.contains(*extension);
  }

  auto is_enabled(const type& ty) -> bool {
    return is_profile_enabled(ty) and is_extension_enabled(ty);
  }

  auto cast(basic_series<record_type> input, const record_type& ty,
            value_path path) -> basic_series<record_type> {
    auto fields = std::vector<record_type::field_view>{};
    auto field_arrays = arrow::ArrayVector{};
    for (auto&& field : ty.fields()) {
      if (not is_enabled(field.type)) {
        continue;
      }
      auto field_series = input.field(field.name);
      if (field_series) {
        auto casted
          = cast(std::move(*field_series), field.type, path.field(field.name));
        field_arrays.push_back(std::move(casted.array));
        fields.emplace_back(field.name, std::move(casted.type));
        continue;
      }
      if (null_fill_) {
        // No warning if the a target field does not exist.
        auto cast_ty = cast_type(field.type);
        fields.emplace_back(field.name, cast_ty);
        field_arrays.push_back(check(arrow::MakeArrayOfNull(
          cast_ty.to_arrow_type(), input.array->length())));
      }
    }
    for (const auto& field : input.array->struct_type()->fields()) {
      // Warn for fields that do not exist in the target type.
      auto field_path = path.field(field->name());
      auto field_index = ty.resolve_field(field->name());
      if (field_index) {
        auto field_type = ty.field(*field_index).type;
        auto profile = field_type.attribute("profile");
        if (profile and not profiles_.contains(*profile)) {
          diagnostic::warning("dropping `{}` because profile `{}` is not "
                              "enabled",
                              field_path, *profile)
            .primary(self_)
            .emit(dh_);
        }
        auto extension = field_type.attribute("extension");
        if (extension and not extensions_.contains(*extension)) {
          diagnostic::warning("dropping `{}` because extension `{}` is not "
                              "enabled",
                              field_path, *extension)
            .primary(self_)
            .emit(dh_);
        }
      } else {
        // We only include the field path in the note here so that we do not
        // get flooded with diagnostics in case there are many invalid fields.
        diagnostic::warning("dropping field which does not exist in schema",
                            field_path)
          .note("found `{}`", field_path)
          .primary(self_)
          .emit(dh_);
      }
    }
    auto arrow_fields = arrow::FieldVector{};
    arrow_fields.reserve(fields.size());
    for (auto& field : fields) {
      arrow_fields.push_back(field.type.to_arrow_field(field.name));
    }
    return {
      record_type{fields},
      make_struct_array(input.length(), input.array->null_bitmap(),
                        arrow_fields, field_arrays),
    };
  }

  auto print_json(series input, bool nullify_empty_records)
    -> basic_series<string_type> {
    if (auto strings = input.as<string_type>()) {
      // Keep strings as they are (assuming they are already JSON).
      return std::move(*strings);
    }
    auto builder = arrow::StringBuilder{};
    if (nullify_empty_records) {
      if (auto record_ty = try_as<record_type>(input.type)) {
        if (record_ty->num_fields() == 0) {
          check(builder.AppendNulls(input.length()));
          return {string_type{}, finish(builder)};
        }
      }
    }
    input = resolve_enumerations(std::move(input));
    auto printer = json_printer{{.style = no_style(), .oneline = true}};
    auto buffer = std::string{};
    match(*input.array, [&](const auto& array) {
      for (auto value : values3(array)) {
        if (not value) {
          // Preserve nulls instead of rendering them as a string.
          check(builder.AppendNull());
          continue;
        }
        auto it = std::back_inserter(buffer);
        auto success = printer.print(it, *value);
        TENZIR_ASSERT(success);
        check(builder.Append(buffer));
        buffer.clear();
      }
    });
    return {string_type{}, finish(builder)};
  }

  location self_;
  diagnostic_handler& dh_;
  string_list profiles_;
  string_list extensions_;
  bool preserve_variants_;
  bool null_fill_;
  bool timestamp_to_ms_;
};

struct metadata {
  std::shared_ptr<arrow::StringArray> version_array;
  std::shared_ptr<arrow::Int64Array> class_array;
  std::shared_ptr<arrow::StructArray> metadata_array;
};

auto extract_metadata(const table_slice& slice, location self,
                      diagnostic_handler& dh) -> std::optional<metadata> {
  auto ty = as<record_type>(slice.schema());
  auto metadata_index = ty.resolve_field("metadata");
  if (not metadata_index) {
    diagnostic::warning("dropping events where `metadata` does not exist")
      .primary(self)
      .emit(dh);
    return {};
  }
  auto metadata_array = std::dynamic_pointer_cast<arrow::StructArray>(
    to_record_batch(slice)->column(detail::narrow<int>(*metadata_index)));
  if (not metadata_array) {
    diagnostic::warning("dropping events where `metadata` is not a record")
      .primary(self)
      .emit(dh);
    return {};
  }
  auto version_index = metadata_array->struct_type()->GetFieldIndex("version");
  if (version_index == -1) {
    diagnostic::warning(
      "dropping events where `metadata.version` does not exist")
      .primary(self)
      .emit(dh);
    return {};
  }
  auto version_array = std::dynamic_pointer_cast<arrow::StringArray>(
    check(metadata_array->GetFlattenedField(version_index)));
  if (not version_array) {
    diagnostic::warning(
      "dropping events where `metadata.version` is not a string")
      .primary(self)
      .emit(dh);
    return {};
  }
  auto class_index = ty.resolve_field("class_uid");
  if (not class_index) {
    diagnostic::warning("dropping events where `class_uid` does not exist")
      .primary(self)
      .emit(dh);
    return {};
  }
  auto class_array = std::dynamic_pointer_cast<arrow::Int64Array>(
    to_record_batch(slice)->column(detail::narrow<int>(*class_index)));
  if (not class_array) {
    diagnostic::warning("dropping events where `class_uid` is not an integer")
      .primary(self)
      .emit(dh);
    return {};
  }
  return metadata{
    .version_array = std::move(version_array),
    .class_array = std::move(class_array),
    .metadata_array = std::move(metadata_array),
  };
}

auto mangle_version(std::string_view version) -> std::string {
  auto result = std::string{};
  result.reserve(1 + version.size());
  result += 'v';
  for (auto c : version) {
    if (('0' <= c and c <= '9') or ('a' <= c and c <= 'z')
        or ('A' <= c and c <= 'Z') or c == '_') {
      result += c;
    } else if (c == '.' or c == '-') {
      result += '_';
    } else {
      // ignore
    }
  }
  return result;
}

auto mangle_class_name(std::string_view class_name) -> std::string {
  auto result = std::string{};
  for (auto c : class_name) {
    if (c == ' ') {
      result += '_';
    } else {
      result += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
  }
  return result;
}

class trimmer {
public:
  trimmer(bool drop_optional, bool drop_recommended)
    : drop_optional_{drop_optional}, drop_recommended_{drop_recommended} {
  }

  auto trim(const table_slice& slice, const type& ty) -> table_slice {
    auto result = trim(series{slice}, ty);
    auto arrow_schema = result.type.to_arrow_schema();
    return table_slice{
      arrow::RecordBatch::Make(arrow_schema, result.length(),
                               as<arrow::StructArray>(*result.array).fields()),
      std::move(result.type),
    };
  }

private:
  template <class Type>
    requires(basic_type<Type> or std::same_as<Type, enumeration_type>)
  auto trim(basic_series<Type> input, const Type& ty) -> basic_series<Type> {
    TENZIR_UNUSED(ty);
    return input;
  }

  auto trim(basic_series<map_type> input, const map_type& ty)
    -> basic_series<map_type> {
    TENZIR_UNUSED(input, ty);
    TENZIR_UNREACHABLE();
  }

  auto trim(basic_series<list_type> input, const list_type& ty)
    -> basic_series<list_type> {
    auto values = trim(series{input.type.value_type(), input.array->values()},
                       ty.value_type());
    return make_list_series(values, *input.array);
  }

  auto trim(series input, const type& ty) -> series {
    if (ty.attribute("variant")) {
      // Do not attempt trimming in variant fields.
      return input;
    }
    auto name = input.type.name();
    auto attributes = collect(input.type.attributes());
    return match(
      std::tie(input, ty),
      [&]<class Type>(basic_series<Type> input, const Type& ty) -> series {
        auto result = trim(std::move(input), ty);
        return series{
          type{name, result.type, std::move(attributes)},
          std::move(result.array),
        };
      },
      [&]<class Actual, class Expected>(basic_series<Actual>,
                                        const Expected&) -> series {
        // TODO: Figure out what to do in this case.
        return input;
      });
  }

  auto trim(basic_series<record_type> input, const record_type& ty)
    -> basic_series<record_type> {
    auto fields = std::vector<series_field>{};
    for (auto&& field : input.fields()) {
      auto field_ty = ty.field(field.name);
      if (not field_ty) {
        // TODO: Field does not exist according to OCSF.
        continue;
      }
      if (should_drop(*field_ty)) {
        continue;
      }
      fields.emplace_back(field.name, trim(std::move(field.data), *field_ty));
    }
    return make_record_series(fields, *input.array);
  }

  auto should_drop(const type& ty) const -> bool {
    if (drop_optional_ and ty.attribute("optional")) {
      return true;
    }
    if (drop_recommended_ and ty.attribute("recommended")) {
      return true;
    }
    return false;
  }

  bool drop_optional_{};
  bool drop_recommended_{};
};

struct ocsf_schema {
  ocsf_schema(class type type, std::string_view class_name,
              std::string mangled_class_name)
    : type{std::move(type)},
      class_name{class_name},
      mangled_class_name{std::move(mangled_class_name)} {
  }

  class type type;
  std::string_view class_name;
  std::string mangled_class_name;
};

auto get_ocsf_schema(std::optional<std::string_view> version,
                     std::optional<int64_t> class_uid, location self,
                     diagnostic_handler& dh) -> std::optional<ocsf_schema> {
  if (not version) {
    diagnostic::warning("dropping events where `metadata.version` is null")
      .primary(self)
      .emit(dh);
    return {};
  }
  auto parsed_version = parse_ocsf_version(*version);
  if (not parsed_version) {
    diagnostic::warning("dropping events with unknown OCSF version", *version)
      .primary(self)
      .note("found {:?}", *version)
      .emit(dh);
    return {};
  }
  if (not class_uid) {
    diagnostic::warning("dropping events where `class_uid` is null")
      .primary(self)
      .emit(dh);
    return {};
  }
  auto class_name = ocsf_class_name(*parsed_version, *class_uid);
  if (not class_name) {
    diagnostic::warning("dropping events where `class_uid` is unknown")
      .primary(self)
      .note("could not find class for value `{}`", *class_uid)
      .emit(dh);
    return {};
  }
  auto mangled_class_name = mangle_class_name(*class_name);
  auto schema
    = fmt::format("_ocsf.{}.{}", mangle_version(*version), mangled_class_name);
  auto ty = modules::get_schema(schema);
  if (not ty) {
    diagnostic::warning("could not find schema for the given event")
      .primary(self)
      .note("tried to find version {:?} for class {:?}", *version, *class_name)
      .emit(dh);
    return {};
  }
  return ocsf_schema{
    std::move(*ty),
    *class_name,
    std::move(mangled_class_name),
  };
}

class trim_operator final : public crtp_operator<trim_operator> {
public:
  trim_operator() = default;

  trim_operator(struct location self, bool drop_optional, bool drop_recommended)
    : self_{self},
      drop_optional_{drop_optional},
      drop_recommended_{drop_recommended} {
  }

  auto name() const -> std::string override {
    return "ocsf::trim";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      // Get the required columns `metadata.version` and `class_uid`.
      auto metadata = extract_metadata(slice, self_, ctrl.diagnostics());
      if (not metadata) {
        co_yield {};
        continue;
      }
      auto& version_array = metadata->version_array;
      auto& class_array = metadata->class_array;
      // Figure out longest slices that share:
      // - metadata.version
      // - class_uid
      // We do not take profiles or extensions into account here because that is
      // not strictly needed for trimming.
      auto begin = int64_t{0};
      auto end = begin;
      auto version = view_at(*version_array, begin);
      auto class_uid = view_at(*class_array, begin);
      auto process = [&]() -> table_slice {
        auto schema
          = get_ocsf_schema(version, class_uid, self_, ctrl.diagnostics());
        if (not schema) {
          return {};
        }
        return trimmer{drop_optional_, drop_recommended_}.trim(slice,
                                                               schema->type);
      };
      for (; end < class_array->length(); ++end) {
        auto next_version = view_at(*version_array, end);
        auto next_class_uid = view_at(*class_array, end);
        if (next_version == version and next_class_uid == class_uid) {
          continue;
        }
        co_yield process();
        begin = end;
        version = next_version;
        class_uid = next_class_uid;
      }
      co_yield process();
    }
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, trim_operator& x) -> bool {
    return f.object(x).fields(f.field("self", x.self_),
                              f.field("drop_optional", x.drop_optional_),
                              f.field("drop_recommended", x.drop_recommended_));
  }

private:
  struct location self_;
  bool drop_optional_{};
  bool drop_recommended_{};
};

class deriver {
public:
  deriver(location self, diagnostic_handler& dh) : self_{self}, dh_{dh} {
  }

  auto derive(const table_slice& slice, const type& ty) -> table_slice {
    auto result = derive(series{slice}, ty, value_path{});
    auto arrow_schema = result.type.to_arrow_schema();
    return table_slice{
      arrow::RecordBatch::Make(arrow_schema, result.length(),
                               as<arrow::StructArray>(*result.array).fields()),
      std::move(result.type),
    };
  }

private:
  template <class Type>
    requires(basic_type<Type> or std::same_as<Type, enumeration_type>)
  auto derive(basic_series<Type> input, const Type& ty, value_path path)
    -> basic_series<Type> {
    TENZIR_UNUSED(ty, path);
    return input;
  }

  auto derive(basic_series<map_type> input, const map_type& ty, value_path path)
    -> basic_series<map_type> {
    TENZIR_UNUSED(input, ty, path);
    TENZIR_UNREACHABLE();
  }

  auto derive(basic_series<list_type> input, const list_type& ty,
              value_path path) -> basic_series<list_type> {
    auto values = derive(series{input.type.value_type(), input.array->values()},
                         ty.value_type(), path.list());
    return make_list_series(values, *input.array);
  }

  auto derive(series input, const type& ty, value_path path) -> series {
    if (ty.attribute("variant")) {
      // Do not attempt derivation in variant fields.
      return input;
    }
    auto name = input.type.name();
    auto attributes = collect(input.type.attributes());
    return match(
      std::tie(input, ty),
      [&]<class Type>(basic_series<Type> input, const Type& ty) -> series {
        auto result = derive(std::move(input), ty, path);
        return series{
          type{name, result.type, std::move(attributes)},
          std::move(result.array),
        };
      },
      [&]<class Actual, class Expected>(basic_series<Actual>,
                                        const Expected&) -> series {
        // TODO: Figure out what to do in this case.
        return input;
      });
  }

  auto derive(basic_series<record_type> input, const record_type& ty,
              value_path path) -> basic_series<record_type> {
    auto fields = std::vector<series_field>{};
    // Collect all input fields for fast lookup.
    auto input_fields = boost::unordered_flat_map<std::string_view, series>{};
    for (auto&& field : input.fields()) {
      input_fields[field.name] = field.data;
    }
    // Fields that are referenced as a sibling will be handled together with the
    // field that references them.
    auto skip = boost::unordered_flat_set<std::string_view>{};
    for (auto&& [_, field_ty] : ty.fields()) {
      if (auto sibling = field_ty.attribute("sibling")) {
        skip.insert(*sibling);
      }
    }
    // Go over all OCSF fields marked with "enum" and "sibling" attributes.
    for (auto&& [field_name, field_ty] : ty.fields()) {
      if (skip.contains(field_name)) {
        continue;
      }
      auto enum_attr = field_ty.attribute("enum");
      if (enum_attr) {
        // This is an enum field with a sibling.
        if (is<list_type>(field_ty)) {
          // Enum lists are not supported yet.
          continue;
        }
        auto int_name = field_name;
        auto int_path = path.field(int_name);
        TENZIR_ASSERT(field_ty.kind().is<int64_type>());
        auto sibling_attr = field_ty.attribute("sibling");
        TENZIR_ASSERT(sibling_attr);
        auto string_name = *sibling_attr;
        auto string_path = path.field(string_name);
        auto string_ty = ty.field(string_name);
        TENZIR_ASSERT(string_ty);
        TENZIR_ASSERT(string_ty->kind().is<string_type>());
        auto int_field = input_fields.find(int_name);
        auto string_field = input_fields.find(string_name);
        if (int_field != input_fields.end()
            and string_field != input_fields.end()) {
          // Both exist - derive bidirectionally.
          auto [derived_enum, derived_sibling]
            = derive_bidirectionally(int_field->second, string_field->second,
                                     *enum_attr, int_path, string_path);
          fields.emplace_back(int_name, std::move(derived_enum));
          fields.emplace_back(string_name, std::move(derived_sibling));
        } else if (int_field != input_fields.end()) {
          // Only enum exists - derive sibling.
          auto derived_sibling
            = string_from_int(int_field->second, *enum_attr, int_path);
          fields.emplace_back(int_name, std::move(int_field->second));
          fields.emplace_back(string_name, std::move(derived_sibling));
        } else if (string_field != input_fields.end()) {
          // Only sibling exists - derive enum.
          auto derived_enum
            = int_from_string(string_field->second, *enum_attr, string_path);
          fields.emplace_back(int_name, std::move(derived_enum));
          fields.emplace_back(string_name, std::move(string_field->second));
        } else {
          // Neither exists. This also happens for fields that are in profiles
          // or extensions that are not used.
        }
        skip.insert(int_name);
        TENZIR_ASSERT(skip.contains(string_name));
      } else {
        // Non-enum field processing.
        auto field_iter = input_fields.find(field_name);
        if (field_iter != input_fields.end()) {
          fields.emplace_back(field_name,
                              derive(std::move(field_iter->second), field_ty,
                                     path.field(field_name)));
        }
        skip.insert(field_name);
      }
    }
    // Make sure the OCSF fields are sorted. The logic above doesn't guarantee
    // that due to the insertion of the siblings.
    std::ranges::sort(fields, {}, &series_field::name);
    // Add any remaining input fields not in the schema.
    for (auto&& [field_name, field_data] : input_fields) {
      if (not skip.contains(field_name)) {
        fields.emplace_back(field_name, field_data);
      }
    }
    return make_record_series(fields, *input.array);
  }

  auto
  derive_bidirectionally(const series& int_field, const series& string_field,
                         std::string_view enum_id, value_path int_path,
                         value_path string_path) -> std::pair<series, series> {
    auto int_array = int_field.as<int64_type>();
    if (not int_array) {
      if (int_field.as<null_type>()) {
        return {
          int_from_string(string_field, enum_id, string_path),
          string_field,
        };
      }
      diagnostic::warning("field `{}` must be `int`, but got `{}`", int_path,
                          int_field.type.kind())
        .primary(self_)
        .emit(dh_);
      return {int_field, string_field};
    }
    auto string_array = string_field.as<string_type>();
    if (not string_array) {
      if (string_field.as<null_type>()) {
        return {
          int_field,
          string_from_int(int_field, enum_id, int_path),
        };
      }
      diagnostic::warning("field `{}` must be `int`, but got `{}`", int_path,
                          int_field.type.kind())
        .primary(self_)
        .emit(dh_);
      return {int_field, string_field};
    }
    return derive_bidirectionally(*int_array, *string_array, enum_id, int_path,
                                  string_path);
  }

  auto derive_bidirectionally(const basic_series<int64_type>& int_field,
                              const basic_series<string_type>& string_field,
                              std::string_view enum_id, value_path int_path,
                              value_path string_path)
    -> std::pair<basic_series<int64_type>, basic_series<string_type>> {
    const auto& enum_lookup = check(get_ocsf_int_to_string(enum_id)).get();
    const auto& reverse_lookup = check(get_ocsf_string_to_int(enum_id)).get();
    auto int_builder = arrow::Int64Builder{};
    auto string_builder = arrow::StringBuilder{};
    check(int_builder.Reserve(int_field.length()));
    check(string_builder.Reserve(string_field.length()));
    for (auto i = int64_t{0}; i < int_field.length(); ++i) {
      auto int_value = int_field.at(i);
      auto string_value = string_field.at(i);
      // Determine final values based on derivation rules
      auto int_result = int_value;
      auto string_result = string_value;
      if (int_value and string_value) {
        // Both present - just validate consistency
        auto expected_string = enum_lookup.find(*int_value);
        if (expected_string == enum_lookup.end()) {
          diagnostic::warning("found invalid value for `{}`", int_path)
            .primary(self_)
            .note("got {}", *int_value)
            .emit(dh_);
        }
        auto expected_int = reverse_lookup.find(*string_value);
        if (expected_int == reverse_lookup.end()) {
          diagnostic::warning("found invalid value for `{}`", string_path)
            .primary(self_)
            .note("got {:?}", *string_value)
            .emit(dh_);
        }
        if (expected_string != enum_lookup.end()
            and expected_int != reverse_lookup.end()) {
          if (*int_value != expected_int->second
              or *string_value != expected_string->second) {
            diagnostic::warning("found inconsistency between `{}` and `{}`",
                                int_path, string_path)
              .primary(self_)
              .note("got {} ({:?}) and {:?} ({})", *int_value,
                    expected_string->second, *string_value,
                    expected_int->second)
              .emit(dh_);
          }
        }
      } else if (int_value and not string_value) {
        // Derive string from int
        auto it = enum_lookup.find(*int_value);
        if (it != enum_lookup.end()) {
          string_result = it->second;
        } else {
          diagnostic::warning("found invalid value for field `{}`", int_path)
            .primary(self_)
            .note("got {}", *int_value)
            .emit(dh_);
        }
      } else if (string_value and not int_value) {
        // Derive int from string
        auto it = reverse_lookup.find(*string_value);
        if (it != reverse_lookup.end()) {
          int_result = it->second;
        } else {
          diagnostic::warning("found invalid value for field `{}`", string_path)
            .primary(self_)
            .note("got {:?}", *string_value)
            .emit(dh_);
        }
      } else {
        // Both are null. Keep them as-is, no warning.
      }
      if (int_result) {
        check(int_builder.Append(*int_result));
      } else {
        check(int_builder.AppendNull());
      }
      if (string_result) {
        check(string_builder.Append(*string_result));
      } else {
        check(string_builder.AppendNull());
      }
    }
    return {finish(int_builder), finish(string_builder)};
  }

  auto string_from_int(const series& int_field, std::string_view enum_id,
                       value_path int_path) -> basic_series<string_type> {
    if (int_field.as<null_type>()) {
      return basic_series<string_type>::null(int_field.length());
    }
    auto enum_int_array = int_field.as<int64_type>();
    if (not enum_int_array) {
      diagnostic::warning("expected field `{}` to be `int`, but got `{}`",
                          int_path, int_field.type.kind())
        .primary(self_)
        .emit(dh_);
      return basic_series<string_type>::null(int_field.length());
    }
    const auto& int_to_string = check(get_ocsf_int_to_string(enum_id)).get();
    auto string_builder = arrow::StringBuilder{};
    check(string_builder.Reserve(int_field.length()));
    for (auto i = int64_t{0}; i < int_field.length(); ++i) {
      if (auto value = enum_int_array->at(i)) {
        auto it = int_to_string.find(*value);
        if (it != int_to_string.end()) {
          check(string_builder.Append(it->second));
        } else {
          diagnostic::warning("found invalid value for `{}`", int_path)
            .primary(self_)
            .note("got {}", *value)
            .emit(dh_);
          check(string_builder.AppendNull());
        }
      } else {
        check(string_builder.AppendNull());
      }
    }
    return finish(string_builder);
  }

  auto int_from_string(const series& string_field, std::string_view enum_id,
                       value_path string_path) -> basic_series<int64_type> {
    if (string_field.as<null_type>()) {
      return basic_series<int64_type>::null(string_field.length());
    }
    auto sibling_string_array = string_field.as<string_type>();
    if (not sibling_string_array) {
      diagnostic::warning("expected field `{}` to be `string`, but got `{}`",
                          string_path, string_field.type.kind())
        .primary(self_)
        .emit(dh_);
      return basic_series<int64_type>::null(string_field.length());
    }
    const auto& string_to_int = check(get_ocsf_string_to_int(enum_id)).get();
    auto int_builder = arrow::Int64Builder{};
    check(int_builder.Reserve(string_field.length()));
    for (auto i = int64_t{0}; i < string_field.length(); ++i) {
      if (auto value = sibling_string_array->at(i)) {
        auto it = string_to_int.find(*value);
        if (it != string_to_int.end()) {
          check(int_builder.Append(it->second));
        } else {
          diagnostic::warning("found invalid value for `{}`", string_path)
            .primary(self_)
            .note("got {:?}", *value)
            .emit(dh_);
          check(int_builder.AppendNull());
        }
      } else {
        check(int_builder.AppendNull());
      }
    }
    return finish(int_builder);
  }

  location self_;
  diagnostic_handler& dh_;
};

class derive_operator final : public crtp_operator<derive_operator> {
public:
  derive_operator() = default;

  derive_operator(struct location self) : self_{self} {
  }

  auto name() const -> std::string override {
    return "ocsf::derive";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      // Get the required columns `metadata.version` and `class_uid`.
      auto metadata = extract_metadata(slice, self_, ctrl.diagnostics());
      if (not metadata) {
        co_yield {};
        continue;
      }
      auto& version_array = metadata->version_array;
      auto& class_array = metadata->class_array;
      // Figure out longest slices that share:
      // - metadata.version
      // - class_uid
      auto begin = int64_t{0};
      auto end = begin;
      auto version = view_at(*version_array, begin);
      auto class_uid = view_at(*class_array, begin);
      auto process = [&]() -> table_slice {
        auto schema
          = get_ocsf_schema(version, class_uid, self_, ctrl.diagnostics());
        if (not schema) {
          return {};
        }
        return deriver{self_, ctrl.diagnostics()}.derive(
          subslice(slice, begin, end), schema->type);
      };
      for (; end < class_array->length(); ++end) {
        auto next_version = view_at(*version_array, end);
        auto next_class_uid = view_at(*class_array, end);
        if (next_version == version and next_class_uid == class_uid) {
          continue;
        }
        co_yield process();
        begin = end;
        version = next_version;
        class_uid = next_class_uid;
      }
      co_yield process();
    }
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, derive_operator& x) -> bool {
    return f.object(x).fields(f.field("self", x.self_));
  }

private:
  struct location self_;
};

class cast_operator final : public crtp_operator<cast_operator> {
public:
  cast_operator() = default;

  cast_operator(struct location self, bool preserve_variants, bool null_fill,
                bool timestamp_to_ms)
    : self_{self},
      preserve_variants_{preserve_variants},
      null_fill_{null_fill},
      timestamp_to_ms_{timestamp_to_ms} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      // Get the required columns `metadata.version` and `class_uid`.
      auto metadata = extract_metadata(slice, self_, ctrl.diagnostics());
      if (not metadata) {
        co_yield {};
        continue;
      }
      auto& version_array = metadata->version_array;
      auto& class_array = metadata->class_array;
      auto& metadata_array = metadata->metadata_array;
      auto profiles_at = std::invoke([&]() {
        auto profiles_index
          = metadata_array->struct_type()->GetFieldIndex("profiles");
        if (profiles_index == -1) {
          return make_string_list_function(nullptr);
        }
        auto profiles_array
          = check(metadata_array->GetFlattenedField(profiles_index));
        if (dynamic_cast<arrow::NullArray*>(&*profiles_array)) {
          return make_string_list_function(nullptr);
        };
        auto profiles_lists = std::dynamic_pointer_cast<arrow::ListArray>(
          std::move(profiles_array));
        if (not profiles_lists) {
          diagnostic::warning("ignoring profiles for events where "
                              "`metadata.profiles` is not a list")
            .primary(self_)
            .emit(ctrl.diagnostics());
          return make_string_list_function(nullptr);
        }
        if (dynamic_cast<arrow::NullArray*>(&*profiles_lists->values())) {
          return make_string_list_function(nullptr);
        }
        if (not dynamic_cast<arrow::StringArray*>(&*profiles_lists->values())) {
          diagnostic::warning("ignoring profiles for events where "
                              "`metadata.profiles` is not a list of strings")
            .primary(self_)
            .emit(ctrl.diagnostics());
          return make_string_list_function(nullptr);
        }
        // Optimize the case where we know that all lists are trivially empty.
        if (profiles_lists->value_offset(0)
            == profiles_lists->value_offset(profiles_lists->length())) {
          return make_string_list_function(nullptr);
        }
        return make_string_list_function(profiles_lists);
      });
      auto extensions_at = std::invoke([&] {
        auto extensions_index
          = metadata_array->struct_type()->GetFieldIndex("extensions");
        if (extensions_index == -1) {
          return make_string_list_function(nullptr);
        }
        auto extensions_array
          = check(metadata_array->GetFlattenedField(extensions_index));
        if (dynamic_cast<arrow::NullArray*>(&*extensions_array)) {
          return make_string_list_function(nullptr);
        };
        auto extensions_lists = std::dynamic_pointer_cast<arrow::ListArray>(
          std::move(extensions_array));
        if (not extensions_lists) {
          diagnostic::warning("ignoring extensions for events where "
                              "`metadata.extensions` is not a list")
            .primary(self_)
            .emit(ctrl.diagnostics());
          return make_string_list_function(nullptr);
        }
        if (dynamic_cast<arrow::NullArray*>(&*extensions_lists->values())) {
          return make_string_list_function(nullptr);
        }
        auto* extensions_structs
          = dynamic_cast<arrow::StructArray*>(&*extensions_lists->values());
        if (not extensions_structs) {
          diagnostic::warning("ignoring extensions for events where "
                              "`metadata.extensions` is not a list of records")
            .primary(self_)
            .emit(ctrl.diagnostics());
          return make_string_list_function(nullptr);
        }
        auto name_index
          = extensions_structs->struct_type()->GetFieldIndex("name");
        if (name_index == -1) {
          diagnostic::warning("ignoring extensions for events where "
                              "`metadata.extensions[].name` does not exist")
            .primary(self_)
            .emit(ctrl.diagnostics());
          return make_string_list_function(nullptr);
        }
        auto name_array
          = check(extensions_structs->GetFlattenedField(name_index));
        if (not dynamic_cast<arrow::StringArray*>(&*name_array)) {
          diagnostic::warning("ignoring extensions for events where "
                              "`metadata.extensions[].name` is not a string")
            .primary(self_)
            .emit(ctrl.diagnostics());
          return make_string_list_function(nullptr);
        }
        auto name_lists = make_list_series(series{string_type{}, name_array},
                                           *extensions_lists);
        return make_string_list_function(std::move(name_lists.array));
      });
      // Figure out longest slices that share:
      // - metadata.version
      // - metadata.profiles
      // - class_uid
      // - metadata.extensions[].name
      // Since we only support extensions that are served by the OCSF server
      // for the corresponding version, we know that they have a
      // non-conflicting name and there is no need to take their version into
      // account (although we could check for consistency with the event).
      auto begin = int64_t{0};
      auto end = begin;
      auto version = view_at(*version_array, begin);
      auto class_uid = view_at(*class_array, begin);
      auto profiles = profiles_at(begin);
      auto extensions = extensions_at(begin);
      auto process = [&]() -> table_slice {
        auto schema
          = get_ocsf_schema(version, class_uid, self_, ctrl.diagnostics());
        if (not schema) {
          return {};
        }
        auto extension = schema->type.attribute("extension");
        if (extension and not extensions.contains(*extension)) {
          diagnostic::warning("dropping event for class {:?} because "
                              "extension "
                              "{:?} is not enabled",
                              schema->class_name, *extension)
            .primary(self_)
            .emit(ctrl.diagnostics());
          return {};
        }
        auto type_name = "ocsf." + schema->mangled_class_name;
        return caster{self_,           ctrl.diagnostics(), profiles,
                      extensions,      preserve_variants_, null_fill_,
                      timestamp_to_ms_}
          .cast(subslice(slice, begin, end), schema->type, type_name);
      };
      for (; end < class_array->length(); ++end) {
        auto next_version = view_at(*version_array, end);
        auto next_class_uid = view_at(*class_array, end);
        auto next_profiles = profiles_at(end);
        auto next_extensions = extensions_at(end);
        if (next_version == version and next_class_uid == class_uid
            and next_profiles == profiles and extensions == next_extensions) {
          continue;
        }
        co_yield process();
        begin = end;
        version = next_version;
        class_uid = next_class_uid;
        profiles = next_profiles;
        extensions = next_extensions;
      }
      co_yield process();
    }
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "ocsf::cast";
  }

  friend auto inspect(auto& f, cast_operator& x) -> bool {
    return f.object(x).fields(f.field("self_", x.self_),
                              f.field("preserve_variants_",
                                      x.preserve_variants_),
                              f.field("null_fill_", x.null_fill_),
                              f.field("timestamp_to_ms_", x.timestamp_to_ms_));
  }

private:
  struct location self_;
  bool preserve_variants_{};
  bool null_fill_{};
  bool timestamp_to_ms_{};
};

class apply_plugin final : public operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "ocsf::apply";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto preserve_variants = false;
    argument_parser2::operator_(name())
      .named("preserve_variants", preserve_variants)
      .parse(inv, ctx)
      .ignore();
    return std::make_unique<cast_operator>(inv.self.get_location(),
                                           preserve_variants, true, false);
  }
};

class cast_plugin final : public operator_plugin2<cast_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto encode_variants = false;
    auto timestamp_to_ms = false;
    auto null_fill = false;
    TRY(argument_parser2::operator_(name())
          .named("encode_variants", encode_variants)
          .named("null_fill", null_fill)
          .named("timestamp_to_ms", timestamp_to_ms)
          .parse(inv, ctx));
    return std::make_unique<cast_operator>(
      inv.self.get_location(), not encode_variants, null_fill, timestamp_to_ms);
  }
};

class trim_plugin final : public operator_plugin2<trim_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    // TODO: Consider using a more intelligent default that is not simply
    // based on attributes being optional.
    auto drop_optional = true;
    auto drop_recommended = false;
    argument_parser2::operator_(name())
      .named("drop_optional", drop_optional)
      .named("drop_recommended", drop_recommended)
      .parse(inv, ctx)
      .ignore();
    return std::make_unique<trim_operator>(inv.self.get_location(),
                                           drop_optional, drop_recommended);
  }
};

class derive_plugin final : public operator_plugin2<derive_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_(name()).parse(inv, ctx).ignore();
    return std::make_unique<derive_operator>(inv.self.get_location());
  }
};

} // namespace
} // namespace tenzir::plugins::ocsf

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::apply_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::cast_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::trim_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::derive_plugin)
