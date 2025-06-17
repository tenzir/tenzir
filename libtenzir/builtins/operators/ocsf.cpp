//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ocsf.hpp"

#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/view3.hpp"

namespace tenzir::plugins::ocsf {
namespace {

class profile_list {
public:
  profile_list() = default;

  profile_list(const arrow::StringArray* array, int64_t begin, int64_t length)
    : array_{array}, begin_{begin}, length_{length} {
    if (length > 0) {
      TENZIR_ASSERT(array_);
    }
  }

  auto operator==(const profile_list& other) const -> bool {
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

class caster {
public:
  explicit caster(location self, diagnostic_handler& dh, profile_list profiles)
    : self_{self}, dh_{dh}, profiles_{profiles} {
  }

  auto cast(const table_slice& slice, const type& ty, std::string_view name)
    -> table_slice {
    auto array = check(to_record_batch(slice)->ToStructArray());
    TENZIR_ASSERT(array);
    auto result = cast(*array, as<record_type>(ty), "");
    auto schema = type{name, result.type};
    auto arrow_schema = schema.to_arrow_schema();
    return table_slice{
      arrow::RecordBatch::Make(std::move(arrow_schema), result.length(),
                               result.array->fields()),
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
    for (auto [field_name, field_ty] : ty.fields()) {
      auto profile = field_ty.attribute("profile");
      if (profile and not profiles_.contains(*profile)) {
        continue;
      }
      fields.emplace_back(field_name, cast_type(field_ty));
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
    return match(ty, [&](const auto& ty) {
      return type{cast_type(ty)};
    });
  }

  template <basic_type Type>
  auto cast(const type_to_arrow_array_t<Type>& array, const Type&,
            std::string_view path) -> basic_series<Type> {
    (void)path;
    return {Type{},
            std::make_shared<type_to_arrow_array_t<Type>>(array.data())};
  }

  auto cast(const arrow::Array& array, const type& ty, std::string_view path)
    -> series {
    if (is<string_type>(ty) and ty.attribute("print_json")) {
      return series{
        string_type{},
        print_json(array, ty.attribute("nullify_empty_records").has_value())};
    }
    auto result = match(
      std::tie(array, ty),
      [&]<class Type>(const type_to_arrow_array_t<Type>& array,
                      const Type& ty) -> series {
        return cast(array, ty, path);
      },
      [&]<class Array, class Type>(const Array& array, const Type& ty) -> series
        requires(not std::same_as<Array, type_to_arrow_array_t<Type>>)
      {
        // TODO: Might want to try some conversions instead.
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

  auto cast(const enumeration_type::array_type&, const enumeration_type&,
            std::string_view) -> basic_series<enumeration_type> {
    TENZIR_UNREACHABLE();
  }

  auto cast(const arrow::MapArray&, const map_type&, std::string_view)
    -> basic_series<map_type> {
    TENZIR_UNREACHABLE();
  }

  auto cast(const arrow::ListArray& array, const list_type& ty,
            std::string_view path) -> basic_series<list_type> {
    auto values
      = cast(*array.values(), ty.value_type(), std::string{path} + "[]");
    return {list_type{values.type},
            check(arrow::ListArray::FromArrays(
              *array.offsets(), *values.array, arrow::default_memory_pool(),
              array.null_bitmap(), array.data()->null_count))};
  }

  auto cast(const arrow::StructArray& array, const record_type& ty,
            std::string_view path) -> basic_series<record_type> {
    auto fields = std::vector<record_type::field_view>{};
    auto field_arrays = arrow::ArrayVector{};
    for (auto&& field : ty.fields()) {
      auto profile = field.type.attribute("profile");
      if (profile and not profiles_.contains(*profile)) {
        continue;
      }
      auto field_array = array.GetFieldByName(std::string{field.name});
      if (not field_array) {
        // No warning if the a target field does not exist.
        auto cast_ty = cast_type(field.type);
        fields.emplace_back(field.name, cast_ty);
        field_arrays.push_back(check(
          arrow::MakeArrayOfNull(cast_ty.to_arrow_type(), array.length())));
        continue;
      }
      auto field_path = std::string{path};
      if (not field_path.empty()) {
        field_path += '.';
      }
      field_path += field.name;
      auto casted = cast(*field_array, field.type, field_path);
      field_arrays.push_back(std::move(casted.array));
      fields.emplace_back(field.name, std::move(casted.type));
    }
    for (auto& field : array.struct_type()->fields()) {
      // Warn for fields that do not exist in the target type.
      auto field_path = std::string{path};
      if (not field_path.empty()) {
        field_path += '.';
      }
      field_path += field->name();
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
    return {record_type{fields},
            make_struct_array(array.length(), array.null_bitmap(), arrow_fields,
                              field_arrays)};
  }

  auto print_json(const arrow::Array& array, bool nullify_empty_records)
    -> std::shared_ptr<arrow::StringArray> {
    if (is<arrow::StringArray>(array)) {
      // Keep strings as they are (assuming they are already JSON).
      return std::make_shared<arrow::StringArray>(array.data());
    }
    auto builder = arrow::StringBuilder{};
    if (nullify_empty_records) {
      if (auto struct_array = dynamic_cast<const arrow::StructArray*>(&array)) {
        if (struct_array->num_fields() == 0) {
          check(builder.AppendNulls(array.length()));
          return finish(builder);
        }
      }
    }
    auto printer = json_printer{{.style = no_style(), .oneline = true}};
    auto buffer = std::string{};
    match(array, [&](const auto& array) {
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
    return finish(builder);
  }

  location self_;
  diagnostic_handler& dh_;
  profile_list profiles_;
};

class ocsf_operator final : public crtp_operator<ocsf_operator> {
public:
  ocsf_operator() = default;

  explicit ocsf_operator(struct location self) : self_{self} {
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
      auto ty = as<record_type>(slice.schema());
      auto metadata_index = ty.resolve_field("metadata");
      if (not metadata_index) {
        diagnostic::warning("dropping events where `metadata` does not exist")
          .primary(self_)
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
      auto metadata_array = std::dynamic_pointer_cast<arrow::StructArray>(
        to_record_batch(slice)->column(detail::narrow<int>(*metadata_index)));
      if (not metadata_array) {
        diagnostic::warning("dropping events where `metadata` is not a record")
          .primary(self_)
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
      auto version_index
        = metadata_array->struct_type()->GetFieldIndex("version");
      if (version_index == -1) {
        diagnostic::warning(
          "dropping events where `metadata.version` does not exist")
          .primary(self_)
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
      auto version_array = std::dynamic_pointer_cast<arrow::StringArray>(
        check(metadata_array->GetFlattenedField(version_index)));
      if (not version_array) {
        diagnostic::warning(
          "dropping events where `metadata.version` is not a string")
          .primary(self_)
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
      auto class_index = ty.resolve_field("class_uid");
      if (not class_index) {
        diagnostic::warning("dropping events where `class_uid` does not exist")
          .primary(self_)
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
      auto class_array = std::dynamic_pointer_cast<arrow::Int64Array>(
        to_record_batch(slice)->column(detail::narrow<int>(*class_index)));
      if (not class_array) {
        diagnostic::warning(
          "dropping events where `class_uid` is not an integer")
          .primary(self_)
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
      // Now something for the optional set of profiles in `metadata.profile`.
      auto make_profiles_at = [](std::shared_ptr<arrow::ListArray> list) {
        return [list = std::move(list)](int64_t i) {
          if (not list or list->IsNull(i)) {
            return profile_list{};
          }
          auto offset = list->value_offset(i);
          auto length = list->value_length(i);
          return profile_list{
            static_cast<arrow::StringArray*>(&*list->values()),
            offset,
            length,
          };
        };
      };
      auto profiles_at = std::invoke([&]() {
        auto profiles_index
          = metadata_array->struct_type()->GetFieldIndex("profiles");
        if (profiles_index == -1) {
          return make_profiles_at(nullptr);
        }
        auto profiles_array
          = check(metadata_array->GetFlattenedField(profiles_index));
        if (dynamic_cast<arrow::NullArray*>(&*profiles_array)) {
          return make_profiles_at(nullptr);
        };
        auto profiles_lists = std::dynamic_pointer_cast<arrow::ListArray>(
          std::move(profiles_array));
        if (not profiles_lists) {
          diagnostic::warning("ignoring profiles for events where "
                              "`metadata.profiles` is not a list")
            .primary(self_)
            .emit(ctrl.diagnostics());
          return make_profiles_at(nullptr);
        }
        if (dynamic_cast<arrow::NullArray*>(&*profiles_lists->values())) {
          return make_profiles_at(nullptr);
        }
        if (not dynamic_cast<arrow::StringArray*>(&*profiles_lists->values())) {
          diagnostic::warning("ignoring profiles for events where "
                              "`metadata.profiles` is not a list of strings")
            .primary(self_)
            .emit(ctrl.diagnostics());
          return make_profiles_at(nullptr);
        }
        // Optimize the case where we know that all lists are trivially empty.
        if (profiles_lists->value_offset(0)
            == profiles_lists->value_offset(profiles_lists->length())) {
          return make_profiles_at(nullptr);
        }
        return make_profiles_at(profiles_lists);
      });
      if (not class_array) {
        diagnostic::warning(
          "dropping events where `class_uid` is not an integer")
          .primary(self_)
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
      // Figure out longest slices that share:
      // - metadata.version
      // - metadata.profiles
      // - class_uid
      // TODO: Could do the same for extensions here. We should then simply use
      // `metadata.extensions[].name` the name for them because we would only
      // support extensions that are served by the OCSF schema server, and those
      // have a non-conflicting name and are versioned together with OCSF, so
      // there is no need to take their ID and version into account.
      auto begin = int64_t{0};
      auto end = begin;
      auto version = view_at(*version_array, begin);
      auto id = view_at(*class_array, begin);
      auto profiles = profiles_at(begin);
      auto process = [&]() -> table_slice {
        if (not version) {
          diagnostic::warning(
            "dropping events where `metadata.version` is null")
            .primary(self_)
            .emit(ctrl.diagnostics());
          return {};
        }
        if (not id) {
          diagnostic::warning("dropping events where `class_uid` is null")
            .primary(self_)
            .emit(ctrl.diagnostics());
          return {};
        }
        auto class_name = ocsf_class_name(*id);
        if (not class_name) {
          diagnostic::warning("dropping events where `class_uid` is unknown")
            .primary(self_)
            .note("could not find class for value `{}`", *id)
            .emit(ctrl.diagnostics());
          return {};
        }
        auto schema = std::string{"_ocsf.v"};
        for (auto c : *version) {
          if (c == '.') {
            schema += '_';
          } else {
            schema += c;
          }
        }
        schema += '.';
        auto snake_case_class_name = std::string{};
        for (auto c : *class_name) {
          if (c == ' ') {
            snake_case_class_name += '_';
          } else {
            snake_case_class_name
              += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
          }
        }
        schema += snake_case_class_name;
        auto ty = modules::get_schema(schema);
        if (not ty) {
          diagnostic::warning("could not find schema for the given event")
            .primary(self_)
            .note("tried to find version {:?} for class {:?}", *version,
                  *class_name)
            .emit(ctrl.diagnostics());
          return {};
        }
        auto type_name = "ocsf." + snake_case_class_name;
        auto result = caster{self_, ctrl.diagnostics(), profiles}.cast(
          subslice(slice, begin, end), *ty, type_name);
        return result;
      };
      for (; end < class_array->length(); ++end) {
        auto next_version = view_at(*version_array, end);
        auto next_id = view_at(*class_array, end);
        auto next_profiles = profiles_at(end);
        if (next_version == version and next_id == id
            and next_profiles == profiles) {
          continue;
        }
        co_yield process();
        begin = end;
        version = next_version;
        id = next_id;
        profiles = next_profiles;
      }
      co_yield process();
    }
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "ocsf::apply";
  }

  friend auto inspect(auto& f, ocsf_operator& x) -> bool {
    return f.apply(x.self_);
  }

private:
  struct location self_;
};

class ocsf_plugin final : public operator_plugin2<ocsf_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_(name()).parse(inv, ctx).ignore();
    return std::make_unique<ocsf_operator>(inv.self.get_location());
  }
};

} // namespace
} // namespace tenzir::plugins::ocsf

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::ocsf_plugin)
