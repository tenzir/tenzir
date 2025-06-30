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
         string_list extensions, bool print_json)
    : self_{self},
      dh_{dh},
      profiles_{profiles},
      extensions_{extensions},
      print_json_{print_json} {
  }

  auto cast(const table_slice& slice, const type& ty, std::string_view name)
    -> table_slice {
    auto array = check(to_record_batch(slice)->ToStructArray());
    TENZIR_ASSERT(array);
    auto result
      = cast(series{slice.schema(),
                    std::static_pointer_cast<arrow::Array>(std::move(array))},
             ty, "");
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
    for (auto [field_name, field_ty] : ty.fields()) {
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
    if (ty.attribute("print_json")) {
      if (print_json_) {
        return type{string_type{}};
      }
      // We don't know the actual type, so we just use `null`.
      return type{null_type{}};
    }
    return match(ty, [&](const auto& ty) {
      return type{cast_type(ty)};
    });
  }

  template <basic_type Type>
  auto cast(basic_series<Type> input, const Type&, std::string_view path)
    -> basic_series<Type> {
    (void)path;
    return input;
  }

  auto cast(series input, const type& ty, std::string_view path) -> series {
    if (ty.attribute("print_json")) {
      TENZIR_ASSERT(is<string_type>(ty));
      if (ty.attribute("must_be_record")
          and not(
            input.type.kind().is_any<null_type, record_type>()
            // Strings are also allowed so that `ocsf::apply` is idempotent.
            or (print_json_ and input.type.kind().is<string_type>()))) {
        diagnostic::warning("expected type `record` for `{}`, but got `{}`",
                            path, input.type.kind())
          .primary(self_)
          .emit(dh_);
        auto result_ty = print_json_ ? type{string_type{}} : type{null_type{}};
        return series{result_ty, check(arrow::MakeArrayOfNull(
                                   result_ty.to_arrow_type(), input.length()))};
      }
      if (print_json_) {
        return series{
          string_type{},
          print_json(*input.array,
                     ty.attribute("nullify_empty_records").has_value())};
      }
      // Otherwise, we just return the data exactly as we received it, without
      // any further casting.
      // TODO: Should we also nullify empty records here?
      return input;
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

  auto cast(basic_series<enumeration_type>, const enumeration_type&,
            std::string_view) -> basic_series<enumeration_type> {
    TENZIR_UNREACHABLE();
  }

  auto cast(basic_series<map_type>, const map_type&, std::string_view)
    -> basic_series<map_type> {
    TENZIR_UNREACHABLE();
  }

  auto cast(basic_series<list_type> input, const list_type& ty,
            std::string_view path) -> basic_series<list_type> {
    auto values = cast(series{input.type.value_type(), input.array->values()},
                       ty.value_type(), std::string{path} + "[]");
    return {list_type{values.type},
            check(arrow::ListArray::FromArrays(
              *input.array->offsets(), *values.array,
              arrow::default_memory_pool(), input.array->null_bitmap(),
              input.array->data()->null_count))};
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
            std::string_view path) -> basic_series<record_type> {
    auto fields = std::vector<record_type::field_view>{};
    auto field_arrays = arrow::ArrayVector{};
    for (auto&& field : ty.fields()) {
      if (not is_enabled(field.type)) {
        continue;
      }
      auto field_series = input.field(field.name);
      if (not field_series) {
        // No warning if the a target field does not exist.
        auto cast_ty = cast_type(field.type);
        fields.emplace_back(field.name, cast_ty);
        field_arrays.push_back(check(arrow::MakeArrayOfNull(
          cast_ty.to_arrow_type(), input.array->length())));
        continue;
      }
      auto field_path = std::string{path};
      if (not field_path.empty()) {
        field_path += '.';
      }
      field_path += field.name;
      auto casted = cast(std::move(*field_series), field.type, field_path);
      field_arrays.push_back(std::move(casted.array));
      fields.emplace_back(field.name, std::move(casted.type));
    }
    for (auto& field : input.array->struct_type()->fields()) {
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
    return {record_type{fields},
            make_struct_array(input.length(), input.array->null_bitmap(),
                              arrow_fields, field_arrays)};
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
    // TODO: Resolve enumerations?
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
  string_list profiles_;
  string_list extensions_;
  bool print_json_;
};

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

class ocsf_operator final : public crtp_operator<ocsf_operator> {
public:
  ocsf_operator() = default;

  ocsf_operator(struct location self, bool print_json)
    : self_{self}, print_json_{print_json} {
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
      if (not class_array) {
        diagnostic::warning(
          "dropping events where `class_uid` is not an integer")
          .primary(self_)
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
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
        auto extensions_structs
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
        auto name_lists = std::make_shared<arrow::ListArray>(
          arrow::list(name_array->type()), extensions_lists->length(),
          extensions_lists->value_offsets(), std::move(name_array),
          extensions_lists->null_bitmap(), extensions_lists->data()->null_count,
          extensions_lists->offset());
        check(name_lists->ValidateFull());
        return make_string_list_function(std::move(name_lists));
      });
      // Figure out longest slices that share:
      // - metadata.version
      // - metadata.profiles
      // - class_uid
      // - metadata.extensions[].name
      // Since we only support extensions that are served by the OCSF server for
      // the corresponding version, we know that they have a non-conflicting
      // name and there is no need to take their version into account (although
      // we could check for consistency with the event).
      auto begin = int64_t{0};
      auto end = begin;
      // TODO: If any of these attributes is changing with a high-frequency in
      // the input stream, this operator will produce very small batches. This
      // could be fixed by reordering events if needed.
      auto version = view_at(*version_array, begin);
      auto id = view_at(*class_array, begin);
      auto profiles = profiles_at(begin);
      auto extensions = extensions_at(begin);
      auto process = [&]() -> table_slice {
        if (not version) {
          diagnostic::warning(
            "dropping events where `metadata.version` is null")
            .primary(self_)
            .emit(ctrl.diagnostics());
          return {};
        }
        auto parsed_version = parse_ocsf_version(*version);
        if (not parsed_version) {
          diagnostic::warning("dropping events with unknown OCSF version",
                              *version)
            .primary(self_)
            .note("found {:?}", *version)
            .emit(ctrl.diagnostics());
          return {};
        }
        if (not id) {
          diagnostic::warning("dropping events where `class_uid` is null")
            .primary(self_)
            .emit(ctrl.diagnostics());
          return {};
        }
        auto class_name = ocsf_class_name(*parsed_version, *id);
        if (not class_name) {
          diagnostic::warning("dropping events where `class_uid` is unknown")
            .primary(self_)
            .note("could not find class for value `{}`", *id)
            .emit(ctrl.diagnostics());
          return {};
        }
        auto snake_case_class_name = std::string{};
        for (auto c : *class_name) {
          if (c == ' ') {
            snake_case_class_name += '_';
          } else {
            snake_case_class_name
              += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
          }
        }
        auto schema = fmt::format("_ocsf.{}.{}", mangle_version(*version),
                                  snake_case_class_name);
        auto ty = modules::get_schema(schema);
        if (not ty) {
          diagnostic::warning("could not find schema for the given event")
            .primary(self_)
            .note("tried to find version {:?} for class {:?}", *version,
                  *class_name)
            .emit(ctrl.diagnostics());
          return {};
        }
        auto extension = ty->attribute("extension");
        if (extension and not extensions.contains(*extension)) {
          diagnostic::warning("dropping event for class {:?} because extension "
                              "{:?} is not enabled",
                              *class_name, *extension)
            .primary(self_)
            .emit(ctrl.diagnostics());
          return {};
        }
        auto type_name = "ocsf." + snake_case_class_name;
        auto result
          = caster{self_, ctrl.diagnostics(), profiles, extensions, print_json_}
              .cast(subslice(slice, begin, end), *ty, type_name);
        return result;
      };
      for (; end < class_array->length(); ++end) {
        auto next_version = view_at(*version_array, end);
        auto next_id = view_at(*class_array, end);
        auto next_profiles = profiles_at(end);
        auto next_extensions = extensions_at(end);
        if (next_version == version and next_id == id
            and next_profiles == profiles and extensions == next_extensions) {
          continue;
        }
        co_yield process();
        begin = end;
        version = next_version;
        id = next_id;
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
    return "ocsf::apply";
  }

  friend auto inspect(auto& f, ocsf_operator& x) -> bool {
    return f.object(x).fields(f.field("self", x.self_),
                              f.field("print_json", x.print_json_));
  }

private:
  struct location self_;
  bool print_json_{};
};

class ocsf_plugin final : public operator_plugin2<ocsf_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto print_json = true;
    argument_parser2::operator_(name())
      .named("print_json", print_json)
      .parse(inv, ctx)
      .ignore();
    return std::make_unique<ocsf_operator>(inv.self.get_location(), print_json);
  }
};

} // namespace
} // namespace tenzir::plugins::ocsf

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::ocsf_plugin)
