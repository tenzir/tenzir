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

class caster {
public:
  explicit caster(location self, diagnostic_handler& dh)
    : self_{self}, dh_{dh} {
  }

  auto cast(const table_slice& slice, const type& ty) -> table_slice {
    auto array = check(to_record_batch(slice)->ToStructArray());
    TENZIR_ASSERT(array);
    auto result
      = std::dynamic_pointer_cast<arrow::StructArray>(cast(*array, ty));
    TENZIR_ASSERT(result);
    auto columns = check(result->Flatten());
    // TODO: Don't we need matching metadata here?
    return table_slice{
      arrow::RecordBatch::Make(ty.to_arrow_schema(), result->length(),
                               std::move(columns)),
      ty,
    };
  }

private:
  template <basic_type Type>
  auto cast(const type_to_arrow_array_t<Type>& array, const Type&)
    -> std::shared_ptr<type_to_arrow_array_t<Type>> {
    return std::make_shared<type_to_arrow_array_t<Type>>(array.data());
  }

  auto cast(const arrow::Array& array, const type& ty)
    -> std::shared_ptr<arrow::Array> {
    if (is<string_type>(ty) and ty.attribute("print_json")) {
      return print_json(array);
    }
    auto result = match(
      std::tie(array, ty),
      [&]<class Type>(const type_to_arrow_array_t<Type>& array,
                      const Type& ty) -> std::shared_ptr<arrow::Array> {
        return cast(array, ty);
      },
      [&]<class Array, class Type>(const Array& array, const Type& ty)
        requires(not std::same_as<Array, type_to_arrow_array_t<Type>>)
      {
        if constexpr (not std::same_as<Array, arrow::NullArray>) {
          // TODO: Give field path here!
          diagnostic::warning("expected type `{}` for field, but got `{}`",
                              type_kind::of<Type>,
                              type_kind::of<type_from_arrow_t<Array>>)
            .primary(self_)
            .emit(dh_);
        }
        // TODO: If we check `#required`, we need to check this as well.
        return check(
          arrow::MakeArrayOfNull(ty.to_arrow_type(), array.length()));
      });
    if (ty.attribute("required") and result->null_count() > 0) {
      // TODO: Warn because required attribute is not set.
    }
    return result;
  }

  auto cast(const enumeration_type::array_type&, const enumeration_type&)
    -> std::shared_ptr<enumeration_type::array_type> {
    TENZIR_UNREACHABLE();
  }

  auto cast(const arrow::MapArray&, const map_type&)
    -> std::shared_ptr<arrow::MapArray> {
    TENZIR_UNREACHABLE();
  }

  auto cast(const arrow::ListArray& array, const list_type& ty)
    -> std::shared_ptr<arrow::ListArray> {
    auto values = cast(*array.values(), ty.value_type());
    // TODO: What about `array.offset()`?
    return check(arrow::ListArray::FromArrays(
      *array.offsets(), *values, arrow::default_memory_pool(),
      array.null_bitmap(), array.data()->null_count));
  }

  auto cast(const arrow::StructArray& array, const record_type& ty)
    -> std::shared_ptr<arrow::StructArray> {
    auto fields = arrow::FieldVector{};
    auto field_arrays = arrow::ArrayVector{};
    for (auto&& field : ty.fields()) {
      fields.push_back(field.type.to_arrow_field(field.name));
      auto field_array = array.GetFieldByName(std::string{field.name});
      if (not field_array) {
        // No warning if the a target field does not exist.
        // TODO: Maybe if it is required?
        field_arrays.push_back(check(
          arrow::MakeArrayOfNull(field.type.to_arrow_type(), array.length())));
        continue;
      }
      field_arrays.push_back(cast(*field_array, field.type));
    }
    for (auto& field : array.struct_type()->fields()) {
      // Warn for fields that do not exist in the target type.
      if (not ty.has_field(field->name())) {
        // TODO: Should give full path to field.
        // TODO: Field name should probably not be included in message.
        diagnostic::warning(
          "dropping field `{}` which does not exist in schema", field->name())
          .primary(self_)
          .emit(dh_);
      }
    }
    return make_struct_array(array.length(), array.null_bitmap(), fields,
                             field_arrays);
  }

  auto print_json(const arrow::Array& array)
    -> std::shared_ptr<arrow::StringArray> {
    if (is<arrow::StringArray>(array)) {
      // Keep strings as they are (assuming they are already JSON).
      return std::make_shared<arrow::StringArray>(array.data());
    }
    // Convert everything else into JSON.
    auto builder = arrow::StringBuilder{};
    auto printer = json_printer{{.style = no_style(), .oneline = true}};
    auto buffer = std::string{};
    // TODO: At least for `unmapped`, we should think about encoding empty
    // records as `null` as well. However, for other fields, maybe not.
    // auto empty_records = match(
    //   ty,
    //   [](const record_type& ty) {
    //     return ty.num_fields() == 0;
    //   },
    //   [](const auto&) {
    //     return false;
    //   });
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
      auto ty = as<record_type>(slice.schema());
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
      // Figure out longest slices for `(metadata.version, class_uid)`.
      auto begin = int64_t{0};
      auto end = begin;
      auto version = view_at(*version_array, begin);
      auto id = view_at(*class_array, begin);
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
        for (auto c : *class_name) {
          if (c == ' ') {
            schema += '_';
          } else {
            schema
              += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
          }
        }
        auto ty = modules::get_schema(schema);
        if (not ty) {
          diagnostic::warning("dropping events with unknown version `{}` for "
                              "class `{}`",
                              *version, *class_name)
            .primary(self_)
            .emit(ctrl.diagnostics());
          return {};
        }
        // TODO: Drop metadata after casting and rename!
        return caster{self_, ctrl.diagnostics()}.cast(
          subslice(slice, begin, end), *ty);
      };
      for (; end < class_array->length(); ++end) {
        auto next_version = view_at(*version_array, end);
        auto next_id = view_at(*class_array, end);
        if (next_version == version and next_id == id) {
          continue;
        }
        co_yield process();
        begin = end;
        version = next_version;
        id = next_id;
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
