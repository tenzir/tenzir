#include "vast/transform_steps/delete.hpp"

#include "vast/error.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"

#if VAST_ENABLE_ARROW > 0
#  include "arrow/type.h"
#endif

namespace vast {

delete_step::delete_step(const std::string& fieldname) : fieldname_(fieldname) {
}

caf::expected<table_slice> delete_step::operator()(table_slice&& slice) const {
  const auto& layout = slice.layout();
  auto offset = layout.resolve(fieldname_);
  if (!offset)
    return std::move(slice);
  auto flat_index = layout.flat_index_at(*offset);
  // We just got the offset from `layout`, so it should be valid.
  VAST_ASSERT(flat_index);
  auto column_index = static_cast<int>(*flat_index);
  auto modified_fields = layout.fields;
  modified_fields.erase(modified_fields.begin() + column_index);
  vast::record_type modified_layout(modified_fields);
  modified_layout.name(layout.name());
  auto builder_ptr
    = factory<table_slice_builder>::make(slice.encoding(), modified_layout);
  builder_ptr->reserve(slice.rows());
  for (size_t i = 0; i < slice.rows(); ++i) {
    for (size_t j = 0; j < slice.columns(); ++j) {
      if (j == flat_index)
        continue;
      if (!builder_ptr->add(slice.at(i, j)))
        return caf::make_error(ec::unspecified, "delete step: unknown error "
                                                "in table slice builder");
    }
  }
  return builder_ptr->finish();
}

#if VAST_ENABLE_ARROW > 0

std::pair<vast::record_type, std::shared_ptr<arrow::RecordBatch>>
delete_step::operator()(vast::record_type layout,
                        std::shared_ptr<arrow::RecordBatch> batch) const {
  auto offset = layout.resolve(fieldname_);
  if (!offset)
    return std::make_pair(std::move(layout), std::move(batch));
  auto flat_index = layout.flat_index_at(*offset);
  // We just got the offset from `layout`, so it should be valid.
  VAST_ASSERT(flat_index);
  auto column_index = static_cast<int>(*flat_index);
  auto result_fields = layout.fields;
  result_fields.erase(result_fields.begin() + column_index);
  vast::record_type result_layout(result_fields);
  result_layout.name(layout.name());
  auto columns = batch->columns();
  columns.erase(columns.begin() + column_index);
  auto schema = batch->schema()->RemoveField(column_index);
  if (!schema.ok())
    return std::make_pair(std::move(layout), nullptr);
  auto result_batch
    = arrow::RecordBatch::Make(schema.ValueOrDie(), batch->num_rows(), columns);
  return std::make_pair(result_layout, result_batch);
}

#endif

class delete_step_plugin final : public virtual transform_plugin {
public:
  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "delete";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<transform_step_ptr>
  make_transform_step(const caf::settings& opts) const override {
    auto field = caf::get_if<std::string>(&opts, "field");
    if (!field)
      return caf::make_error(ec::invalid_configuration,
                             "key 'field' is missing or not a string in "
                             "configuration for delete step");
    return std::make_unique<delete_step>(*field);
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::delete_step_plugin)