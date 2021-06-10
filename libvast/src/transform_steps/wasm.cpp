//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform_steps/wasm.hpp"

#include "vast/arrow_table_slice_builder.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/error.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/io/read.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <arrow/ipc/reader.h>
#include <fmt/format.h>

#include <wasmer.h>

namespace {

// Use the last_error API to retrieve error messages
static void print_wasmer_error() {
  int error_len = wasmer_last_error_length();
  char* error_str = static_cast<char*>(malloc(error_len));
  wasmer_last_error_message(error_str, error_len);
  printf("Error: %s\n", error_str);
}

static int find_export_by_name(wasm_module_t* module, const char* name) {
  wasm_exporttype_vec_t module_exports;
  wasm_module_exports(module, &module_exports);
  for (size_t i = 0; i < module_exports.size; ++i) {
    auto symbol_name = wasm_exporttype_name(module_exports.data[i]);
    if (!strncmp(name, symbol_name->data, symbol_name->size))
      return i;
  }
  return -1;
}

template <class Callback>
class record_batch_listener final : public arrow::ipc::Listener {
public:
  record_batch_listener(Callback&& callback) noexcept
    : callback_{std::forward<Callback>(callback)} {
    // nop
  }

  virtual ~record_batch_listener() noexcept override = default;

private:
  arrow::Status OnRecordBatchDecoded(
    std::shared_ptr<arrow::RecordBatch> record_batch) override {
    std::invoke(callback_, std::move(record_batch));
    return arrow::Status::OK();
  }

  Callback callback_;
};

template <class Callback>
auto make_record_batch_listener(Callback&& callback) {
  return std::make_shared<record_batch_listener<Callback>>(
    std::forward<Callback>(callback));
}

} // namespace

namespace vast {

struct wasm_step::wasm_step_impl {
  wasm_engine_t* engine;
  wasm_store_t* store;
  wasm_module_t* module;
  wasm_memory_t* memory;
  wasm_instance_t* instance;
  const wasm_func_t* transform_fn;

  ~wasm_step_impl() {
    // FIXME!
  }
};

wasm_step::wasm_step(chunk_ptr program)
  : pimpl_(std::make_unique<wasm_step_impl>()) {
  auto& state = *pimpl_;
  // Create engine.
  // TODO: We should even be able to make a global engine
  // for the plugin itself, and have one module per step?
  state.engine = wasm_engine_new();
  if (!state.engine) {
    VAST_ERROR("couldn't create engine");
    return;
  }
  // Create store.
  state.store = wasm_store_new(state.engine);
  if (!state.store) {
    VAST_ERROR("couldn't create store");
    return;
  }
  // Create the module.
  wasm_byte_vec_t wasm;
  wasm_byte_vec_new_uninitialized(&wasm, program->size());
  ::memcpy(wasm.data, program->data(), program->size());
  state.module = wasm_module_new(state.store, &wasm);
  if (!state.module) {
    print_wasmer_error();
    VAST_ERROR("couldn't create module from program with {} bytes",
               program->size());
    return;
  }
  // Create memory.
  // TODO: use real values for min/max
  wasm_limits_t memory_limits;
  memory_limits.min = 15;
  memory_limits.max = 25;
  wasm_memorytype_t* memtype = wasm_memorytype_new(&memory_limits);
  state.memory = wasm_memory_new(state.store, memtype);
  if (!state.memory) {
    VAST_ERROR("couldn't create memory");
    return;
  }
  // Create the imports
  std::map<std::pair<std::string, std::string>, wasm_extern_t*>
    satisfiable_imports;
  satisfiable_imports[{"env", "memory"}] = wasm_memory_as_extern(state.memory);
  wasm_importtype_vec_t module_imports;
  wasm_module_imports(state.module, &module_imports);
  wasm_extern_vec_t imports;
  wasm_extern_vec_new_uninitialized(&imports, module_imports.size);
  for (size_t i = 0; i < module_imports.size; ++i) {
    const auto im = module_imports.data[i];
    auto name = wasm_importtype_name(im);
    auto module = wasm_importtype_module(im);
    auto both = std::make_pair(std::string{module->data, module->size},
                               std::string{name->data, name->size});
    auto it = satisfiable_imports.find(both);
    if (it == satisfiable_imports.end())
      return;
    imports.data[i] = it->second;
  }
  // Create the instance
  wasm_trap_t* traps = nullptr;
  state.instance
    = wasm_instance_new(state.store, state.module, &imports, &traps);
  //
  auto transform_idx = find_export_by_name(state.module, "transform");
  if (transform_idx == -1)
    return;
  wasm_exporttype_vec_t module_exports;
  wasm_module_exports(state.module, &module_exports);
  auto transform_export = module_exports.data[transform_idx];
  // Verify that the 'transform' function has the correct signature
  auto transform_extern = wasm_exporttype_type(transform_export);
  auto kind = wasm_externtype_kind(transform_extern);
  if (kind != WASM_EXTERN_FUNC)
    return;
  auto transform_fntype = wasm_externtype_as_functype_const(transform_extern);
  auto params = wasm_functype_params(transform_fntype);
  auto results = wasm_functype_results(transform_fntype);
  if (params->size != 2)
    return;
  if (results->size != 0)
    return;
  // TODO: verify that all args are of type i32
  wasm_extern_vec_t out;
  wasm_instance_exports(state.instance, &out);
  auto transform_fn = wasm_extern_as_func(out.data[transform_idx]);
  state.transform_fn = transform_fn;
}

caf::expected<table_slice> wasm_step::operator()(table_slice&& slice) const {
  auto& state = *pimpl_;
  wasm_trap_t* traps = nullptr;
  // Require
  if (slice.encoding() != table_slice_encoding::arrow) {
    slice = rebuild(slice, table_slice_encoding::arrow);
  }
  auto bytes = as_bytes(slice);
  auto fb = fbs::GetTableSlice(bytes.data());
  auto fb_arrow = fb->table_slice_as_arrow_v0();
  auto schema = fb_arrow->schema();
  auto batch = fb_arrow->record_batch();
  auto total_size = schema->size() + batch->size();
  if (total_size > wasm_memory_data_size(state.memory)) {
    // TODO: attempt to grow memory
    return caf::make_error(ec::out_of_memory, "table slice too big for wasm "
                                              "memory");
  }
  // Prepare args.
  auto mem_schema = wasm_memory_data(state.memory);
  auto mem_batch = mem_schema + schema->size();
  ::memcpy(mem_schema, schema->data(), schema->size());
  ::memcpy(mem_batch, batch->data(), batch->size());
  wasm_val_t args_values[2]
    = {WASM_I32_VAL(0), WASM_I32_VAL(static_cast<int>(schema->size()))};
  wasm_val_vec_t args = WASM_ARRAY_VEC(args_values);
  // TODO: Allow return types so the transform can actually modify things
  wasm_val_vec_t rets = WASM_EMPTY_VEC;
  // Do the function call.
  wasm_trap_t* trap = wasm_func_call(state.transform_fn, &args, &rets);
  if (trap) {
    wasm_name_t message;
    wasm_trap_message(trap, &message);
    return caf::make_error(ec::unspecified, fmt::format("{}", message.data));
    // TODO: free message
  }
  // Transform the result back into a table_slice
  std::shared_ptr<arrow::RecordBatch> result = nullptr;
  arrow::ipc::StreamDecoder decoder{make_record_batch_listener(
    [&](std::shared_ptr<arrow::RecordBatch> record_batch) {
      result = std::move(record_batch);
    })};
  decoder.Consume((const uint8_t*)mem_schema, schema->size());
  decoder.Consume((const uint8_t*)mem_batch, batch->size());
  if (!result) {
    return caf::make_error(ec::convert_error, "couldn't deserialize result "
                                              "batch");
  }
  return table_slice{result, slice.layout()};
}

class wasm_step_plugin final : public virtual transform_plugin {
public:
  // Plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "wasm";
  };

  // Transform Plugin API
  [[nodiscard]] caf::expected<transform_step_ptr>
  make_transform_step(const caf::settings& opts) const override {
    auto program = caf::get_if<std::string>(&opts, "program");
    if (!program)
      return caf::make_error(ec::invalid_configuration, "missing 'program' key "
                                                        "with path to .wasm "
                                                        "program");
    std::filesystem::path program_path{*program};
    auto vec = io::read(program_path);
    if (!vec)
      return vec.error();
    auto data = chunk::make(std::move(*vec));
    return std::make_unique<wasm_step>(std::move(data));
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::wasm_step_plugin)
