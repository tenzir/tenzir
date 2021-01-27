#include <flatbuffers/reflection.h>
#include <flatbuffers/util.h>

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>

#include "partition_bfbs_generated.h"
#include <sys/mman.h>
#include <sys/stat.h>

const reflection::Object*
getUnionTypeByName(const reflection::Type& union_type,
                   const reflection::Schema& schema, const std::string& name) {
  assert(union_type.base_type() == reflection::BaseType::Union);
  auto underlying_enum = (*schema.enums())[union_type.index()];
  for (auto v : *underlying_enum->values()) {
    if (v->name()->c_str() != name)
      continue;
    auto underlying_type = v->union_type();
    if (underlying_type->base_type() != reflection::BaseType::Obj)
      continue;
    auto object = (*schema.objects())[underlying_type->index()];
    return object;
  }
  return nullptr;
}

size_t getStructSize(const reflection::Field& field,
                     const reflection::Schema& schema) {
  auto base_type = field.type()->base_type();
  assert(base_type == reflection::BaseType::Obj);
  auto index = field.type()->index();
  auto object = schema.objects()->Get(index);
  assert(object->is_struct());
  return object->bytesize();
}

struct page_accesses {
  page_accesses() {
    large_buffer = static_cast<char*>(::malloc(1024 * 1024 * 512));
  }

  void access(const void* p, size_t len = 0) {
    ::memcpy(large_buffer + cursor, p, len);
    cursor += len;

    size_t addr = reinterpret_cast<size_t>(p);
    do {
      pages.insert(addr & ~4095ull);
      len = len >= 4096 ? len - 4096 : 0;
      addr += 4096;
    } while (len > 0);
  }

  char* large_buffer = nullptr; // 512 MiB
  size_t cursor = 0;
  std::set<size_t> pages;
};

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " partition_file\n";
    return 1;
  }

  auto pid = ::getpid();
  std::cout << "pid: " << pid << std::endl;
  ::sleep(10);

  page_accesses pages;

  // TODO: mmap
  // std::string partition_data;
  // if (!flatbuffers::LoadFile(argv[1], true, &partition_data)) {
  // 	std::cerr << "Error loading file\n";
  // 	return 1;
  // }
  struct stat st = {};
  ::stat(argv[1], &st);
  auto fd = ::open(argv[1], O_RDONLY);
  auto map = ::mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);

  const reflection::Schema& schema
    = *reflection::GetSchema(vast::fbs::PartitionBinarySchema::data());
  auto root_table = schema.root_table();
  auto partition_field = root_table->fields()->LookupByKey("partition");

  // pages.access(partition_data.data());
  // auto& partition = *flatbuffers::GetAnyRoot(reinterpret_cast<const
  // uint8_t*>(partition_data.data()));
  auto& partition = *flatbuffers::GetAnyRoot(static_cast<uint8_t*>(map));
  auto& partition_v0_type = flatbuffers::GetUnionType(
    schema, *root_table, *partition_field, partition);
  assert(partition_v0_type.name()->str() == "vast.fbs.partition.v0");

  auto partition_v0 = flatbuffers::GetFieldT(partition, *partition_field);

  auto partition_synopsis_field
    = partition_v0_type.fields()->LookupByKey("partition_synopsis");
  auto partition_synopsis_type
    = schema.objects()->Get(partition_synopsis_field->type()->index());
  auto partition_synopsis
    = flatbuffers::GetFieldT(*partition_v0, *partition_synopsis_field);

  auto synopses_field
    = partition_synopsis_type->fields()->LookupByKey("synopses");
  auto synopses
    = flatbuffers::GetFieldAnyV(*partition_synopsis, *synopses_field);

  for (int i = 0; i < synopses->size(); ++i) {
    auto synopsis
      = flatbuffers::GetAnyVectorElemPointer<const flatbuffers::Table>(synopses,
                                                                       i);
    auto synopsis_type = schema.objects()->Get(synopses_field->type()->index());

    auto bool_synopsis_field
      = synopsis_type->fields()->LookupByKey("bool_synopsis");
    auto time_synopsis_field
      = synopsis_type->fields()->LookupByKey("time_synopsis");
    auto opaque_synopsis_field
      = synopsis_type->fields()->LookupByKey("opaque_synopsis");
    auto qualified_record_field
      = synopsis_type->fields()->LookupByKey("qualified_record_field");

    auto opaque_synopsis_type
      = schema.objects()->Get(opaque_synopsis_field->type()->index());
    auto data_field = opaque_synopsis_type->fields()->LookupByKey("data");

    auto qualified_record
      = flatbuffers::GetFieldV<uint8_t>(*synopsis, *qualified_record_field);
    auto bool_synopsis
      = flatbuffers::GetFieldStruct(*synopsis, *bool_synopsis_field);
    auto time_synopsis
      = flatbuffers::GetFieldStruct(*synopsis, *time_synopsis_field);
    auto opaque_synopsis
      = flatbuffers::GetFieldT(*synopsis, *opaque_synopsis_field);

    if (qualified_record) {
      pages.access(qualified_record->data(), qualified_record->Length());
    }
    if (time_synopsis)
      pages.access(synopsis->GetAddressOf(time_synopsis_field->offset()),
                   getStructSize(*time_synopsis_field, schema));
    else if (bool_synopsis)
      pages.access(synopsis->GetAddressOf(bool_synopsis_field->offset()),
                   getStructSize(*bool_synopsis_field, schema));
    else {
      auto data
        = flatbuffers::GetFieldV<uint8_t>(*opaque_synopsis, *data_field);
      std::cout << "opaq syn size: " << data->size() << "@"
                << (void*) data->data() << std::endl;
      pages.access(data->data(), data->Length());
    }
    // std::cout << "qual rec size: " << qualified_record->size() << "@" <<
    // (void*)qualified_record->data() << std::endl; std::cout << "bool syn
    // size: " << getStructSize(*bool_synopsis_field, schema) << std::endl;
    // std::cout << "time syn size: " << getStructSize(*time_synopsis_field,
    // schema) << std::endl;
  }

  std::cout << "total pages: " << pages.pages.size() << std::endl;
  std::cout << "total bytes: " << pages.cursor << std::endl;
}