#include "vast/address.h"
#include "vast/announce.h"
#include "vast/bitmap_index_polymorphic.h"
#include "vast/bitstream.h"
#include "vast/chunk.h"
#include "vast/expression.h"
#include "vast/event.h"
#include "vast/filesystem.h"
#include "vast/key.h"
#include "vast/offset.h"
#include "vast/operator.h"
#include "vast/pattern.h"
#include "vast/query_options.h"
#include "vast/time.h"
#include "vast/type.h"
#include "vast/uuid.h"
#include "vast/value.h"
#include "vast/concept/serializable/builtin.h"
#include "vast/concept/serializable/bitstream_polymorphic.h"
#include "vast/concept/serializable/bitmap_index_polymorphic.h"
#include "vast/concept/serializable/data.h"
#include "vast/concept/serializable/expression.h"
#include "vast/concept/serializable/schema.h"
#include "vast/concept/serializable/state.h"
#include "vast/concept/serializable/type.h"
#include "vast/concept/serializable/vector_event.h"
#include "vast/concept/serializable/caf/message.h"
#include "vast/concept/serializable/std/array.h"
#include "vast/concept/serializable/std/chrono.h"
#include "vast/concept/serializable/std/string.h"
#include "vast/concept/serializable/std/unordered_map.h"
#include "vast/concept/serializable/std/vector.h"
#include "vast/concept/serializable/std/map.h"
#include "vast/concept/state/address.h"
#include "vast/concept/state/bitmap_index.h"
#include "vast/concept/state/block.h"
#include "vast/concept/state/chunk.h"
#include "vast/concept/state/event.h"
#include "vast/concept/state/filesystem.h"
#include "vast/concept/state/pattern.h"
#include "vast/concept/state/time.h"
#include "vast/concept/state/type.h"
#include "vast/concept/state/uuid.h"
#include "vast/concept/state/value.h"
#include "vast/concept/state/util/error.h"
#include "vast/concept/state/util/none.h"
#include "vast/io/compression.h"

namespace vast {
namespace {

template <typename Bitstream>
void announce_bmi_hierarchy(std::string const& bs_name)
{
  using detail::bitmap_index_concept;
  using detail::bitmap_index_model;
  static auto wrap = 
    [](std::string const& prefix, std::string const& str, std::string const& t)
      -> std::string
  {
    return prefix + '<' + str + ">,T=" + t;
  };
  static auto model_wrap = [](std::string const& str, std::string const& t)
    -> std::string
  {
    return wrap("vast::detail::bitmap_index_model", str, t);
  };
  announce<arithmetic_bitmap_index<Bitstream, boolean>>(
    wrap("vast::arithmetic_bitmap_index", "T,boolean", bs_name));
  announce<arithmetic_bitmap_index<Bitstream, integer>>(
    wrap("vast::arithmetic_bitmap_index", "T,integer", bs_name));
  announce<arithmetic_bitmap_index<Bitstream, count>>(
    wrap("vast::arithmetic_bitmap_index", "T,count", bs_name));
  announce<arithmetic_bitmap_index<Bitstream, real>>(
    wrap("vast::arithmetic_bitmap_index", "T,real", bs_name));
  announce<arithmetic_bitmap_index<Bitstream, time::point>>(
    wrap("vast::arithmetic_bitmap_index", "T,time::point", bs_name));
  announce<arithmetic_bitmap_index<Bitstream, time::duration>>(
    wrap("vast::arithmetic_bitmap_index", "T,time::duration", bs_name));
  announce<address_bitmap_index<Bitstream>>(
    wrap("vast::address_bitmap_index", "T", bs_name));
  announce<subnet_bitmap_index<Bitstream>>(
    wrap("vast::subnet_bitmap_index", "T", bs_name));
  announce<port_bitmap_index<Bitstream>>(
    wrap("vast::port_bitmap_index", "T", bs_name));
  announce<string_bitmap_index<Bitstream>>(
    wrap("vast::string_bitmap_index", "T", bs_name));
  announce<sequence_bitmap_index<Bitstream>>(
    wrap("vast::sequence_bitmap_index", "T", bs_name));
  announce_hierarchy<
    bitmap_index_concept<Bitstream>,
    bitmap_index_model<arithmetic_bitmap_index<Bitstream, boolean>>,
    bitmap_index_model<arithmetic_bitmap_index<Bitstream, integer>>,
    bitmap_index_model<arithmetic_bitmap_index<Bitstream, count>>,
    bitmap_index_model<arithmetic_bitmap_index<Bitstream, real>>,
    bitmap_index_model<arithmetic_bitmap_index<Bitstream, time::point>>,
    bitmap_index_model<arithmetic_bitmap_index<Bitstream, time::duration>>,
    bitmap_index_model<address_bitmap_index<Bitstream>>,
    bitmap_index_model<subnet_bitmap_index<Bitstream>>,
    bitmap_index_model<port_bitmap_index<Bitstream>>,
    bitmap_index_model<string_bitmap_index<Bitstream>>,
    bitmap_index_model<sequence_bitmap_index<Bitstream>>
  >(model_wrap("arithmetic_bitmap_index<T,boolean>", bs_name),
    model_wrap("arithmetic_bitmap_index<T,integer>", bs_name),
    model_wrap("arithmetic_bitmap_index<T,count>", bs_name),
    model_wrap("arithmetic_bitmap_index<T,real>", bs_name),
    model_wrap("arithmetic_bitmap_index<T,time::point>", bs_name),
    model_wrap("arithmetic_bitmap_index<T,time::duration>", bs_name),
    model_wrap("address_bitmap_index<T>", bs_name),
    model_wrap("subnet_bitmap_index<T>", bs_name),
    model_wrap("port_bitmap_index<T>", bs_name),
    model_wrap("string_bitmap_index<T>", bs_name),
    model_wrap("sequence_bitmap_index<T>", bs_name)
  );
}

} // namespace <anonymous>

void announce_types()
{
  announce<path>("vast::path");
  announce<uuid>("vast::uuid");
  announce<arithmetic_operator>("vast::arithmetic_operator");
  announce<relational_operator>("vast::relational_operator");
  announce<boolean_operator>("vast::boolean_operator");
  announce<query_options>("vast::query_options");
  announce<block>("vast::block");
  announce<chunk>("vast::chunk");
  announce<schema>("vast::schema");
  announce<time::point>("vast::time::point");
  announce<time::duration>("vast::time::duration");
  announce<time::moment>("vast::time::moment");
  announce<time::extent>("vast::time::extent");
  announce<pattern>("vast::pattern");
  announce<vector>("vast::vector");
  announce<set>("vast::set");
  announce<table>("vast::table");
  announce<record>("vast::record");
  announce<address>("vast::address");
  announce<subnet>("vast::subnet");
  announce<port>("vast::port");
  announce<data>("vast::data");
  announce<type>("vast::type");
  announce<key>("vast::key");
  announce<offset>("vast::offset");
  announce<value>("vast::value");
  announce<event>("vast::event");
  announce<expression>("vast::expression");
  announce<predicate>("vast::predicate");
  announce<io::compression>("vast::io::compression");
  announce<none>("vast::util::none");
  announce<error>("vast::util::error");
  // std::vector<T>
  announce<std::vector<data>>("std::vector<vast::data>");
  announce<std::vector<event>>("std::vector<vast::event>");
  announce<std::vector<value>>("std::vector<vast::value>");
  announce<std::vector<uuid>>("std::vector<vast::uuid>");
  // std::map<T,U>
  announce<std::map<std::string, caf::message>>(
    "std::map<std::string,caf::message>>");
  // Polymorphic bitstreams
  announce<ewah_bitstream>("vast::ewah_bitstream");
  announce<null_bitstream>("vast::null_bitstream");
  announce_hierarchy<
    detail::bitstream_concept,
    detail::bitstream_model<null_bitstream>,
    detail::bitstream_model<ewah_bitstream>
  >("vast::detail::bitstream_model<vast::null_bitstream>",
    "vast::detail::bitstream_model<vast::ewah_bitstream>"
  );
  // Polymorphic bitmap indexes.
  announce_bmi_hierarchy<ewah_bitstream>("ewah_bitstream");
  announce_bmi_hierarchy<null_bitstream>("null_bitstream");
}

} // namespace vast
