#ifndef VAST_FORMAT_ARROW_HPP

#define VAST_FORMAT_ARROW_HPP

#include <string>

#include "plasma/client.h"
#include "plasma/common.h"

#include "arrow/api.h"
#include "arrow/builder.h"

#include "vast/data.hpp"
#include "vast/expected.hpp"
#include "vast/type.hpp"

namespace vast {

class event;

namespace format {
namespace arrow {

/// Converts events into Arrow Record batches and writes them into a Plasma
/// store.

class writer {
public:
  writer() = default;

  /// Constructs an Arrow writer that connects to a (local) plasma store.
  /// @param plasma_socket The path to the local Plasma listening socket.
  writer(const std::string& plasma_socket);

  ~writer();

  expected<void> write(const std::vector<event>& e);

  expected<void> write(const std::vector<event>& e,
                       std::vector<plasma::ObjectID>& oids);

  expected<void> write(const event& x);

  expected<void> flush();

  const char* name() const;

  /// Checks whether the writer is connected to the Plasma store.
  /// @returns `true` if the connection to the Plasma store is alive.
  bool connected() const;

private:
  expected<plasma::ObjectID> make_object(const void* data, size_t size);

  bool connected_;

  plasma::PlasmaClient plasma_client_;

  std::vector<event> buffer_;
};

/// Converts a VAST type to an Arrow schema for a `RecordBatch`.
struct convert_visitor {
  using result_type = std::shared_ptr<::arrow::Field>;

  result_type operator()(const boolean_type&);

  result_type operator()(const count_type&);

  result_type operator()(const integer_type&);

  result_type operator()(const real_type&);

  result_type operator()(const string_type&);

  result_type operator()(const pattern_type&);

  result_type operator()(const address_type&);

  result_type operator()(const port_type&);

  result_type operator()(const subnet_type&);

  result_type operator()(const timespan_type&);

  result_type operator()(const timestamp_type&);

  result_type operator()(const vector_type& t);

  result_type operator()(const set_type& t);

  template <class T>
  result_type operator()(const T& t) {
    // TODO: remove debugging output.
    std::cout << "NONE: " << typeid(t).name() << std::endl;
    return ::arrow::field("none", ::arrow::null());
  }
};

struct insert_visitor_helper {
  using result_type = ::arrow::Status;

  ::arrow::ArrayBuilder* builder;

  insert_visitor_helper(::arrow::ArrayBuilder* b);

  result_type operator()(const boolean_type&, boolean d);

  result_type operator()(const count_type&, count d);

  result_type operator()(const integer_type&, int d);

  result_type operator()(const real_type&, real d);

  result_type operator()(const string_type&, std::string d);

  result_type operator()(const pattern_type&, const pattern& d);

  result_type operator()(const address_type&, const address& d);

  result_type operator()(const port_type&, const port& d);

  result_type operator()(const subnet_type&, const subnet& d);

  result_type operator()(const timespan_type&, const timespan& d);

  result_type operator()(const timestamp_type&, const timestamp& d);

  result_type operator()(const none_type&, none);

  template <class T, class D>
  result_type operator()(const T& t, const D& d) {
    return append_to_list(t, d);
  }
  
  template <class T, class D>
  result_type append_to_list(const T& t, const D& d) {
    if constexpr ((std::is_same_v<T, set_type> || std::is_same_v<T, vector_type>) 
        && (std::is_same_v<D, std::vector<data>> || std::is_same_v<D, set>)) {
      auto l_builder = static_cast<::arrow::ListBuilder*>(builder);
      auto status = l_builder->Reserve(d.size());
      if (!status.ok())
        return status;
      status = l_builder->Append();
      if (!status.ok())
        return status;
      for (auto v : d) {
        format::arrow::insert_visitor_helper a(l_builder->value_builder());
        status = visit(a, t.value_type, v);
        if (!status.ok())
          return status;
      }
      return result_type::OK();
    } else
      return result_type::TypeError("Invalid Type");
  }
};

struct insert_visitor {
  using result_type = ::arrow::Status;

  std::shared_ptr<::arrow::RecordBatchBuilder> rbuilder;

  u_int64_t counter = 0;

  u_int64_t offset = 0;

  u_int64_t c_builder = 0;

  insert_visitor(std::shared_ptr<::arrow::RecordBatchBuilder>& b);

  insert_visitor(std::shared_ptr<::arrow::RecordBatchBuilder>& b, u_int64_t c);

  insert_visitor(std::shared_ptr<::arrow::RecordBatchBuilder>& b, u_int64_t c,
                 u_int64_t c_builder);

  result_type operator()(const record_type& t, const std::vector<data>& d);

  result_type operator()(const count_type& t, count d);

  result_type operator()(const integer_type& t, integer d);

  result_type operator()(const real_type& t, real d);

  result_type operator()(const type& t, const data& d);

  result_type operator()(const string_type& t, std::string d);

  result_type operator()(const boolean_type& t, bool d);

  result_type operator()(const timestamp_type& t, const timestamp& d);

  result_type operator()(const timespan_type& t, const timespan& d);

  result_type operator()(const subnet_type& t, const subnet& d);

  result_type operator()(const address_type& t, const address& d);

  result_type operator()(const port_type& t, const port& d);

  // none data -> AppendNull
  result_type operator()(const none_type&, none);

  result_type operator()(const count_type& t, none);

  result_type operator()(const integer_type& t, none d);

  result_type operator()(const real_type& t, none);

  result_type operator()(const string_type& t, none);

  result_type operator()(const boolean_type& t, none);

  result_type operator()(const timespan_type& t, none);

  result_type operator()(const address_type& t, none);

  result_type operator()(const port_type& t, none);

  result_type operator()(const vector_type& t, none);

  result_type operator()(const set_type& t, none);

  template <class T, class D>
  result_type operator()(const T& t, const D& d) {
    auto l_builder = rbuilder->GetField(c_builder);
    auto a = insert_visitor_helper(l_builder);
    return a.append_to_list(t, d);
  };
};

} // namespace arrow
} // namespace format
} // namespace vast

#endif
