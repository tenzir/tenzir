#ifndef VAST_FRAGMENT_H
#define VAST_FRAGMENT_H

#include <cppa/cppa.hpp>
#include "vast/file_system.h"
#include "vast/bitmap_index/address.h"
#include "vast/bitmap_index/arithmetic.h"
#include "vast/bitmap_index/port.h"
#include "vast/bitmap_index/string.h"
#include "vast/bitmap_index/time.h"

namespace vast {

class event;
class expression;

/// A light-weight actor around a bitmap index.
class fragment : public cppa::event_based_actor
{
public:
  /// Spawns a fragment.
  /// @param dir The absolute path to this fragment.
  fragment(path dir);

  virtual ~fragment() = default;
  /// Loads a fragment from the file system.
  virtual void load(path const& dir) = 0;

  /// Writes a fragment to the file system.
  virtual void store(path const& p) = 0;

  /// Records an event into the internal indexes.
  /// @param e The event to index.
  virtual void index(event const& e) = 0;

  /// Looks up an expression.
  /// @param e The expression to throw at the index.
  /// @return A bitstream representing the result of the lookup.
  virtual option<bitstream> lookup(expression const& e) = 0;

  /// Implements `event_based_actor::init`.
  virtual void init() final;

  /// Overrides `event_based_actor::on_exit`.
  virtual void on_exit() final;

protected:
  /// Appends a value to a bitmap index and adds fill if necessary.
  /// @param bmi The bitmap index.
  /// @param event_id The event id.
  /// @param val The value to append.
  /// @return `true` on success.
  static bool append(bitmap_index& bmi, uint64_t event_id, value const& val);

  path const dir_;
};

class meta_fragment : public fragment
{
public:
  meta_fragment(path dir);
  virtual void load(path const& dir) override;
  virtual void store(path const& dir) override;
  virtual void index(const event& e) final;
  virtual option<bitstream> lookup(expression const& e) final;

private:
  time_bitmap_index timestamp_;
  string_bitmap_index name_;
};

class type_fragment : public fragment
{
public:
  type_fragment(path dir);
  virtual void load(path const& dir) override;
  virtual void store(path const& dir) override;
  virtual void index(const event& e) final;
  virtual option<bitstream> lookup(expression const& e) final;

private:
  bool index(uint64_t event_id, value const& v);

  arithmetic_bitmap_index<bool_type> bool_;
  arithmetic_bitmap_index<int_type> int_;
  arithmetic_bitmap_index<uint_type> uint_;
  arithmetic_bitmap_index<double_type> double_;
  time_bitmap_index time_range_;
  time_bitmap_index time_point_;
  string_bitmap_index string_;
  address_bitmap_index address_;
  port_bitmap_index port_;
};

} // namespace vast

#endif
