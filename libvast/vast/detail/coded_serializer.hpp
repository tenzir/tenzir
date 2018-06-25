/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>

#include <caf/none.hpp>
#include <caf/stream_serializer.hpp>

#include "vast/error.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/zigzag.hpp"

namespace vast::detail {

template <class Streambuf>
class coded_serializer : public caf::stream_serializer<Streambuf> {
  using super = caf::stream_serializer<Streambuf>;
  using builtin = typename super::builtin;

public:
  using super::super;

protected:
  template <class T>
  error zig_zag_varbyte_encode(T x) {
    static_assert(std::is_signed_v<T>, "T must be an signed type");
    return this->varbyte_encode(zigzag::encode(x));
  }

  error apply_builtin(builtin type, void* val) override {
    VAST_ASSERT(val != nullptr);
    switch (type) {
      default: // i8_v or u8_v
        VAST_ASSERT(type == builtin::i8_v || type == builtin::u8_v);
        return this->apply_raw(sizeof(uint8_t), val);
      case super::i16_v:
        return zig_zag_varbyte_encode(*reinterpret_cast<int16_t*>(val));
      case super::i32_v:
        return zig_zag_varbyte_encode(*reinterpret_cast<int32_t*>(val));
      case super::i64_v:
        return zig_zag_varbyte_encode(*reinterpret_cast<int64_t*>(val));
      case super::u16_v:
        return this->varbyte_encode(*reinterpret_cast<uint16_t*>(val));
      case super::u32_v:
        return this->varbyte_encode(*reinterpret_cast<uint32_t*>(val));
      case super::u64_v:
        return this->varbyte_encode(*reinterpret_cast<uint64_t*>(val));
      case super::float_v:
        return this->apply_int(
          caf::detail::pack754(*reinterpret_cast<float*>(val)));
      case super::double_v:
        return this->apply_int(
          caf::detail::pack754(*reinterpret_cast<double*>(val)));
      case super::ldouble_v: {
        // the IEEE-754 conversion does not work for long double
        // => fall back to string serialization (event though it sucks)
        std::ostringstream oss;
        oss << std::setprecision(std::numeric_limits<long double>::digits)
            << *reinterpret_cast<long double*>(val);
        auto tmp = oss.str();
        return this->apply(tmp);
      }
      case super::string8_v: {
        auto& str = *reinterpret_cast<std::string*>(val);
        auto size = str.size();
        return error::eval([&] { return this->begin_sequence(size); },
                           [&] { return this->apply_raw(size, &str[0]); },
                           [&] { return this->end_sequence(); });
      }
      case super::string16_v: {
        auto str = reinterpret_cast<std::u16string*>(val);
        auto size = str->size();
        // the standard does not guarantee that char16_t is exactly 16 bits...
        return error::eval([&] { return this->begin_sequence(size); },
                           [&] { return this->template
                                   consume_range_c<uint16_t>(*str); },
                           [&] { return this->end_sequence(); });
      }
      case super::string32_v: {
        auto str = reinterpret_cast<std::u32string*>(val);
        auto size = str->size();
        // the standard does not guarantee that char32_t is exactly 32 bits...
        return error::eval([&] { return this->begin_sequence(size); },
                           [&] { return this->template
                                   consume_range_c<uint32_t>(*str); },
                           [&] { return this->end_sequence(); });
      }
    }
  }
};

} // namespace vast::detail

