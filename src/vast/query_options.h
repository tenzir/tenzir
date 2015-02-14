#ifndef VAST_QUERY_OPTIONS_H
#define VAST_QUERY_OPTIONS_H

namespace vast {

/// Stores query options.
enum class query_options : uint32_t
{
  none = 0x00,
  historical = 0x01,
  continuous = 0x02
};

/// Concatenates two query options.
constexpr query_options operator+(const query_options& lhs,
                                  const query_options& rhs)
{
   return static_cast<query_options>(
       static_cast<uint32_t>(lhs) | static_cast<uint32_t>(rhs));
}

constexpr query_options no_query_options = query_options::none;
constexpr query_options historical = query_options::historical;
constexpr query_options continuous = query_options::continuous;
constexpr query_options unified = historical + continuous;

constexpr bool has_query_option(query_options haystack, query_options needle)
{
  return (static_cast<uint32_t>(haystack) & static_cast<uint32_t>(needle)) != 0;
}

constexpr bool has_historical_option(query_options opts)
{
  return has_query_option(opts, historical);
}

constexpr bool has_continuous_option(query_options opts)
{
  return has_query_option(opts, continuous);
}

constexpr bool has_unified_option(query_options opts)
{
  return has_query_option(opts, historical)
      && has_query_option(opts, continuous);
}

} // namespace vast

#endif
