#ifndef VAST_CONCEPT_CONVERTIBLE_VAST_TIME_H
#define VAST_CONCEPT_CONVERTIBLE_VAST_TIME_H

#include <string>

#include "vast/time.h"

namespace vast {

class json;

bool convert(time::duration tr, double& d);
bool convert(time::duration tr, time::duration::duration_type& dur);
bool convert(time::duration tr, json& j);

bool convert(time::point tp, double& d);
bool convert(time::point tp, std::tm& tm);
bool convert(time::point tp, json& j);

// FIXME: conversion to string should be implemented via Printable.
bool convert(time::point tp, std::string& str,
             char const* fmt = time::point::format);

} // namespace vast

#endif
