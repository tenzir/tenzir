#pragma once

struct vast_metrics {
  const char* version;
};

struct VAST;

// E.g.: `vast_open("localhost:42000")`
extern "C" VAST* vast_open(const char* endpoint);

// Return 0 on success
extern "C" int vast_metrics(VAST*, struct vast_metrics* out);

// Closes the connection
extern "C" void vast_close(VAST*);