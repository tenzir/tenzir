#include "vast/api/vast_api.h"

#include <stdio.h>

int main() {
  struct vast_info vm;
  vast_info(&vm);
  printf("version: %s\n", vm.version);
  const char* endpoint = "localhost:42000";
  struct VAST* vast = vast_open(endpoint);
  if (!vast) {
    fprintf(stderr, "failed to open connection to VAST at %s\n", endpoint);
    return 1;
  }
  char buf[1024];
  vast_status_json(vast, buf, 1024);
  printf("status: %s\n", buf);
  return 0;
}