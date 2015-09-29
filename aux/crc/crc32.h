#ifndef CRC32_H
#define CRC32_H

#include <stdint.h>

#if defined (__cplusplus)
extern "C" {
#endif

void crc32(const void* key, int len, uint32_t seed, void* out);

#if defined (__cplusplus)
}
#endif

#endif
