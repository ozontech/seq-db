#include "assert.h"
#include "emmintrin.h"
#include "immintrin.h"
#include "stdint.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"

typedef struct {
  uint32_t start;
  uint16_t len;
} span;

int32_t asciionly(const char *data, size_t len);
int16_t boundaries(__m128i block, uint8_t lo, uint8_t hi);
int32_t tokenize(const char *text, size_t len, span *out, int out_cap);
