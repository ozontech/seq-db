#include "assert.h"
#include "emmintrin.h"
#include "immintrin.h"
#include "stdint.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"

#include "tokenize.h"

static inline uint16_t eq_mask(__m128i block, char c) {
  return _mm_movemask_epi8(_mm_cmpeq_epi8(block, _mm_set1_epi8(c)));
}

static inline int is_token_char(char c) {
  return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') ||
         ('0' <= c && c <= '9') || c == '_' || c == '*';
}

int32_t asciionly(const char *text, size_t len) {
  char high = 0x80;
  __m128i mask = _mm_set1_epi8(high);

  size_t i;
  int32_t result = 1;
  for (i = 0; i + 16 < len; i += 16) {
    __m128i input = _mm_lddqu_si128((__m128i_u *)(text + i));
    __m128i masked = _mm_and_si128(input, mask);

    result &= (_mm_movemask_epi8(masked) == 0);
    if (!result)
      return 0;
  }

  for (; i < len; i++)
    result &= ((text[i] & high) == 0);

  return result;
}

int16_t boundaries(__m128i block, uint8_t lo, uint8_t hi) {
  __m128i block_lo = _mm_set1_epi8(lo);
  __m128i block_zero = _mm_set1_epi8((char)0x0);
  __m128i block_range = _mm_set1_epi8(hi - lo);

  __m128i_u res = _mm_sub_epi8(block, block_lo);
  res = _mm_subs_epu8(res, block_range);

  return _mm_movemask_epi8(_mm_cmpeq_epi8(res, block_zero));
}

int32_t tokenize(const char *text, size_t len, span *out, int out_cap) {
  if (!asciionly(text, len))
    return -1;

  int count = 0;
  int token_start = -1;

  size_t i;
  for (i = 0; i + 16 <= len; i += 16) {
    __m128i block = _mm_lddqu_si128((__m128i_u *)(text + i));

    // I need to check PSHUFB approach.
    // Seems like there is so much overhead in here.
    uint16_t bitmap_token =
        boundaries(block, 'a', 'z') | boundaries(block, 'A', 'Z') |
        boundaries(block, '0', '9') | eq_mask(block, '_') | eq_mask(block, '*');
    uint16_t bitmap_delimeters = ~bitmap_token;

    if (token_start == -1) {
      // Whole block of 16 bytes contains no text symbols.
      if (bitmap_token == 0)
        continue;
      token_start = i + __builtin_ctz(bitmap_token);
    }

    // Whole block of 16 bytes contains text symbols.
    if (bitmap_delimeters == 0)
      continue;

    while (bitmap_delimeters && count < out_cap) {
      int pos = i + __builtin_ctz(bitmap_delimeters);

      if (token_start != -1 && pos > token_start) {
        out[count++] = (span){
            .start = token_start,
            .len = pos - token_start,
        };
        token_start = -1;
      }

      bitmap_delimeters &= bitmap_delimeters - 1;
      if (token_start == -1 && bitmap_token) {
        int bit = pos - i;
        uint16_t remaining = bitmap_token & ~((1 << (bit + 1)) - 1);
        if (remaining)
          token_start = i + __builtin_ctz(remaining);
      }
    }
  }

  for (size_t j = i; j < len; j++) {
    if (is_token_char(text[j])) {
      if (token_start == -1)
        token_start = j;
      continue;
    }

    if (token_start != -1 && count < out_cap) {
      out[count++] = (span){
          .start = token_start,
          .len = j - token_start,
      };
      token_start = -1;
    }
  }

  if (token_start != -1 && count < out_cap) {
    out[count++] = (span){
        .start = token_start,
        .len = len - token_start,
    };
  }

  return count;
}
