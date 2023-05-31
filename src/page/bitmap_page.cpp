#include "page/bitmap_page.h"

template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_ == GetMaxSupportedSize()) //full
    return false;
  while(!IsPageFreeLow(next_free_page_ / 8, next_free_page_ % 8))
    next_free_page_++;
  page_offset = next_free_page_;
  bytes[next_free_page_ / 8] |= (1 << (next_free_page_ % 8));
  page_allocated_++;
  next_free_page_++;
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(IsPageFree(page_offset))
    return false;
  bytes[page_offset / 8] &= ~(1 << (page_offset % 8));
  page_allocated_--;
  next_free_page_ = std::min(next_free_page_, page_offset);
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return (bytes[byte_index] & (1u << bit_index)) == 0;
}

template
    class BitmapPage<64>;

template
    class BitmapPage<128>;

template
    class BitmapPage<256>;

template
    class BitmapPage<512>;

template
    class BitmapPage<1024>;

template
    class BitmapPage<2048>;

template
    class BitmapPage<4096>;