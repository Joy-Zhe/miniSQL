#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
//  ASSERT(false, "Not implemented yet.");
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
  return std::make_pair(leaf_page->KeyAt(item_index), leaf_page->ValueAt(item_index));
}

IndexIterator &IndexIterator::operator++() {
//  ASSERT(false, "Not implemented yet.");
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
  if (item_index + 1 < leaf_page->GetSize()) {
    item_index++;
  } else {
    current_page_id = leaf_page->GetNextPageId();
    item_index = 0;
    buffer_pool_manager->UnpinPage(leaf_page->GetPageId(), false);
    if (current_page_id != INVALID_PAGE_ID) {
      leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
    } else {
      leaf_page = nullptr;
    }
  }
  page = leaf_page;
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}
