#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetKeySize(key_size);
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  int l = 0, r = GetSize() - 1;
  int m = (l + r) / 2;
  int result = -1;
  while (l <= r) {
    m = (l + r) / 2;
    GenericKey *keyNow = KeyAt(m);
    if(KM.CompareKeys(keyNow, key) >= 0) {
//      result = m;
      r = m - 1;
    } else {
      l = m + 1;
    }
  }
//  return result;
  return (r + 1);
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
    // replace with your own code
    return make_pair(KeyAt(index), ValueAt(index));
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
//  if (index >= 0 && KM.CompareKeys(key, KeyAt(index)) == 0) {
//    return GetSize();
//  }
//  if(GetSize() >= GetMaxSize()) {
//    return GetSize();
//  }
  for(int i = GetSize(); i > index; i--) {
    PairCopy(PairPtrAt(i), PairPtrAt(i - 1));
  }
  SetKeyAt(index, key);
  SetValueAt(index, value);
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int size = GetSize();
  int half = GetMaxSize() / 2;
  recipient->CopyNFrom(PairPtrAt(half), size - half);
  recipient->SetSize(size - half); // right half
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
  SetSize(half); // split
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  PairCopy(data_, src, size);
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int key_index = KeyIndex(key, KM);
  if(key_index < GetSize() && KM.CompareKeys(key, KeyAt(key_index)) == 0) {
    value = ValueAt(key_index);
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int key_index = KeyIndex(key, KM);
  if(key_index < GetSize() && KM.CompareKeys(key, KeyAt(key_index)) == 0) {
    int num_pairs = GetSize() - key_index - 1;
    PairCopy(PairPtrAt(key_index), PairPtrAt(key_index - 1), num_pairs);
    IncreaseSize(-1);
    return GetSize();
  }
  return -1;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  int num_pairs = GetSize();
  recipient->PairCopy(recipient->PairPtrAt(0), PairPtrAt(0), num_pairs);
  recipient->SetSize(recipient->GetSize() + num_pairs);
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  GenericKey *key = KeyAt(0);
  RowId value = ValueAt(0);
  recipient->CopyLastFrom(key, value);
  int num_pairs = GetSize() - 1;
  PairCopy(PairPtrAt(0), PairPtrAt(1), num_pairs);
  SetSize(GetSize() - 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  int index = GetSize();
  SetKeyAt(index, key);
  SetValueAt(index, value);
  SetSize(GetSize() + 1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  int index = GetSize() - 1;
  GenericKey *key = KeyAt(index);
  RowId value = ValueAt(index);
  recipient->CopyFirstFrom(key, value);
  SetSize(GetSize() - 1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  int num_pairs = GetSize();
  PairCopy(PairPtrAt(1), PairPtrAt(0), num_pairs);
  // insert new key value page
  SetKeyAt(0, key);
  SetValueAt(0, value);
  SetSize(GetSize() + 1);
}
