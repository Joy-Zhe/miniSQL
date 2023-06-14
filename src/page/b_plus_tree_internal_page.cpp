#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetKeySize(key_size);
    SetMaxSize(max_size);
    SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int L = 1;
  int R = GetSize();
  int M = (L + R) / 2;

  while (L <= R) {
    M = (L + R) / 2;
    GenericKey *mKey = KeyAt(M);
    if(KM.CompareKeys(key, mKey) == 0) { //==
      return ValueAt(M); // found
    } else if(KM.CompareKeys(key, mKey) < 0) {// > key, continue finding in the left
      R = M - 1;
    } else {
      L = M + 1;
    }
  }
//  return ValueAt(L - 1); // not found, prepare for insertion
  return INVALID_PAGE_ID;
}


/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
//  SetPageType(IndexPageType::INTERNAL_PAGE);
//  SetPageId(GetPageId());
//  SetParentPageId(INVALID_PAGE_ID);
  SetSize(2);
//  SetMaxSize(GetMaxSize());
  SetKeyAt(1, new_key);
  SetValueAt(0, old_value);
  SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int insert_index = ValueIndex(old_value);
  PairCopy(PairPtrAt(insert_index + 2), PairPtrAt(insert_index + 1), GetSize() - insert_index - 1);
  SetKeyAt(insert_index + 1, new_key);
  SetValueAt(insert_index + 1, new_value);

  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  int start = GetMaxSize() / 2;
  int length = size - start;
  recipient->CopyNFrom(PairPtrAt(GetMinSize()), length, buffer_pool_manager);
  SetSize(GetMinSize());
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  PairCopy(PairPtrAt(GetSize() /** pair_size*/), src, size);
  for (int i = GetSize(); i < GetSize() + size; i++) {
    Page *child_page = buffer_pool_manager->FetchPage(ValueAt(i));
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  if(index < 0 || index >= GetSize())
    return ;
  PairCopy(PairPtrAt(index), PairPtrAt(index + 1), GetSize() - index - 1);
//  for(int i = index; i < GetSize() - 1; i++) {
//    SetKeyAt(i, KeyAt(i + 1));
//    SetValueAt(i, ValueAt(i + 1));
//  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  Remove(1);
  SetSize(0);
  return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
//  int size = recipient->GetSize();
//  for(int i = 0; i < GetSize(); i++) {
//    recipient->SetKeyAt(size + i, KeyAt(i));
//    recipient->SetValueAt(size + i, ValueAt(i));
//    page_id_t child_id = ValueAt(i);
//    InternalPage *child_page = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(child_id)->GetData());
//    child_page->SetParentPageId(recipient->GetPageId());
//    buffer_pool_manager->UnpinPage(child_id, true);
//  }
//  //set middle search key
//  recipient->SetKeyAt(size - 1, middle_key);
//  // update the size of recipient page
//  recipient->IncreaseSize(GetSize());
//  // update the parent page of recipient page
//  recipient->SetParentPageId(GetParentPageId());
//  // changed, mark dirty
//  buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
//  buffer_pool_manager->UnpinPage(GetPageId(), true);
  LOG(INFO) << "MoveAllTo";
  SetKeyAt(0, middle_key);
  recipient->CopyNFrom(PairPtrAt(0), GetSize(), buffer_pool_manager);
  buffer_pool_manager->UnpinPage(GetPageId(),true);
  buffer_pool_manager->UnpinPage(recipient->GetPageId(),true);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  LOG(INFO) << "MoveFirstToEndOf";
//  GenericKey *key = KeyAt(0);
//  page_id_t value = ValueAt(0);
//
//  Remove(0);
//  recipient->CopyLastFrom(key, value, buffer_pool_manager);
//  recipient->CopyLastFrom(middle_key, recipient->ValueAt(recipient->GetSize() - 2), buffer_pool_manager);
  SetKeyAt(0, middle_key);
  recipient->CopyLastFrom(KeyAt(0), ValueAt(0), buffer_pool_manager);
  Remove(0);
  Page *parent = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parent_page = reinterpret_cast<BPlusTreeInternalPage *>(parent->GetData());
  parent_page->SetKeyAt(parent_page->ValueIndex(GetPageId()), KeyAt(0));
  buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager->UnpinPage(GetPageId(), true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
//  int last_index = GetSize() - 1;
//  SetKeyAt(last_index, key);
//  SetValueAt(last_index, value);
//  //update the parent page id
//  if(buffer_pool_manager != nullptr) {
//    page_id_t page_id = ValueAt(last_index);
//    InternalPage *page = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(page_id));
//    if(page != nullptr) {
//      page->SetParentPageId(GetPageId());
//      buffer_pool_manager->UnpinPage(page_id, true);
//    }
//  }
  LOG(INFO) << "CopyLastFrom";
  SetKeyAt(GetSize(), key);
  SetValueAt(GetSize(), value);
  Page *page = buffer_pool_manager->FetchPage(ValueAt(GetSize()));
  BPlusTreeInternalPage *node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
//  int last_index = GetSize() - 1;
//  page_id_t value = ValueAt(last_index);
//  Remove(last_index);
//  recipient->CopyFirstFrom(value, buffer_pool_manager);
//  recipient->SetKeyAt(0, middle_key);
//  //update the parent id of the moved page
//  if (buffer_pool_manager != nullptr) {
//    InternalPage *page = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(value));
//    if(page != nullptr) {
//      page->SetParentPageId(recipient->GetPageId());
//      buffer_pool_manager->UnpinPage(value, true);
//    }
//  }
  LOG(INFO) << "MoveLastToFrontOf";
  Page * page = buffer_pool_manager->FetchPage(GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(page->GetData());
  parent->SetKeyAt(ValueIndex(recipient->GetPageId()), KeyAt(GetSize()-1));
  buffer_pool_manager->UnpinPage(GetParentPageId(),true);
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(ValueAt(GetSize() - 1), buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
//  int size = GetSize();
//  //spare room
//  for(int i = size; i > 0; i--) {
//    SetKeyAt(i, KeyAt(i - 1));
//    SetValueAt(i, ValueAt(i - 1));
//  }
//  SetKeyAt(0, nullptr);
//  SetValueAt(0, value);
//  if(buffer_pool_manager != nullptr) {
//    InternalPage *page = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(value));
//    if(page != nullptr) {
//      page->SetParentPageId(GetPageId());
//      buffer_pool_manager->UnpinPage(value, true);
//    }
//  }
//  IncreaseSize(1);
  LOG(INFO) << "CopyFirstFrom";
  PairCopy(PairPtrAt(1), PairPtrAt(0), GetSize()); // spare room
  SetValueAt(0, value); //no need to replace invalid key
  Page *page = buffer_pool_manager->FetchPage(ValueAt(0));
  BPlusTreeInternalPage *node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  Page *parent = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parent_page = reinterpret_cast<BPlusTreeInternalPage *>(parent->GetData());
  parent_page->SetKeyAt(parent_page->ValueIndex(GetPageId()), KeyAt(0));
  buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
  IncreaseSize(1);
}
