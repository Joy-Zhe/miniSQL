#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(current_page_id);
  if (page == nullptr) {
    return;
  }

  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage*>(page->GetData());

  if (tree_page->IsLeafPage()) {
    buffer_pool_manager_->DeletePage(current_page_id);
    return;
  }

  BPlusTreeInternalPage *internal_page = reinterpret_cast<BPlusTreeInternalPage*>(tree_page);
  for (int i = 0; i < internal_page->GetSize(); i++) {
    Destroy(internal_page->ValueAt(i));
  }

  buffer_pool_manager_->DeletePage(current_page_id);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return (root_page_id_ == INVALID_PAGE_ID);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
  LOG(INFO) << "GetValue called.";
  Page *leaf = FindLeafPage(key, root_page_id_, true); //false?
  if(leaf == nullptr)
    return false;
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(leaf);
  int index = leaf_page->KeyIndex(key, processor_); //search for the key
  if(index == -1) {//not found
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return false;
  }
  result.push_back(leaf_page->ValueAt(index));
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
  LOG(INFO) << "Insert called.";
  if (IsEmpty()) { // empty, create a new tree
    StartNewTree(key, value); // update root_page_id in StartNewTree
    return true;
  } else {
    Page *leaf_page = FindLeafPage(key, root_page_id_, false);
    if(leaf_page == nullptr)
      return false;
    BPlusTreeLeafPage *leaf = reinterpret_cast<BPlusTreeLeafPage*>(leaf_page->GetData());
    if (leaf->GetSize() < leaf->GetMaxSize()) {
      bool success = InsertIntoLeaf(key, value, transaction);
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), success);
      return success;
    } else {
      BPlusTreeLeafPage *new_leaf = Split(leaf, transaction);
      GenericKey *new_key = new_leaf->KeyAt(0);

      // Insert the new key and new leaf into the parent page
      Page *parent_page = FindLeafPage(new_key, root_page_id_, false);
      BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
      InsertIntoParent(parent, new_key, new_leaf, transaction);
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
      return true;
    }
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  LOG(INFO) << "StartNewTree called.";
  page_id_t new_page_id;
  Page *page = buffer_pool_manager_->NewPage(new_page_id);
  if(page == nullptr) {
    throw runtime_error("out of memory");
  }
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(page->GetData());
  leaf_page->Init(page->GetPageId());
  leaf_page->Insert(key, value, processor_);
  root_page_id_ = page->GetPageId();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true); //unpin, dirty
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
  LOG(INFO) << "InsertInfoLeaf called.";
  Page *page = FindLeafPage(key, root_page_id_, true);
  if (page == nullptr) {
    return false;
  }
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(page->GetData());
  int index = leaf_page->KeyIndex(key, processor_);
  if (index != -1) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }
  leaf_page->Insert(key, value, processor_);

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) {
  LOG(INFO) << "Split_internal called.";
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) {
    throw std::runtime_error("Out of memory");
  }
  BPlusTreeInternalPage *new_internal_page = reinterpret_cast<BPlusTreeInternalPage*>(new_page->GetData());
  // Move half of the key-childId pairs from the original internal page to the new internal page
  node->MoveHalfTo(new_internal_page, buffer_pool_manager_);
  // Update parent-child relationships
  new_internal_page->SetParentPageId(node->GetParentPageId());
  new_internal_page->SetSize(node->GetSize());
  // Unpin the pages
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  return new_internal_page;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
  LOG(INFO) << "Split_leaf called.";
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);

  if(new_page == nullptr) {
    throw runtime_error("out of memory");
  }
  BPlusTreeLeafPage *new_leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(new_page->GetData());
  new_leaf_page->Init(new_page->GetPageId());
  node->MoveHalfTo(new_leaf_page);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  return new_leaf_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
  LOG(INFO) << "InsertIntoParent called.";
  if(old_node->IsRootPage()) {
    page_id_t new_page_id;
    Page *new_root_page = buffer_pool_manager_->NewPage(new_page_id);
    if(new_root_page == nullptr) {
      throw runtime_error("out of memory!");
    }
    BPlusTreeInternalPage *new_root = reinterpret_cast<BPlusTreeInternalPage*>(new_root_page->GetData());
    new_root->Init(new_root_page->GetPageId());
//    root_page_id_ = new_root_page->GetPageId();
    UpdateRootPageId(new_root_page->GetPageId());
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId()); //insert the key and pointer to the new root
    //update the parent id of old node and new node
    old_node->SetParentPageId(new_root_page->GetPageId());
    new_node->SetParentPageId(new_root_page->GetPageId());
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
  } else {
    Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    if(parent_page == nullptr) {
      throw runtime_error("Fail to fetch parent page");
    }
    BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(parent_page->GetPageId());
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

    if(parent->GetSize() > parent->GetMaxSize()){
      BPlusTreeInternalPage *new_internal_page = Split(parent, transaction);
      InsertIntoParent(parent, new_internal_page->KeyAt(0), new_internal_page, transaction);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
  LOG(INFO) << "Remove called.";
  if(IsEmpty()) {
    return;
  }
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, root_page_id_, false));
  if(leaf_page == nullptr) {
    return; //not found
  }
  bool if_success = leaf_page->RemoveAndDeleteRecord(key, processor_);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), if_success);

  CoalesceOrRedistribute(leaf_page, transaction);
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
  LOG(INFO) << "CoalesceOrRedistribute called.";
  if (node->IsRootPage()) {
    return false;
  }

  Page *current_page = buffer_pool_manager_->FetchPage(node->GetPageId());
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());

  InternalPage *parent = nullptr;
  int index = -1;

  // Find the parent node and index of the current node in the parent
  if (current_page != nullptr && parent_page != nullptr) {
    parent = reinterpret_cast<InternalPage*>(parent_page->GetData());
    for (int i = 0; i < parent->GetSize(); i++) {
      if (parent->ValueAt(i) == node->GetPageId()) {
        index = i;
        break;
      }
    }
  }

  // Find the sibling node
  N *sibling = nullptr;
  if (parent != nullptr && index != -1) {
    if (index == 0) {
      // Left sibling
      sibling = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(parent->ValueAt(1)));
    } else {
      // Right sibling
      sibling = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1)));
    }
  }

  // Perform redistribution or coalescing
  bool success = false;
  if (sibling != nullptr) {
    if (sibling->GetSize() + node->GetSize() > node->GetMaxSize()) {
      // Redistribute
      Redistribute(sibling, node, index);
      success = true;
    } else {
      // Coalesce
      if (std::is_same<N, InternalPage>::value) {
        success = Coalesce(sibling, node, parent, index + 1, transaction);
      } else if (std::is_same<N, LeafPage>::value) {
        success = Coalesce(sibling, node, parent, index, transaction);
      }
    }
  }

  // Unpin sibling and parent pages
  buffer_pool_manager_->UnpinPage(sibling->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);

  return success;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  LOG(INFO) << "Coalesce_leaf called.";
  if(neighbor_node->GetSize() + node->GetSize() > node->GetMaxSize()) {
    return false;
  }
  //move all key-value pair from node to neighbour node
  node->MoveAllTo(neighbor_node);
  parent->Remove(index + 1);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  return true;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  LOG(INFO) << "Coalesce_internal called.";
  if(neighbor_node->GetSize() + node->GetSize() > node->GetMaxSize()) {
    return false;
  }
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
  parent->Remove(index + 1);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  return true;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  LOG(INFO) << "Redistribute_leaf called.";
  if(index == 0) {
    node->MoveFirstToEndOf(neighbor_node);
  } else {
    node->MoveLastToFrontOf(neighbor_node);
  }
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  LOG(INFO) << "Redistribute_internal called.";
  if (index == 0) {
    // Move the first key-value pair from neighbor_node to the end of node
    node->MoveFirstToEndOf(neighbor_node, node->KeyAt(0), buffer_pool_manager_);
  } else {
    // Move the last key-value pair from neighbor_node to the head of node
    node->MoveLastToFrontOf(neighbor_node, neighbor_node->KeyAt(neighbor_node->GetSize() - 2), buffer_pool_manager_);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  LOG(INFO) << "AdjustRoot called.";
  if (old_root_node->IsLeafPage()) {
    // The old root is a leaf page with one last child
    // No action is needed, return false
    return false;
  }

  InternalPage *root_node = reinterpret_cast<InternalPage *>(old_root_node);

  if (root_node->GetSize() == 0) {
    // Case 2: The root node is empty, indicating the whole B+ tree is empty
    // Set the root page ID to INVALID_PAGE_ID
    root_page_id_ = INVALID_PAGE_ID;
    // Delete the old root page
    buffer_pool_manager_->DeletePage(root_node->GetPageId());
    // Update the root page ID in the header page
    UpdateRootPageId();
    // Return true to indicate that the root page should be deleted
    return true;
  }

  // No action is needed, return false
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  LOG(INFO) << "Begin called.";
  Page *leaf_page = FindLeafPage(nullptr, root_page_id_, true);
  if(leaf_page == nullptr) {
    return End();
  }
  BPlusTreeLeafPage *leaf = reinterpret_cast<BPlusTreeLeafPage *>(leaf_page->GetData());
  return IndexIterator(leaf->GetPageId(), buffer_pool_manager_, 0);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  LOG(INFO) << "Begin_internal called.";
  Page *leaf_page = FindLeafPage(key, root_page_id_, false);
  if(leaf_page == nullptr) {
    return End();
  }
  BPlusTreeLeafPage *leaf = reinterpret_cast<BPlusTreeLeafPage *>(leaf_page->GetData());
  int index = leaf->KeyIndex(key, processor_);
  return IndexIterator(leaf->GetPageId(), buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  LOG(INFO) << "End called.";
  return IndexIterator();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  LOG(INFO) << "FindLeafPage called.";
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if(page == nullptr)
    return nullptr;
  BPlusTreeInternalPage *internal_page = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
  if(internal_page->IsLeafPage()) //is leaf, found, return
    return page;
  int index = 0;
//  page_id_t child_id;
  if(leftMost) {
    index = 1;
  }
  while(index <= internal_page->GetSize() && processor_.CompareKeys(key, internal_page->KeyAt(index)) >= 0) {
    index++;
  }
  page_id_t child_id = internal_page->ValueAt(index - 1);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return FindLeafPage(key, child_id, leftMost);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  LOG(INFO) << "UpdateRootPageId called.";
  Page *index_roots_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage *roots_page = reinterpret_cast<IndexRootsPage *>(index_roots_page->GetData());

  if (insert_record) {
    // Insert a new record into the index roots page
    roots_page->Insert(index_id_, root_page_id_);
  } else {
    // Update the root page ID directly
    roots_page->Update(index_id_, root_page_id_);
  }

  // Mark the index roots page as dirty
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  buffer_pool_manager_->FlushPage(INDEX_ROOTS_PAGE_ID);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}
