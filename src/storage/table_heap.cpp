//#include "storage/table_heap.h"
//#include <iostream>
///**
// * TODO: Student Implement
// */
//bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
//      if(row.GetSerializedSize(schema_) > TablePage::SIZE_MAX_ROW)
//        return false;
//
//      auto cur_page = (TablePage *)buffer_pool_manager_->FetchPage(first_page_id_);
//      if (cur_page == nullptr) {
//        return false;
//      }
//      TablePage *new_page;
//      page_id_t next_page_id_;
//
//      while (!cur_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
//        next_page_id_ = cur_page->GetNextPageId();
//        if (next_page_id_ != INVALID_PAGE_ID) {  // next page valid
//          buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), false);
//          cur_page = (TablePage *)buffer_pool_manager_->FetchPage(next_page_id_);  // get next
//        }
//        // no valid, create new one
//        else {
//          new_page = (TablePage *)buffer_pool_manager_->NewPage(next_page_id_);
//          if (new_page == nullptr) {//new page fails
//            buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), false);
//            return false;
//          } else {
//            new_page->Init(next_page_id_, cur_page->GetTablePageId(), log_manager_, txn);
//            cur_page->SetNextPageId(next_page_id_);  // update next page id
//            cur_page = new_page;
//            buffer_pool_manager_->UnpinPage(new_page->GetTablePageId(), true);
//            cur_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);return true;
//          }
//        }
//      }
//
//      // dirty
//      buffer_pool_manager_->UnpinPage(cur_page->GetTablePageId(), true);
//      return true;
//}
//
//bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
//  // Find the page which contains the tuple.
//  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
//  // If the page could not be found, then abort the transaction.
//  if (page == nullptr) {
//    return false;
//  }
//  // Otherwise, mark the tuple as deleted.
//  page->WLatch();
//  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
//  page->WUnlatch();
//  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
//  return true;
//}
//
///**
// * TODO: Student Implement
// */
//bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
//  auto page = (TablePage *)buffer_pool_manager_->FetchPage(rid.GetPageId());
//
//  if (page == nullptr) return false;
//
//  Row old_row_(row);
//  TablePage::RetState ret_state = page->UpdateTuple(row, &old_row_, schema_, txn, lock_manager_, log_manager_);
//
//  switch (ret_state) {
//    case TablePage::RetState::ILLEGAL_CALL:
//          buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
//          return false;
//    case TablePage::RetState::INSUFFICIENT_TABLE_PAGE:
//          // do deletion on old row
//          MarkDelete(rid, txn);
//          // do insertion
//          InsertTuple(old_row_, txn);
//          buffer_pool_manager_->UnpinPage(old_row_.GetRowId().GetPageId(), true);
//          return true;
//    case TablePage::RetState::DOUBLE_DELETE:
//          buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
//          return false;
//    case TablePage::RetState::SUCCESS:
//          buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
//          return true;
//    default:
//          return false;
//  }
//}
//
///**
// * TODO: Student Implement
// */
//void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
//  // Step1: Find the page which contains the tuple.
//  // Step2: Delete the tuple from the page.
//  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
//  ASSERT(page != nullptr, "Can not find a page containing the RID.");
//  page->ApplyDelete(rid, txn, log_manager_);
//  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
//}
//
//void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
//  // Find the page which contains the tuple.
//  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
//  assert(page != nullptr);
//  // Rollback to delete.
//  page->WLatch();
//  page->RollbackDelete(rid, txn, log_manager_);
//  page->WUnlatch();
//  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
//}
//
//bool TableHeap::GetTuple(Row *row, Transaction *txn) {
//  auto page_found = (TablePage *)buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId());
//  if (page_found == nullptr) {
//    return false;
//  }
//
//  auto ret = page_found->GetTuple(row, schema_, txn, lock_manager_);
//  buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);
//  return ret;
//}
//
//void TableHeap::DeleteTable(page_id_t page_id) {
//  if (page_id != INVALID_PAGE_ID) {
//    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
//    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
//      DeleteTable(temp_table_page->GetNextPageId());
//    buffer_pool_manager_->UnpinPage(page_id, false);
//    buffer_pool_manager_->DeletePage(page_id);
//  } else {
//    DeleteTable(first_page_id_);
//  }
//}
//
///**
// * TODO: Student Implement
// */
//TableIterator TableHeap::Begin(Transaction *txn) {//
//  RowId begin_row_id = INVALID_ROWID;
//  page_id_t page_id = first_page_id_;
//  TablePage *page;
//  while (page_id != INVALID_PAGE_ID) {
//    page = (TablePage *)buffer_pool_manager_->FetchPage(page_id);
//    // found
//    if (page->GetFirstTupleRid(&begin_row_id)) {
//      break;
//    }
//    page_id = page->GetNextPageId();
//  }
//  // would return INVALID_ROWID if there is no page
//  return TableIterator(this, begin_row_id, txn);
//}
//
///**
// * TODO: Student Implement
// */
//TableIterator TableHeap::End() {
//  return TableIterator(this, RowId(INVALID_PAGE_ID, 0), nullptr);//
//}
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {

  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));

  while(!page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){

    page_id_t next_page_id = page->GetNextPageId();
    if(next_page_id==INVALID_PAGE_ID){
      auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      if(next_page_id==INVALID_PAGE_ID) return false;
      next_page->Init(next_page_id,page->GetPageId(),log_manager_,txn);
      page->SetNextPageId(next_page->GetPageId());
      buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
    }
    else
      buffer_pool_manager_->UnpinPage(page->GetPageId(),page->IsDirty());
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  Row *old_row = new Row(rid);
  buffer_pool_manager_->UnpinPage(page->GetPageId(),page->IsDirty());
  bool temp_res = page->UpdateTuple(row,old_row,schema_,txn,lock_manager_,log_manager_);
  if(temp_res==true) return true;
  else if(temp_res==false){
    Row new_row(row);
    page->MarkDelete(rid,txn,lock_manager_,log_manager_);
    return InsertTuple(new_row,txn);
  }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  page->ApplyDelete(rid,txn,log_manager_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
  return;
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(),true);
  return page->GetTuple(row,schema_,txn,lock_manager_);
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId row_id;
  while(!page->GetFirstTupleRid(&row_id)){
    buffer_pool_manager_->UnpinPage(page->GetPageId(),page->IsDirty());
    page_id_t next_page_id = page->GetNextPageId();
    if(next_page_id==INVALID_PAGE_ID) return TableHeap::End();
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(),page->IsDirty());
  return TableIterator(this,row_id,txn);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  static RowId row_id;
  row_id.Set(INVALID_PAGE_ID,0);
  static TableIterator end(this,row_id,nullptr);
  return end;
}
