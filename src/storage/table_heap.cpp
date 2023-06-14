#include "storage/table_heap.h"
#include <iostream>
/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
    if(row.GetSerializedSize(schema_)>=PAGE_SIZE ||
      row.GetSerializedSize(schema_) > TablePage::SIZE_MAX_ROW)
      return false;
    page_id_t pid = first_page_id_;
    bool flag = false;
    TablePage * page = nullptr;
    //page_id_t prev_pid;//jy added for debug
    for(;pid != INVALID_PAGE_ID;){
      //std::cout<<pid<<std::endl;
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(pid));//fetchpage atomatically pin
      if(page == nullptr) break;//jy added
      page->WLatch();
      flag=page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
      page->WUnlatch();
      if(flag) {
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(),true);//GetPageId-->GetTablePageId added
        return true;
      }
      pid=page->GetNextPageId();
      if(pid == INVALID_PAGE_ID) break;
      //buffer_pool_manager_->UnpinPage(page->GetPageId(),true);// added
      delete page;page = nullptr;
    }
    //buffer_pool_manager_->UnpinPage(page->GetPageId(),false);// added
    buffer_pool_manager_->NewPage(pid);
    if(pid == INVALID_PAGE_ID) return false;//new page fails
    //new page and insert tuple
    auto Npage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(pid));
    if(Npage == nullptr) return false;// added
    Npage->WLatch();
    if(page!= nullptr) page->WLatch();// comment
    //initialize Npage
    if(page!= nullptr)
      Npage->Init(pid,page->GetTablePageId(),log_manager_,txn);//PageId --> tablePageId
    else
      Npage->Init(pid,INVALID_PAGE_ID,log_manager_,txn);
    //end initialize N page
    Npage->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
    if(page!= nullptr)  page->SetNextPageId(pid);
    Npage->WUnlatch();
    if(page!= nullptr) page->WUnlatch();// comment
    buffer_pool_manager_->UnpinPage(pid, true);//Npage->GetPageId() --> pid     added
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
    if (page == nullptr || row.GetSerializedSize(schema_)>=PAGE_SIZE) return false;
    Row *p = new Row;
    bool flag;
    p->SetRowId(rid);
    page->WLatch();page->RLatch();
    page->GetTuple(p,schema_,txn,lock_manager_);
    flag=page->UpdateTuple(row,p,schema_,txn,lock_manager_,log_manager_);
    page->WUnlatch();page->RUnlatch();
    delete p;
    return flag;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) return;
  page->WLatch();page->RLatch();
  page->ApplyDelete(rid,txn,log_manager_);
  page->WUnlatch();page->RUnlatch();
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

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr) return false;
  bool flag;
  page->RLatch();
  flag=page->GetTuple(row,schema_,txn,lock_manager_);
  page->RUnlatch();
  return flag;
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
TableIterator TableHeap::Begin(Transaction *txn) {//
  RowId begin_row_id = INVALID_ROWID;
  page_id_t page_id = first_page_id_;
  TablePage *page;
  while (page_id != INVALID_PAGE_ID) {
    page = (TablePage *)buffer_pool_manager_->FetchPage(page_id);
    // found
    if (page->GetFirstTupleRid(&begin_row_id)) {
      break;
    }
    page_id = page->GetNextPageId();
  }
  // would return INVALID_ROWID if there is no page
  return TableIterator(this, begin_row_id, txn);//
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  return TableIterator(this, RowId(INVALID_PAGE_ID, 0), nullptr);//
}
