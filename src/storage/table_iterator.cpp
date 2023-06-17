//#include "storage/table_iterator.h"
//
//#include "common/macros.h"
//#include "storage/table_heap.h"
//
//TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Transaction* txn)
//    :table_heap(table_heap),
//      row(new Row(rid)),
//      txn(txn) {
//  if (rid.GetPageId() != INVALID_PAGE_ID) {
//    this->table_heap->GetTuple(row, txn);
//  }
//}
//
//TableIterator::TableIterator(const TableIterator &other)
//    :table_heap(other.table_heap),
//      row(new Row(*other.row)),
//      txn(other.txn) {
//
//}
//
//TableIterator::~TableIterator() { delete row;
//}
//
//bool TableIterator::operator==(const TableIterator &itr) const {
//  return row->GetRowId().Get() == itr.row->GetRowId().Get();
//}
//
//bool TableIterator::operator!=(const TableIterator &itr) const {
//  return !(*this == itr);
//}
//
//TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
//  this->table_heap = itr.table_heap;
//  this->txn = itr.txn;
//  //this->row_id_ = itr.row_id_;
//  this->row = new Row(*itr.row);
////  row_->destroy();
////  if(row_id_.GetPageId()!=INVALID_PAGE_ID)
////    table_heap->GetTuple(row,txn);
//  return *this;
//}
//
//const Row &TableIterator::operator*() {
//  //  ASSERT(false, "Not implemented yet.");
//  ASSERT(*this != table_heap->End(), "TableHeap iterator out of range, invalid dereference.");
//  return *row;
//}
//
//Row *TableIterator::operator->() {
//  ASSERT(*this != table_heap->End(), "TableHeap iterator out of range, invalid dereference.");
//  return row;
//}
//TableIterator &TableIterator::operator++() {
//  BufferPoolManager *buffer_pool_manager = table_heap->buffer_pool_manager_;
//  auto cur_page = reinterpret_cast<TablePage *>(buffer_pool_manager->FetchPage(row->GetRowId().GetPageId()));
//  cur_page->RLatch();
//  assert(cur_page != nullptr);  // all pages are pinned
//
//  RowId next_tuple_rid;
//  if (!cur_page->GetNextTupleRid(row->GetRowId(),
//                                 &next_tuple_rid)) {  // end of this page
//    while (cur_page->GetNextPageId() != INVALID_PAGE_ID) {
//      auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager->FetchPage(cur_page->GetNextPageId()));
//      cur_page->RUnlatch();
//      buffer_pool_manager->UnpinPage(cur_page->GetTablePageId(), false);
//      cur_page = next_page;
//      cur_page->RLatch();
//      if (cur_page->GetFirstTupleRid(&next_tuple_rid)) {
//        break;
//      }
//    }
//  }
//  row = new Row(next_tuple_rid);
//
//  if (*this != table_heap->End()) {
//    table_heap->GetTuple(row ,nullptr);
//  }
//  // release until copy the tuple
//  cur_page->RUnlatch();
//  buffer_pool_manager->UnpinPage(cur_page->GetTablePageId(), false);
//  return *this;
//}
//
//TableIterator TableIterator::operator++(int) {  // postfix
//  TableIterator clone(*this);
//  ++(*this);
//  return clone;
//}
//
#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator() : table_heap_(nullptr) {
  row_id_.Set(INVALID_PAGE_ID, 0);
  row_ = new Row(row_id_);
}

TableIterator::TableIterator(TableHeap *table_heap, RowId row_id, Transaction *txn){
  txn_ = txn;
  table_heap_ = table_heap;
  row_id_ = row_id;
  if(row_id_.GetPageId()!=INVALID_PAGE_ID){
    row_ = new Row(row_id_);
    table_heap_->GetTuple(row_,txn_);
  }
}

TableIterator::TableIterator(const TableIterator &other) :
                                                           txn_(other.txn_), table_heap_(other.table_heap_), row_id_(other.row_id_), row_(new Row(other.row_id_)){
}

TableIterator::~TableIterator() {
  delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return (this->row_id_==itr.row_id_);
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this==itr);
}

const Row &TableIterator::operator*() {

  return *(this->row_);
}

Row *TableIterator::operator->() {

  return this->row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  this->table_heap_ = itr.table_heap_;
  this->txn_ = itr.txn_;
  this->row_id_ = itr.row_id_;
  this->row_ = new Row(*itr.row_);
  row_->destroy();
  if(row_id_.GetPageId()!=INVALID_PAGE_ID)
    table_heap_->GetTuple(row_,txn_);
}

// ++iter
TableIterator &TableIterator::operator++() {
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(row_id_.GetPageId()));
  RowId row_id;
  while(!page->GetNextTupleRid(row_id_,&row_id)){
    table_heap_->buffer_pool_manager_->UnpinPage(row_id_.GetPageId(),false);
    page_id_t next_page_id = page->GetNextPageId();
    if(next_page_id==INVALID_PAGE_ID){
      row_id_.Set(INVALID_PAGE_ID,0);
      return *this;
    }
    page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
    row_id_.Set(page->GetPageId(),-1);
  }
  row_id_ = row_id;
  row_->SetRowId(row_id_);
  row_->destroy();
  table_heap_->GetTuple(row_,txn_);
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator temp = TableIterator(*this);
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(row_id_.GetPageId()));
  RowId row_id;
  while(!page->GetNextTupleRid(row_id_,&row_id)){
    table_heap_->buffer_pool_manager_->UnpinPage(row_id_.GetPageId(),false);
    page_id_t next_page_id = page->GetNextPageId();
    if(next_page_id==INVALID_PAGE_ID){
      row_id_.Set(INVALID_PAGE_ID,0);
      return temp;
    }
    page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
    row_id_.Set(page->GetPageId(),-1);
  }
  row_id_ = row_id;
  row_->SetRowId(row_id_);
  row_->destroy();
  table_heap_->GetTuple(row_,txn_);

  return temp;
}
