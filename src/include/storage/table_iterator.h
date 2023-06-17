//#ifndef MINISQL_TABLE_ITERATOR_H
//#define MINISQL_TABLE_ITERATOR_H
//
//#include "common/rowid.h"
//#include "record/row.h"
//#include "transaction/transaction.h"
//
//class TableHeap;
//
//class TableIterator {
//public:
//  // you may define your own constructor based on your member variables
//  TableIterator(TableHeap *table_heap, RowId rid, Transaction *txn);//explicit
//
//  TableIterator(const TableIterator &other);//explicit
//
//  virtual ~TableIterator();
//
//  inline bool operator==(const TableIterator &itr) const;
//
//  inline bool operator!=(const TableIterator &itr) const;
//
//  const Row &operator*();
//
//  Row *operator->();
//
//  TableIterator &operator=(const TableIterator &itr) noexcept;
//
//  TableIterator &operator++();
//
//  TableIterator operator++(int);
//
//private:
//  // add your own private member variables here
// TableHeap *table_heap;
// Row *row;
// Transaction *txn;  // not implemented, but need one
//};
//
//#endif  // MINISQL_TABLE_ITERATOR_H
#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator {
 public:
  // you may define your own constructor based on your member variables
  explicit TableIterator();

  explicit TableIterator(TableHeap *table_heap, RowId row_id, Transaction *txn);

  TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int);

 private:
  // add your own private member variables here
  Transaction *txn_;
  [[maybe_unused]] TableHeap *table_heap_;
  RowId row_id_;
  Row * row_{nullptr};
};

#endif  // MINISQL_TABLE_ITERATOR_H
