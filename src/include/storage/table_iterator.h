#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator {
public:
  // you may define your own constructor based on your member variables
<<<<<<< HEAD
  explicit TableIterator(TableHeap *table_heap, RowId rid, Transaction *txn);//

  //explicit TableIterator(TableHeap *table_heap, RowId rid, Transaction *txn);//, Transaction *txn
=======

  explicit TableIterator(TableHeap *table_heap, RowId rid, Transaction *txn);
>>>>>>> c8776571441040e84be8bdd8f7202d90b56c03ae

  TableIterator(const TableIterator &other);//explicit

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const;

  inline bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  // add your own private member variables here
 TableHeap *table_heap;
 Row *row;
 Transaction *txn;  // not implemented, but need one
};

#endif  // MINISQL_TABLE_ITERATOR_H
