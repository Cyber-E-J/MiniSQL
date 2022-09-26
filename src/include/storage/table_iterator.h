#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"
#include "page/table_page.h"

class TableHeap;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
  TableIterator(TableHeap* table_heap,TablePage* page,RowId rid);
  explicit TableIterator();

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator() {
    row_->SetRowId(INVALID_ROWID);
    delete row_;
  }

  inline bool operator==(const TableIterator &itr) const{
    return(table_heap_==itr.table_heap_&&rid_==itr.rid_);
  }

  inline bool operator!=(const TableIterator &itr) const{
    return !((*this)==itr);
  }

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  // add your own private member variables here

TableHeap* table_heap_;
 Row *row_;
 RowId rid_;
 TablePage *page_;
};

#endif //MINISQL_TABLE_ITERATOR_H
