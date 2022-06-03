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
  //自己定义的constructor:
  TableIterator(TableHeap* table_heap,TablePage* cur_page,RowId row_id_);
  explicit TableIterator();

  explicit TableIterator(const TableIterator &other); 

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const{
    return(table_heap_==itr.table_heap_&&row_id==itr.row_id);
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
  // 所指向元组所在页的id和在该页的序号(RowID)，确保可以得到该row
  // 可以利用table_page里的函数getfirsttuple和getnexttuple

  //指向tuple所在的位置，然后调用反序列化？,还是这个row变量的空间何时定义的问题
  TableHeap* table_heap_; //
  TablePage* current_page;
  RowId row_id;//构造时初始化为invalid
  Row* current_row; 

};

#endif //MINISQL_TABLE_ITERATOR_H
