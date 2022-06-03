#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator() {

}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_=other.table_heap_;
  current_page=other.current_page;
  row_id=other.row_id;
  current_row=new Row(row_id);
  //current_row=other.current_row;//涉及指针的拷贝构造
}

TableIterator::TableIterator (TableHeap* table_heap,TablePage* cur_page,RowId row_id_){
  table_heap_=table_heap;
  current_page=cur_page;
  row_id=row_id_;
  current_row=new Row(row_id);
}

TableIterator::~TableIterator() {
  delete current_row;
}

// bool TableIterator::operator==(const TableIterator &itr) const {
//   return(table_heap_==itr.table_heap_&&row_id==itr.row_id);
// }

// bool TableIterator::operator!=(const TableIterator &itr) const {
//   return !((*this)==itr);
// }

const Row &TableIterator::operator*() {
  // auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(row_id.GetPageId()));
  // //关于row相关的memheap的问题
  // Row current_row(row_id);
  // page->GetTuple(&current_row,table_heap_->schema_,nullptr,table_heap_->lock_manager_);
  // //同样也是这个row的问题，

  // //return nullptr;
  return *current_row;
  //ASSERT(false, "Not implemented yet.");
}

Row *TableIterator::operator->() {
  //在哪里new 一个row呢，在哪里释放啊？？？
  //需要new 一个空间吧，保存在一个vector中，在析构时释放，？？？
  //传入一个memheap,然后等memheap 析构
  return current_row;
}

TableIterator &TableIterator::operator++() {
  //在这一页有
  TablePage* page= reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(row_id.GetPageId()));
  RowId  next;
  if(page->GetNextTupleRid(row_id,&next)){
      delete current_row;
      current_page=page;
      table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
      current_row=new Row(next);
      row_id=next;
      return *this;
  }
  while(1){
    //std::cout<<"next_page_id="<<row_id.GetPageId()<<std::endl;
    page_id_t next_page=page->GetNextPageId();
    std::cout<<"next_page_id="<<next_page<<std::endl;
    if(next_page==INVALID_PAGE_ID){
      current_page=nullptr;
      row_id=INVALID_ROWID;
      std::cout<<"1 success"<<std::endl;
      delete current_row;
      std::cout<<"delete success"<<std::endl;
      break;
    }
    page=reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page));
    if(page->GetFirstTupleRid(&next)){
      delete current_row;
      current_page=page;
      table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
      current_row=new Row(next);
      row_id=next;
      break;
    }
  }

  return *this;
  //忘记unpin了
  //下一页找第一个元组
  //table_iterator 的用处只能用来遍历，不能用来修改
}

TableIterator TableIterator::operator++(int) {
  TablePage* old_page=current_page;
  RowId old_id=row_id;
  ++(*this);
  if(this->row_id==INVALID_ROWID)std::cout<<"2 success"<<std::endl;
  return TableIterator(this->table_heap_,old_page,old_id);
}
