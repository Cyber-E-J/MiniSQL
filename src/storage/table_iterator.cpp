#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"



TableIterator:: TableIterator(TableHeap* table_heap,TablePage* page,RowId rid){
    this->table_heap_ = table_heap;
    this->rid_ = rid;
    this->row_=new Row(rid);
    if (rid_.GetPageId() != INVALID_PAGE_ID) {
      this->table_heap_->GetTuple(row_, nullptr);
    }
  }

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_=other.table_heap_;
  page_=other.page_;
  rid_=other.rid_;
  delete row_;
  this->row_=new Row(rid_);
}

const Row &TableIterator::operator*() { return *(this->row_); }

Row *TableIterator::operator->() {
  ASSERT(*this != table_heap_->End(),"itr is at end");
  return row_;
}

TableIterator &TableIterator::operator++() {
  RowId rid = rid_;
  auto page = reinterpret_cast<TablePage *>(this->table_heap_->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  ASSERT(page != nullptr,"Not found this page!");
  RowId next_rid;
  int flag = 0;//指示是不是末尾
  page->RLatch();
  bool is_get=page->GetNextTupleRid(rid,&next_rid);//获取下个rid
  if(!is_get||next_rid.Get()==INVALID_ROWID.Get() ){//如果没获取,可能是当前页最后一条记录
    auto next_page_id = page->GetNextPageId();
    page->RUnlatch();
    this->table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    while(next_page_id!=INVALID_PAGE_ID){//当下一页合法
      auto next_page = reinterpret_cast<TablePage *>(this->table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
      next_page->RLatch();
      if(next_page->GetFirstTupleRid(&next_rid)){//如果有第一个元组
        this->page_ = next_page;
        break;
      }
      next_page->RUnlatch();
      this->table_heap_->buffer_pool_manager_->UnpinPage(next_page->GetTablePageId(), false);
      next_page_id = next_page->GetNextPageId();
    }
    if(next_page_id==INVALID_PAGE_ID){
      flag = 1;
      rid_ = RowId(INVALID_PAGE_ID, 0);
      delete row_;
      row_ =new Row(rid_);
      page_ = nullptr;
    }
  }
  else{
    this->page_ = page;
  }
  if(!flag){
    delete row_;
    row_ =new Row(next_rid);
    this->rid_ = next_rid;
    page->GetTuple(this->row_, this->table_heap_->schema_, nullptr, this->table_heap_->lock_manager_);
    page_->RUnlatch();
    this->table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  }
  
  return *this;
  
}

TableIterator TableIterator::operator++(int) {
  TableHeap *oldheap = this->table_heap_;
  TablePage *oldpage = this->page_;
  RowId oldrid= this->rid_;
  RowId rid = rid_;
  auto page = reinterpret_cast<TablePage *>(this->table_heap_->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  ASSERT(page != nullptr,"Not found this page!");
  RowId next_rid;
  int flag = 0;//指示是不是末尾
  page->RLatch();
  bool is_get=page->GetNextTupleRid(rid,&next_rid);//获取下个rid
  if(!is_get||next_rid.Get()==INVALID_ROWID.Get() ){//如果没获取,可能是当前页最后一条记录
    auto next_page_id = page->GetNextPageId();
    page->RUnlatch();
    this->table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    while(next_page_id!=INVALID_PAGE_ID){//当下一页合法
      auto next_page = reinterpret_cast<TablePage *>(this->table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
      next_page->RLatch();
      if(next_page->GetFirstTupleRid(&next_rid)){//如果有第一个元组
        this->page_ = next_page;
        break;
      }
      next_page->RUnlatch();
      this->table_heap_->buffer_pool_manager_->UnpinPage(next_page->GetTablePageId(), false);
      next_page_id = next_page->GetNextPageId();
    }
    if(next_page_id==INVALID_PAGE_ID){
      flag = 1;
      rid_ = RowId(INVALID_PAGE_ID, 0);
      delete row_;
      row_ =new Row(rid_);
      page_ = nullptr;
    }
  }
  else{
    this->page_ = page;
  }
  if(!flag){
    delete row_;
    row_ =new Row(next_rid);
    this->rid_ = next_rid;
    page->GetTuple(this->row_, this->table_heap_->schema_, nullptr, this->table_heap_->lock_manager_);
    page_->RUnlatch();
    this->table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  }
  
  return TableIterator(oldheap,oldpage,oldrid);
}
