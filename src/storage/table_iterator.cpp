#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"



TableIterator:: TableIterator(TableHeap* table_heap,TablePage* page,RowId rid){
    this->table_heap_ = table_heap;
    this->rid_ = rid;
    this->row_=new Row(rid_);
    if (rid_.GetPageId() != INVALID_PAGE_ID) {
      this->table_heap_->GetTuple(row_, nullptr);
    }
  };

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_=other.table_heap_;
  page_=other.page_;
  rid_=other.rid_;
  row_=new Row(rid_);
}

const Row &TableIterator::operator*() { return *(this->row_); }

Row *TableIterator::operator->() {
  return nullptr;
}

TableIterator &TableIterator::operator++() {
  RowId rid = rid_;
  auto page = reinterpret_cast<TablePage *>(this->table_heap_->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  ASSERT(page != nullptr,"Not found this page!");
  RowId next_rid;
  page->RLatch();
  bool is_get=page->GetNextTupleRid(rid,&next_rid);
  if (next_rid.GetPageId() != INVALID_PAGE_ID && is_get) {
    this->row_->SetRowId(next_rid);
    if(*this!=this->table_heap_->End()){
      page->GetTuple(this->row_, this->table_heap_->schema_, nullptr, this->table_heap_->lock_manager_);
    }
    page->RUnlatch();
    this->page_ = page;
    this->table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  } else {
    auto next_page_id = page->GetNextPageId();
    page->RUnlatch();
    this->table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    while(next_page_id!=INVALID_PAGE_ID){
      auto next_page = reinterpret_cast<TablePage *>(this->table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
      next_page->RLatch();
      if(next_page->GetFirstTupleRid(&next_rid)){
        this->row_->SetRowId(next_rid);
        if(*this!=this->table_heap_->End()){
          next_page->GetTuple(this->row_, this->table_heap_->schema_, nullptr, this->table_heap_->lock_manager_);
        }
        next_page->RUnlatch();
        this->table_heap_->buffer_pool_manager_->UnpinPage(next_page->GetTablePageId(), false);
        this->page_ = next_page;
        break;
      }
      next_page_id=next_page->GetNextPageId();
      next_page->RUnlatch();
      this->table_heap_->buffer_pool_manager_->UnpinPage(next_page->GetTablePageId(), false);
    }
  }

  return *this;
}

TableIterator TableIterator::operator++(int) {
  RowId rid = rid_;
  auto page = reinterpret_cast<TablePage *>(this->table_heap_->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  ASSERT(page != nullptr,"Not found this page!");
  TablePage* cur_page = page;
  RowId next_rid;
  page->RLatch();
  bool is_get=page->GetNextTupleRid(rid,&next_rid);
  if (next_rid.GetPageId() != INVALID_PAGE_ID && is_get) {
    this->row_->SetRowId(next_rid);
    if(*this!=this->table_heap_->End()){
      page->GetTuple(this->row_, this->table_heap_->schema_, nullptr, this->table_heap_->lock_manager_);
    }
    page->RUnlatch();
    this->page_ = page;
    this->table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  } else {
    auto next_page_id = page->GetNextPageId();
    page->RUnlatch();
    this->table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    while(next_page_id!=INVALID_PAGE_ID){
      auto next_page = reinterpret_cast<TablePage *>(this->table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
      next_page->RLatch();
      if(next_page->GetFirstTupleRid(&next_rid)){
        this->row_->SetRowId(next_rid);
        if(*this!=this->table_heap_->End()){
          next_page->GetTuple(this->row_, this->table_heap_->schema_, nullptr, this->table_heap_->lock_manager_);
        }
        next_page->RUnlatch();
        this->table_heap_->buffer_pool_manager_->UnpinPage(next_page->GetTablePageId(), false);
        this->page_ = next_page;
        break;
      }
      next_page_id=next_page->GetNextPageId();
      next_page->RUnlatch();
      this->table_heap_->buffer_pool_manager_->UnpinPage(next_page->GetTablePageId(), false);
    }
  }
  return TableIterator(this->table_heap_,cur_page,rid);
}
