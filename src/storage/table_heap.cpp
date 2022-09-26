#include "storage/table_heap.h"
#include "glog/logging.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  page_id_t last_page_id = INVALID_PAGE_ID;
 // firstfit改为nextfit
  if (cur_pid_ != INVALID_PAGE_ID) {  //如果本页合法
    //插入本页
    auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_pid_));
    ASSERT(page != nullptr,"Not found InsertTuple first page!");
    page->WLatch();
    bool is_insert=page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), is_insert);
    if (is_insert) return true;//如果插入成功就返回
    /*while (!is_insert) {//插入不成功就一直寻找下一页直至成功或下一页不合法
      last_page_id = page->GetPageId();
      if (page->GetNextPageId() == INVALID_PAGE_ID) {
        break;
      }
      auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
      next_page->WLatch();
      is_insert=next_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      next_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(next_page->GetTablePageId(), is_insert);
      page = next_page;
    }
    if (is_insert) return true;*/
  }
//本页不合法就创建新页
  page_id_t new_page_id=INVALID_PAGE_ID;
  auto new_page = static_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
  ASSERT(new_page != nullptr,"Can't create new page!");
  new_page->Init(new_page_id,last_page_id,log_manager_,txn);
  new_page->WLatch();
  bool is_insert=new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  new_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(new_page->GetTablePageId(), is_insert);
  if(cur_pid_!=INVALID_PAGE_ID){
    auto page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_pid_));
    page->SetNextPageId(new_page->GetPageId());//连接新页
    cur_pid_ = new_page->GetPageId();//修改curpid
  } else{
    cur_pid_=first_page_id_ = new_page->GetPageId();
  } 
  return is_insert;
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
//记得修改头文件接口
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
   ASSERT(page != nullptr,"Not found UpdateTuple page!");
  if (rid.GetPageId() == INVALID_PAGE_ID) return false;
  Row old_row(rid);
  bool find_old_row=page->GetTuple(&old_row, schema_,txn,lock_manager_);
  if (!find_old_row) return false;
  page->WLatch();
  
  //接口修改为int 返回值区分错误类型 0代表slotnum不合法，1代表这个元组已被删除，2代表没有足够的空间去update 3代表成功更新
  int is_update = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if(is_update==2||is_update==0){
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }
  else if(is_update==1){
    row.SetRowId(rid);
    this->MarkDelete(rid, txn);
    this->ApplyDelete(rid, txn);
    bool is_insert=this->InsertTuple(row, txn);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return is_insert;
  }
  else{
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), is_update);
    return true;
  }
  
}


void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  ASSERT(page != nullptr,"Not found ApplyDelete page!");
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() { 
  page_id_t current_page_id=first_page_id_;
  page_id_t next_page_id;
  TablePage * page;
  while(current_page_id!=INVALID_PAGE_ID){
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
    next_page_id=page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(current_page_id,false);
    buffer_pool_manager_->DeletePage(current_page_id);
    current_page_id=next_page_id;
  }
  cur_pid_ = INVALID_PAGE_ID;
  first_page_id_ = INVALID_PAGE_ID;  //
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  RowId rid = row->GetRowId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  ASSERT(page != nullptr,"Not found gettuple page!");
  page->RLatch();
  bool get_status=page->GetTuple(row, schema_, txn, lock_manager_);
 // row->SetRowId(rid);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return get_status;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  RowId rid;
  auto pid = first_page_id_;
  bool no_tuple=false;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(pid));
  while(pid!=INVALID_PAGE_ID){
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(pid));
    page->RLatch();
    bool have_tuple = page->GetFirstTupleRid(&rid);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    if(have_tuple){
      return TableIterator(this,page, rid);
    }
    else{
      pid = page->GetNextPageId();
      no_tuple = true;
    }
  }
  if (no_tuple) return TableIterator(this, nullptr, INVALID_ROWID);
  return TableIterator(this, page, rid);
}

TableIterator TableHeap::End() {
  return TableIterator(this, nullptr,RowId(INVALID_PAGE_ID,0));
}
