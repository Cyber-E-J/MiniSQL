#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  //如何计算每一页的空闲空间:有函数
  //需要从缓冲池中取page？
  //在某一页中的插入row直接调用table_page的函数
  //当所有页都不能插入的时候，新增页吗？

  //从firstpageid开始遍历
  page_id_t current_page_id=first_page_id_;
  page_id_t last_page_id=INVALID_PAGE_ID;
  TablePage * page=nullptr;
  while(current_page_id!=INVALID_PAGE_ID){
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
    if (page == nullptr) {
      return false;
    }
    if(page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
      //cout<<"in_page_insert"<<endl;
      buffer_pool_manager_->UnpinPage(current_page_id,true);
      return true;
    }
    last_page_id=current_page_id;
    current_page_id=page->GetNextPageId();
    if(current_page_id==INVALID_PAGE_ID)break;
    buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
  }
  //需要新增page
  //新增页后仍然有可能插入不进去，此时返回false
  //last_page_id 为当前的最后一页

  //上一页的pageid忘记了更新！！！
  bool result=false;
  page_id_t new_page_id;
  auto new_page=reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
  if(new_page==nullptr){
    std::cout<<"new page fail"<<std::endl;
    buffer_pool_manager_->CheckAllUnpinned();
    exit(-1);
  }
  new_page->Init(new_page_id,last_page_id,log_manager_,txn);
  if(first_page_id_==INVALID_PAGE_ID)first_page_id_=new_page_id;
  cout<<"new_page_id="<<new_page_id<<endl;
  if(new_page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
    if(page)page->SetNextPageId(new_page_id);
    result=true;
  }
  if(page)buffer_pool_manager_->UnpinPage(page->GetPageId(),result);
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(),result);
  return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  bool ret;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  
  page->WLatch();
  ret=page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return ret;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  //调用该页的update函数，如果成果直接结束，当taplepage因为空间不够无法update时
  //在本函数中delete+insert
  // Find the page which contains the tuple.
  //在此函数中，只要更新成功就返回true(不管是如何更新的)
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  Row old_row(rid); //old_row 应该还要初始化
  if(page->GetTuple(&old_row,schema_,txn,lock_manager_)){
    //成功取到要更新的tuple,在old_row中
    int false_reason;
    false_reason=page->UpdateTuple(row,&old_row,schema_,txn,lock_manager_,log_manager_);
    if(false_reason==1)return true; //成功在该页更新
    if(false_reason==0)return false; //因为其他原因导致无法更新
    if(false_reason==-1){
      //因为更新后的数据无法容纳
      //delete+insert
      Row new_row_insert=row;
      if(InsertTuple(new_row_insert,txn)){
        page->MarkDelete(rid,txn,lock_manager_,log_manager_);
        page->ApplyDelete(rid,txn,log_manager_);
        return true;
        //成功插入新的tuple后再删除否则不删除，返回false
      }else{
        return false;
      }
    }

  }
  return false;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    std::cout<<"ApplyDelete can't find the page which contains the given tuple"<<std::endl;
    return;
  }
  //需要有latch 操作吗？
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return;
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
//销毁整个TableHeap并释放这些数据页
//遍历堆表释放数据页
  page_id_t current_page_id=first_page_id_;
  page_id_t next_page_id;
  TablePage * page;
  while(current_page_id!=INVALID_PAGE_ID){
    //先fetch,unpin 再delete
   // next_page_id=
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
    next_page_id=page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(current_page_id,false);
    buffer_pool_manager_->DeletePage(current_page_id);
    current_page_id=next_page_id;
  }
  
  first_page_id_=INVALID_PAGE_ID; //这一句话需要吗？
 // delete table
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  //获取RowID 为row->rid_  的记录，(记录应该在row中返回)
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if(page==nullptr)return false;
  return page->GetTuple(row,schema_,txn,lock_manager_);
}

TableIterator TableHeap::Begin(Transaction *txn) {
  TablePage* first_page= reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId first_row;
  bool no_tuple=false;
  while(!first_page->GetFirstTupleRid(&first_row)){
    page_id_t next_page_id=first_page->GetNextPageId();
    if(next_page_id==INVALID_PAGE_ID){
      no_tuple=true;
      break;
    }
    first_page= reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
  }
  if(no_tuple)return TableIterator(this,nullptr,INVALID_ROWID);
  //如果第一页中没有元组
  //构造函数的时候再new Row,方便delete
 // TableIterator first_iterator(this,first_page,first_row);
  return TableIterator(this,first_page,first_row);
}

//尾迭代器理解错误,尾迭代器指的是不存在的内容而不是最后一个元素
TableIterator TableHeap::End() {
//   page_id_t current_page_id=first_page_id_;
//   page_id_t last_page_id;
//   TablePage * page;
//   //get last page of tableheap
//   while(current_page_id!=INVALID_PAGE_ID){
//     page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
//     last_page_id=current_page_id;
//     current_page_id=page->GetNextPageId();
//   }
//   //page=b
//   RowId first_rid;
//   bool no_tuple=false;
//   TablePage* last_page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page_id));
//   while(!last_page->GetFirstTupleRid(&first_rid)){
//     //最后一页没有元组时需要找上一页
//     current_page_id=last_page->GetPrevPageId();
//     if(current_page_id==INVALID_PAGE_ID){
//       no_tuple=true;
//       break;
//     }
//     last_page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
//   }
//   if(no_tuple)return TableIterator(this,nullptr,INVALID_ROWID);
//   RowId last_rid=first_rid;
//   RowId next_rid;
//   while(last_page->GetNextTupleRid(last_rid,&next_rid)){
//     last_rid=next_rid;
//   }
//   //在最后一页中寻找最后一个元组
//  // while(la)
//  return TableIterator(this,last_page,last_rid);
    return TableIterator(this,nullptr,INVALID_ROWID);
}
