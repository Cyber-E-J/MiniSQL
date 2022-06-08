#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  FlushAllPage();
  delete[] pages_;//maybe unused?
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  latch_.lock();
  auto table_iter = page_table_.find(page_id);
  // 1.1    If P exists, pin it and return it immediately.
  if(table_iter != page_table_.end()){
    frame_id_t frame_id = table_iter->second;
    replacer_->Pin(frame_id);
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    latch_.unlock();
    return page;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  else{
    frame_id_t replace_frame_id;
    if(!free_list_.empty()){//find a free page (R) from the free list
      replace_frame_id = free_list_.front();
      free_list_.pop_front();
    }
    else if(replacer_->Victim(&replace_frame_id)){//find a replacement page (R) from the replacer
      // 2.     If R is dirty, write it back to the disk.
      if(pages_[replace_frame_id].IsDirty())
        FlushPage(pages_[replace_frame_id].GetPageId());
      // 3.     Delete R from the page table and insert P.
      //latch?
    }
    else{
      LOG(INFO)<<"Cna't find a replacement page"<<std::endl;
      latch_.unlock();
      return nullptr;
    }
    //use pageid to erase
    page_table_.erase(pages_[replace_frame_id].GetPageId());
    page_table_.emplace(page_id, replace_frame_id);
    // 4.     Update P's metadata
    Page *page = &pages_[replace_frame_id];
    page->page_id_ = page_id;
    page->pin_count_ = 0;
    page->is_dirty_ = false;
    page->pin_count_++;
    //read in the page content from disk
    disk_manager_->ReadPage(page_id, page->data_);
    replacer_->Pin(replace_frame_id);
    //return a pointer to P.
    latch_.unlock();
    return page;
  }
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  latch_.lock();
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if(CheckAllPinned()){
    LOG(INFO)<<"All pinned"<<std::endl;
    latch_.unlock();
    return nullptr;
  }
  page_id_t n_page_id = AllocatePage();
  if(n_page_id == INVALID_PAGE_ID){
    LOG(INFO)<<"AllocatePage failed"<<std::endl;
    latch_.unlock();
    return nullptr;
  }
  //debug
  //LOG(INFO)<<"page_id: "<<n_page_id<<endl;
  // 2.   Pick a victim page P from either the free list or the replacer.
  // Always pick from the free list first.
  frame_id_t frame_id;
  if(!free_list_.empty()){//find a replacement page (P) from the free list
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  else if(replacer_->Victim(&frame_id)){//find a replacement page (P) from the replacer
    if(pages_[frame_id].IsDirty()) FlushPage(pages_[frame_id].GetPageId());
    // 3.   Update P's metadata, zero out memory and add P to the page table.
  }
  else{
    DeallocatePage(n_page_id);
    latch_.unlock();
    return nullptr;
  }
  page_table_.erase(pages_[frame_id].GetPageId());
  page_table_.emplace(n_page_id, frame_id);
  Page *page = &pages_[frame_id];
  page->ResetMemory();
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  page->page_id_ = n_page_id;
  replacer_->Pin(frame_id);
  //FlushPage(n_page_id);
  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_id = n_page_id;
  latch_.unlock();
  return page;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  latch_.lock();
    // 1.   Search the page table for the requested page (P).
  auto table_iter = page_table_.find(page_id);
  // 1.   If P does not exist, return true.
  if(table_iter == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if(pages_[page_table_[page_id]].GetPinCount() != 0) {
    latch_.unlock();
    return false;
  }
  // 0.   Make sure you call DeallocatePage!
  DeallocatePage(page_id);
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  frame_id_t frame_id = page_table_[page_id];
  page_table_.erase(page_id);
  free_list_.emplace_back(frame_id);
  //Update P's metadata
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  latch_.unlock();
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  auto table_iter = page_table_.find(page_id);
  if(table_iter != page_table_.end()){
    frame_id_t frame_id = table_iter->second;
    if(is_dirty) pages_[frame_id].is_dirty_ = true;
    if(pages_[frame_id].GetPinCount() == 0){
      //this page has been moved into replacer
      latch_.unlock();
      return false;
    }
    pages_[frame_id].pin_count_--;
    if(pages_[frame_id].GetPinCount() == 0)
    //need to add into replacer
      replacer_->Unpin(frame_id);
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {//write back
  latch_.lock();
  auto table_iter = page_table_.find(page_id);
  if(table_iter == page_table_.end() || page_id==INVALID_PAGE_ID) {
    latch_.unlock();
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[table_iter->second].GetData());
  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushAllPage() {
  latch_.lock();
  if(page_table_.empty()) {
    latch_.unlock();
    return false;
  }
  auto iter = page_table_.begin();
  while(true){
    if(iter == page_table_.end()) break;
    if(FlushPage(iter->first));
    else{
      latch_.unlock();
      return false;
    }
    iter++;
  }
  latch_.unlock();
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}


// Only used for debug
bool BufferPoolManager::CheckAllPinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ == 0) {
      res = false;
      break;
    }
  }
  return res;
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}