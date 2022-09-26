#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){
  //store the total number of pages
  this->num_pages_=num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  latch_.lock();
  if(lru_list_.empty()){
    frame_id=nullptr;
    latch_.unlock();
    return false;
  }
  else{
    frame_id_t lru_frame_id = lru_list_.back();
    list_mapping_.erase(lru_frame_id);
    lru_list_.pop_back();
    *frame_id = lru_frame_id;
    latch_.unlock();
    return true;
  }
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();
  auto iteramap = list_mapping_.find(frame_id);
  if(iteramap!=list_mapping_.end()){
    //remove from list
    lru_list_.erase(list_mapping_[frame_id]);
    //remove from mapping
    list_mapping_.erase(frame_id);
  }
  latch_.unlock();
  return;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  latch_.lock();
  auto iteramap = list_mapping_.find(frame_id);
  if(iteramap==list_mapping_.end()){
    //the element has not been unpined before
    if(lru_list_.size() >= num_pages_){
      //if the replacer is full
      //remove the page of the front of the list
      list_mapping_.erase(lru_list_.front());
      lru_list_.pop_front(); 
      }
    //add this page into the front position of list
    //must do this operation first
    lru_list_.push_front(frame_id);
    list_mapping_.emplace(frame_id, lru_list_.begin());
  }
  latch_.unlock();
}

size_t LRUReplacer::Size() {
  return this->lru_list_.size();
}