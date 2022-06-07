#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator() {

}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(LeafPage* leaf_page , int index_in_tree, BufferPoolManager* buffer_pool_manager) {

  leaf_page_ = leaf_page;
  index_in_tree_ = index_in_tree;
  buffer_pool_manager_ = buffer_pool_manager;

}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(),false);
}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  return leaf_page_->GetItem(index_in_tree_);

   
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if(index_in_tree_==leaf_page_->GetSize()-1){ 
   page_id_t next_page_id = leaf_page_->GetNextPageId();

    if (next_page_id!=INVALID_PAGE_ID){
      index_in_tree_ = 0;
      Page* page = buffer_pool_manager_->FetchPage(next_page_id);
      leaf_page_ = reinterpret_cast<LeafPage *>(page->GetData());
    }
    else
      index_in_tree_++;
  }

  else index_in_tree_++;

  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  if(leaf_page_->GetPageId()==(itr.leaf_page_)->GetPageId() && index_in_tree_ == itr.index_in_tree_) return true;
  else return false;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {

  if(leaf_page_->GetPageId()!=(itr.leaf_page_)->GetPageId() || index_in_tree_ != itr.index_in_tree_) return true;
  else return false;

}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
