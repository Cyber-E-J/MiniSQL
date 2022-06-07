#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          root_page_id_(INVALID_PAGE_ID),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size)
          {
  IndexRootsPage *rootpage=reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  page_id_t root_page_id;
  bool flag = rootpage->GetRootId(index_id,&root_page_id);
  if(flag==true) root_page_id_ = root_page_id;
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  //if(root_page_id_ ==INVALID_PAGE_ID) return true;
  if(root_page_id_ == INVALID_PAGE_ID) return true;
  return false;

}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  Page * page = FindLeafPage(key);
  LeafPage * leaf_page = reinterpret_cast<LeafPage*> (page->GetData());


  ValueType value;
  bool ret = leaf_page->Lookup(key,value,comparator_);

  buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
  result.push_back(value);
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if(IsEmpty()){
    StartNewTree(key,value);
    return true;
  }

  return InsertIntoLeaf(key,value,transaction);

}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {

  page_id_t root_page_id;

  Page *root_page=buffer_pool_manager_->NewPage(root_page_id);
  //ASSERT(root_page_id!=INVALID_PAGE_ID,"root_page_id is invalid!");
  if(root_page==nullptr){
    throw std::runtime_error("out of memory");
  }else{
    root_page_id_=root_page_id;
    LeafPage *root_node=reinterpret_cast<LeafPage*>(root_page->GetData());
    root_node->Init(root_page_id,INVALID_PAGE_ID,leaf_max_size_);
    root_node->Insert(key,value,comparator_);
    UpdateRootPageId(1);
    buffer_pool_manager_->UnpinPage(root_page_id,true);
  }

}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page* page = FindLeafPage(key);
  LeafPage * leaf_page = reinterpret_cast<LeafPage * >(page->GetData());

  int leaf_size = leaf_page->GetSize();
  int inserted_size = leaf_page->Insert(key,value,comparator_);

  if(leaf_size == inserted_size){//重复 key
    buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
    return false;
  }

  if(inserted_size > leaf_max_size_){//分裂
    LeafPage* NewPage = Split(leaf_page);
    InsertIntoParent(leaf_page,NewPage->KeyAt(0),NewPage,transaction);

    buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
    buffer_pool_manager_->UnpinPage(NewPage->GetPageId(),true);
    UpdateRootPageId(0);
  }

  else{//不分裂
    buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
  } 
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id;
  Page * new_page = buffer_pool_manager_->NewPage(new_page_id);
  if(new_page == nullptr){
    throw std::runtime_error("out of memory");
    return nullptr;
  }
  else{
    N* new_node = reinterpret_cast<N *>(new_page->GetData());

    if(node->IsLeafPage()){
      new_node->SetPageType(IndexPageType::LEAF_PAGE);

      LeafPage * old_leaf_node = reinterpret_cast<LeafPage*>(node);
      LeafPage * new_leaf_node = reinterpret_cast<LeafPage*>(new_node);

      new_leaf_node->Init(new_page_id,old_leaf_node->GetParentPageId(),leaf_max_size_);
      old_leaf_node->MoveHalfTo(new_leaf_node);

      new_leaf_node->SetNextPageId(old_leaf_node->GetNextPageId());
      old_leaf_node->SetNextPageId(new_leaf_node->GetPageId());

      new_node = reinterpret_cast<N*>(new_leaf_node);

    }
    else{
      new_node->SetPageType(IndexPageType::INTERNAL_PAGE);
      InternalPage* old_internal_node = reinterpret_cast<InternalPage*>(node); 
      InternalPage* new_internal_node = reinterpret_cast<InternalPage*>(new_node); 

      new_internal_node->Init(new_page_id,old_internal_node->GetParentPageId(),internal_max_size_);
      old_internal_node->MoveHalfTo(new_internal_node,buffer_pool_manager_);
      new_node = reinterpret_cast<N*>(new_internal_node);

    }



  return new_node;
  }

  
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
//old_node是左孩子，new_node是右孩子，key是他们的分界
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  page_id_t parent_page_id = old_node->GetParentPageId();
  if(parent_page_id == INVALID_PAGE_ID){//根节点
    page_id_t NewRootPageID = INVALID_PAGE_ID;
    Page* page = buffer_pool_manager_->NewPage(NewRootPageID);
    InternalPage* NewRootPage = reinterpret_cast<InternalPage*> (page->GetData());

    root_page_id_ = NewRootPageID;

    old_node->SetParentPageId(NewRootPageID);
    new_node->SetParentPageId(NewRootPageID); 
    NewRootPage->Init(NewRootPageID,INVALID_PAGE_ID,internal_max_size_);
    NewRootPage->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(NewRootPageID,true);

  }else{//内部节点
     
    Page* page = buffer_pool_manager_->FetchPage(parent_page_id);
    InternalPage* parent_page = reinterpret_cast<InternalPage *>(page->GetData());

    page_id_t old_node_value = old_node->GetPageId();
    page_id_t new_node_value = new_node->GetPageId();
    //将new_node插入在old_node之后
    int new_size = parent_page->InsertNodeAfter(old_node_value,key,new_node_value);
    if(new_size<=internal_max_size_) buffer_pool_manager_->UnpinPage(parent_page_id,true);
    else{//parentPage 也满了
      InternalPage* new_page = Split(parent_page);
      KeyType new_key = new_page->KeyAt(0);

      InsertIntoParent(parent_page,new_key,new_page,transaction);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
      buffer_pool_manager_->UnpinPage(new_page->GetPageId(),true);

    }

  }                                      
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if(IsEmpty()) return;
  Page * page = FindLeafPage(key);

  LeafPage * leaf_page = reinterpret_cast<LeafPage * >(page->GetData());

  
  int original_size = leaf_page->GetSize();  
  int after_size = leaf_page->RemoveAndDeleteRecord(key,comparator_);

  //如果没有删除
  if(original_size == after_size){
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),false);
    //没有变换
    return;
  }
  else{
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
    //变换了

    //true说明应该删除
    if(CoalesceOrRedistribute(leaf_page,transaction)){
      buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
    }
  }


}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  //考虑是否是根节点
  if(node->GetPageId() == this->root_page_id_){
    return AdjustRoot(node);
  }
  //中间节点或者叶节点
  else{
    //没到最小
    if(node->GetSize()>= node->GetMinSize()) return false;

    Page* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    InternalPage* parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

    int _index = parent_node->ValueIndex(node->GetPageId());
    int sibling_index;

    //如果是最左节点，和右边的节点操作
    if(_index ==0 ) sibling_index = 1;
    //否则，和左边的节点操作
    else sibling_index = _index - 1;

    page_id_t sibling_page_id = parent_node->ValueAt(sibling_index);

    Page* sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);

    N* sibling_node = reinterpret_cast<N*> (sibling_page->GetData());


    //如果size之和大于maxsize，那么重新分配
    if(sibling_node->GetSize() + node->GetSize()>= node->GetMaxSize()){
      Redistribute(sibling_node,node,_index);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
      buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(),true);
      return false;

    }
    //否则 合并
    else{
      if(Coalesce(&sibling_node,&node,&parent_node,_index,transaction)){
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
        buffer_pool_manager_->DeletePage(parent_node->GetPageId());
      }
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
      buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(),true);
      return true;
    }

  }
  
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
//合并两个结点 index是node的下标
//大多数情况，neighbor_node在node的前面 （neighbour_node,node)
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  bool is_leaf = (*node) -> IsLeafPage();
  //bool flag = false;
  //最左节点
  if(index == 0){

    if(is_leaf){ //最左边的叶节点
      Page* first_page = FindLeafPage((*node)->KeyAt(1),true);
      LeafPage* first_node = reinterpret_cast<LeafPage*>(first_page->GetData());
      if((*node)->GetPageId()!=first_node->GetPageId()){//不是整个树最左边的节点
        while(true){//从叶子结点底层获得并更新NextPageId
          if(first_node->GetNextPageId() == (*node)->GetPageId()){
            first_node->SetNextPageId((*neighbor_node)->GetPageId());
            break;
            
          }

          page_id_t next_page_id = first_node->GetNextPageId();
          Page* next_page = buffer_pool_manager_->FetchPage(next_page_id);
          LeafPage* next_node = reinterpret_cast<LeafPage*> (next_page->GetData());

          buffer_pool_manager_->UnpinPage(next_page_id,false);
          first_node = next_node;

        }
      }

      LeafPage * front_leaf = reinterpret_cast<LeafPage*>(*node);
      LeafPage * rear_leaf = reinterpret_cast<LeafPage*>(*neighbor_node);

      rear_leaf->MoveAllTo(front_leaf);
      front_leaf->MoveAllTo(rear_leaf);

      

    }

    else{//在最左边的中间节点
      InternalPage * front_leaf = reinterpret_cast<InternalPage*>(*node);
      InternalPage * rear_leaf = reinterpret_cast<InternalPage*>(*neighbor_node);
      rear_leaf->MoveAllTo(front_leaf,(*parent)->KeyAt(1),buffer_pool_manager_);
      front_leaf->MoveAllTo(rear_leaf,(*parent)->KeyAt(1),buffer_pool_manager_);

    }
    (*parent)->SetKeyAt(1,((*parent)->KeyAt(0)));

    //交换前两个节点
    // if(flag){
    //     auto value = (*parent)->ValueAt(0);
    //     auto key = (*parent)->KeyAt(0);
    //     //auto tmp = (*parent)->array_[1];
    //     (*parent)->Remove(0);
    //     (*parent)->InsertNodeAfter((*parent)->ValueAt(0),key,value);
    // }


    
    


    (*parent)->Remove(index);
  }
  
  else{
    if(is_leaf){
      LeafPage * front_leaf = reinterpret_cast<LeafPage*>(*neighbor_node);
      LeafPage * rear_leaf = reinterpret_cast<LeafPage*>(*node);

      rear_leaf->MoveAllTo(front_leaf);
    }
    else{
      InternalPage * front_leaf = reinterpret_cast<InternalPage*>(*neighbor_node);
      InternalPage * rear_leaf = reinterpret_cast<InternalPage*>(*node);
      rear_leaf->MoveAllTo(front_leaf,(*parent)->KeyAt(index),buffer_pool_manager_);
    }

    (*parent)->Remove(index);
  }

  
  return CoalesceOrRedistribute(*parent,transaction);

}


/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
//重新分配——从兄弟那儿借一个
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
 bool is_leaf = node->IsLeafPage();
  
  
  if (is_leaf){
    LeafPage* neighbor_leaf = reinterpret_cast<LeafPage *>(neighbor_node);
    LeafPage* leaf = reinterpret_cast<LeafPage *>(node);

    ASSERT(neighbor_leaf!=nullptr,"Redistribute: neighbor leaf is null!");
    ASSERT(leaf!=nullptr,"Redistribute: leaf is null!");

    page_id_t parent_page_id = leaf->GetParentPageId();
    Page* page = buffer_pool_manager_->FetchPage(parent_page_id);
    InternalPage* parent_page = reinterpret_cast<InternalPage *>(page->GetData());

    ASSERT(parent_page!=nullptr,"Redistribute: parent_page is null!");


    if (index == 0){//最左节点
      //neighbor_node 在 node 之后
      neighbor_leaf->MoveFirstToEndOf(leaf);

      parent_page->SetKeyAt(1,neighbor_leaf->KeyAt(0));

      //unpin父页并设为脏页
      buffer_pool_manager_->UnpinPage(parent_page_id,true);
    }
    else{//非最左
      
      //node在neighbor_node之后
      ASSERT(neighbor_leaf!=nullptr,"Redistribute P: neighbor leaf is null!");
      ASSERT(leaf!=nullptr,"Redistribute P: leaf is null!");
      neighbor_leaf->MoveLastToFrontOf(leaf);

      ASSERT(parent_page!=nullptr,"In Redistribute 1: parent_page is null!");

      parent_page->SetKeyAt(index,leaf->KeyAt(0));

      buffer_pool_manager_->UnpinPage(parent_page_id,true);
    }
  }
  
  
  else{
    InternalPage* neighbor_internal = reinterpret_cast<InternalPage *>(neighbor_node);
    InternalPage* internal = reinterpret_cast<InternalPage *>(node);


    page_id_t parent_page_id = internal->GetParentPageId();
    Page* page = buffer_pool_manager_->FetchPage(parent_page_id);
    InternalPage* parent_page = reinterpret_cast<InternalPage *>(page->GetData());

    if (index == 0){//如果node在最左边
      //(node,neighbor_node)

      KeyType middle_key = parent_page->KeyAt(1);


      neighbor_internal->MoveFirstToEndOf(internal,middle_key,buffer_pool_manager_);


      parent_page->SetKeyAt(1,neighbor_internal->KeyAt(0));
      //unpin父页并设为脏页
      buffer_pool_manager_->UnpinPage(parent_page_id,true);
    }
    else{//如果node不在最左边
      //(neighbor_node,node)

      KeyType middle_key=neighbor_internal->KeyAt(neighbor_internal->GetSize()-1);

      neighbor_internal->MoveLastToFrontOf(internal,middle_key,buffer_pool_manager_);

      parent_page->SetKeyAt(index,node->KeyAt(0));

      buffer_pool_manager_->UnpinPage(parent_page_id,true);
    }
  }

}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  //case 2
  if(old_root_node->IsLeafPage()&&old_root_node->GetSize()==0){
    //set empty
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }

  //case 1 child 成为新的 root
  else if(!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1){
    InternalPage* old_root_data = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t child_page_id;
    child_page_id = old_root_data->RemoveAndReturnOnlyChild();
    root_page_id_ = child_page_id;
    UpdateRootPageId(0);
    //更新新的root_page的情况
    Page* new_root_page=buffer_pool_manager_->FetchPage(child_page_id);
    InternalPage* new_root_node=reinterpret_cast<InternalPage*>(new_root_page->GetData());

    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(child_page_id,true);
    return true;
  }

  else{
    return false;
    //不用删除
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {

  Page* first_page=FindLeafPage(KeyType(),true);

  LeafPage* first_node=reinterpret_cast<LeafPage*>(first_page->GetData());
  
  return INDEXITERATOR_TYPE(first_node,0,buffer_pool_manager_);

}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *page = FindLeafPage(key);
  LeafPage *leaf_page =  reinterpret_cast<LeafPage *>(page->GetData());
  int index = leaf_page->KeyIndex(key,comparator_);
  return INDEXITERATOR_TYPE(leaf_page,index,buffer_pool_manager_);

}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  //Page *page = FindLeafPage(KeyType(),false,true);

  Page* curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  buffer_pool_manager_->UnpinPage(root_page_id_,false);

  BPlusTreePage * curr_node = reinterpret_cast<BPlusTreePage*>(curr_page->GetData());

  //找到根节点

  while(!curr_node->IsLeafPage()){
    InternalPage* internal_node = reinterpret_cast<InternalPage*>(curr_node);
    page_id_t child_page_id;

    child_page_id = internal_node->ValueAt(internal_node->GetSize()-1);
    // child_page_id = internal_node->Lookup(key,comparator_);
  
    Page* child_page = buffer_pool_manager_->FetchPage(child_page_id);
    ASSERT(child_page!=nullptr,"child page is null!");

    buffer_pool_manager_->UnpinPage(child_page_id,false);
    BPlusTreePage * child_node = reinterpret_cast<BPlusTreePage*>(child_page->GetData());

    curr_page = child_page;
    curr_node = child_node;

  }


  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(curr_page->GetData());
  return INDEXITERATOR_TYPE(leaf_page,leaf_page->GetSize(),buffer_pool_manager_);

}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */


INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  Page* curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  buffer_pool_manager_->UnpinPage(root_page_id_,false);

  BPlusTreePage * curr_node = reinterpret_cast<BPlusTreePage*>(curr_page->GetData());

  //找到根节点

  while(!curr_node->IsLeafPage()){
    InternalPage* internal_node = reinterpret_cast<InternalPage*>(curr_node);
    page_id_t child_page_id;

    if(leftMost) child_page_id = internal_node->ValueAt(0);
    else child_page_id = internal_node->Lookup(key,comparator_);
  
    Page* child_page = buffer_pool_manager_->FetchPage(child_page_id);
    ASSERT(child_page!=nullptr,"child page is null!");

    buffer_pool_manager_->UnpinPage(child_page_id,false);
    BPlusTreePage * child_node = reinterpret_cast<BPlusTreePage*>(child_page->GetData());

    curr_page = child_page;
    curr_node = child_node;

  }
  return curr_page;

}


/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  IndexRootsPage *rootpage=reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  if(insert_record==0)
    rootpage->Update(index_id_,root_page_id_);
  else
    rootpage->Insert(index_id_,root_page_id_);
  
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,true);
}
/**
 * This method is used for debug only, You don't need to modify
 */


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}





template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;