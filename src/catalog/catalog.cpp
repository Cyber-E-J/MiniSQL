#include "catalog/catalog.h"
using namespace std;
void CatalogMeta::SerializeTo(char *buf) const {
  //CATALOG_METADATA_MAGIC_NUM +
  /*
  std::map<table_id_t, page_id_t> table_meta_pages_+
  std::map<index_id_t, page_id_t> index_meta_pages_
  */
  //write the magic number
  uint32_t len = 0;
  MACH_WRITE_TO(uint32_t, buf + len, CATALOG_METADATA_MAGIC_NUM);
  len += sizeof(uint32_t);
  //write the tables
  uint32_t tables_len = table_meta_pages_.size();
  MACH_WRITE_TO(uint32_t, buf + len, tables_len);
  len += sizeof(uint32_t);
  for (auto table : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf + len, table.first);
    len += sizeof(table_id_t);
    MACH_WRITE_TO(page_id_t, buf + len, table.second);
    len += sizeof(page_id_t);
  }
  //write the indexes
  uint32_t indexes_len = index_meta_pages_.size();
  MACH_WRITE_TO(uint32_t, buf + len, indexes_len);
  len += sizeof(uint32_t);
  for (auto index : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf + len, index.first);
    len += sizeof(index_id_t);
    MACH_WRITE_TO(page_id_t, buf + len, index.second);
    len += sizeof(page_id_t);
  }
  return;
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  CatalogMeta *catalogmeta = ALLOC_P(heap, CatalogMeta)();
  if(!buf){
    return catalogmeta;
  }
  uint32_t magic_num = MACH_READ_FROM(uint32_t, buf);
  if(magic_num != CATALOG_METADATA_MAGIC_NUM){
    LOG(ERROR)<<"WRONG MAGIC NUMBER IN CATALOG"<<endl;
    return catalogmeta;
  } //error
  uint32_t len = sizeof(uint32_t);
  //read the tables
  uint32_t tables_len = MACH_READ_FROM(uint32_t, buf + len);
  len += sizeof(uint32_t);
  for (uint32_t i = 0; i < tables_len; i++) {
    table_id_t table_id = MACH_READ_FROM(table_id_t, buf + len);
    len += sizeof(table_id_t);
    page_id_t page_id = MACH_READ_FROM(page_id_t, buf + len);
    len += sizeof(page_id_t);
    catalogmeta->table_meta_pages_[table_id] = page_id;
  }
  //read the indexes
  uint32_t indexes_len = MACH_READ_FROM(uint32_t, buf + len);
  len += sizeof(uint32_t);
  for (uint32_t i = 0; i < indexes_len; i++) {
    index_id_t index_id = MACH_READ_FROM(index_id_t, buf + len);
    len += sizeof(index_id_t);
    page_id_t page_id = MACH_READ_FROM(page_id_t, buf + len);
    len += sizeof(page_id_t);
    catalogmeta->index_meta_pages_[index_id] = page_id;
  }
  return catalogmeta;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  return sizeof(uint32_t) + //magic number
         sizeof(uint32_t) + //table_meta_pages_ size
         (sizeof(table_id_t) + sizeof(page_id_t)) * table_meta_pages_.size() + //table_meta_pages_
         sizeof(uint32_t) + //index_meta_pages_ size
         (sizeof(index_id_t) + sizeof(page_id_t))* index_meta_pages_.size(); //index_meta_pages_
}

CatalogMeta::CatalogMeta() {}

CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  //init: true if it's the first time to access a db instance
  if(init){
    //create the catalog meta page
    catalog_meta_ = CatalogMeta::NewInstance(heap_);
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
  }
  else{
    //read the catalog meta page
    Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    buffer_pool_manager_ -> UnpinPage(CATALOG_META_PAGE_ID, false);
    //load
    catalog_meta_ = CatalogMeta::DeserializeFrom(catalog_meta_page->GetData(), heap_);
    for(auto table_info : catalog_meta_->table_meta_pages_){
      LoadTable(table_info.first, table_info.second);
    }
    for(auto index_info : catalog_meta_->index_meta_pages_){
      LoadIndex(index_info.first, index_info.second);
    }
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  //table has already been created                             
  if(GetTable(table_name,table_info)==DB_SUCCESS){
    //INFO)<<"Table already exists"<<endl;
    return DB_TABLE_ALREADY_EXIST;
  }
  //create table_info
  if(table_info != nullptr){
    LOG(INFO)<<"table_info is not null"<<endl;
    return DB_FAILED;
  }
  table_info = TableInfo::Create(heap_);
  // create table_meta
  page_id_t table_meta_page_id;
  buffer_pool_manager_->NewPage(table_meta_page_id);
  if(table_meta_page_id == INVALID_PAGE_ID){
    LOG(INFO)<<"CAN'T ALLOCATE A PAGE FOR TABLE META"<<endl;
    return DB_FAILED;
  }
  table_id_t table_id = next_table_id_;
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_,heap_);
  if(!table_heap){
    LOG(INFO)<<"FAILED TO CREATE TABLE HEAP"<<endl;
    return DB_FAILED;
  }
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), schema, table_info->GetMemHeap());
  if(!table_meta){
    LOG(INFO)<<"FAILED TO CREATE TABLE METADATA"<<endl;
    return DB_FAILED;
  }
  table_info -> Init(table_meta, table_heap);
  //add to catalog_meta_
  catalog_meta_ -> table_meta_pages_[table_id] = table_meta_page_id;
  //add to table_names_
  table_names_[table_name] = table_id;
  //add to tables_
  tables_[table_id] = table_info;
  next_table_id_ = catalog_meta_->GetNextTableId();
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if(table_names_.empty()){
    return DB_TABLE_NOT_EXIST;
  }
  auto table_iter = table_names_.find(table_name);
  //not find
  if(table_iter == table_names_.end()){
    table_info = nullptr;
    return DB_TABLE_NOT_EXIST;
  }
  //find
  return GetTable(table_iter->second, table_info);
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if(tables_.empty()) return DB_TABLE_NOT_EXIST;
  for(auto table : tables_) 
    tables.push_back(table.second);
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  //no table
  auto iter = table_names_.find(table_name);
  if(iter == table_names_.end()){
    //LOG(WARNING)<<"No table named "<<table_name<<endl;
    return DB_TABLE_NOT_EXIST;
  }
  //index has already been created                                    
  if(index_names_[table_name].find(index_name) != index_names_[table_name].end())
    return DB_INDEX_ALREADY_EXIST;
  // create index_info
  index_info = IndexInfo::Create(heap_);
  // create index_meta
  page_id_t index_meta_page_id;
  if(!buffer_pool_manager_->NewPage(index_meta_page_id)){
    LOG(ERROR)<<"Failed to allocate index meta page"<<endl;
    return DB_FAILED;
  }
  index_id_t index_id = next_index_id_;
  table_id_t table_id = iter->second;
  //
  std::vector<uint32_t> index_key_map;
  TableInfo *table_info = tables_[table_id];
  Schema *schema = table_info->GetSchema();
  //find index key map
  for(auto key : index_keys){
    uint32_t index_key_id;
    if(schema->GetColumnIndex(key, index_key_id)==DB_COLUMN_NAME_NOT_EXIST){
      //LOG(WARNING)<<"COLUMN NAME NOT EXIST!"<<endl;
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    index_key_map.push_back(index_key_id);
  }
  IndexMetadata *index_meta = IndexMetadata::Create(index_id, index_name, table_id, index_key_map, index_info->GetMemHeap());
  index_info -> Init(index_meta, table_info, buffer_pool_manager_);
  //if there are data in table, insert into the index
  // for (auto it = table_info->GetTableHeap()->Begin(txn); it != table_info->GetTableHeap()->End(); ++it) {
  //     index_info->index_->InsertEntry(*it, it->GetRowId(), txn);
  //  }//insert失败
  //add to catalog_meta_
  catalog_meta_ -> index_meta_pages_[index_id] = index_meta_page_id;
  //add to index_names_
  index_names_[table_name].emplace(index_name, index_id);
  //add to indexes_
  indexes_[index_id] = index_info;
  next_index_id_ = catalog_meta_->GetNextIndexId();
  //创建index<->rootpage等在part3实现，或者在indexes实现
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  //table_name -> index_name
  auto index_iter = index_names_.find(table_name);
  if(index_iter == index_names_.end()){
    index_info = nullptr;
    return DB_INDEX_NOT_FOUND;
  }
  //index_name -> index_id
  auto index_id_iter = index_iter->second.find(index_name);
  if(index_id_iter == index_iter->second.end()){
    index_info = nullptr;
    return DB_INDEX_NOT_FOUND;
  }
  //index_id -> index_info
  auto index_info_iter = indexes_.find(index_id_iter->second);
  if(index_info_iter == indexes_.end()){
    index_info = nullptr;
    return DB_INDEX_NOT_FOUND;
  }
  index_info = index_info_iter->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  //table_name -> index_name
  if(!indexes.empty()) {
    LOG(INFO)<<"the index vector is not empty,"
    " it will be cleared and the function continues running"<<endl;
    indexes.clear();
  }
  auto index_iter = index_names_.find(table_name);
  if(index_iter == index_names_.end())
    return DB_TABLE_NOT_EXIST;
  else if(index_iter->second.empty())
    return DB_INDEX_NOT_FOUND;
  for(auto index_id_iter : index_iter->second){//index_name -> index_id
    auto index_info_iter = indexes_.find(index_id_iter.second);//index_id -> index_info
    indexes.push_back(index_info_iter->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  TableInfo *table_info = nullptr;
  GetTable(table_name, table_info);
  if(table_info == nullptr)
    return DB_TABLE_NOT_EXIST;
  //drop table_info
  table_info -> GetTableHeap() -> FreeHeap(); //release all pages in table
  table_id_t table_id = table_info -> GetTableId();
  //drop the corresponding index_info
  vector<IndexInfo *> indexes;
  if(GetTableIndexes(table_name, indexes) == DB_SUCCESS)
    for(auto index_info : indexes)
      DropIndex(table_name, index_info->GetIndexName());
  //drop the table
  catalog_meta_->table_meta_pages_.erase(table_id);
  table_names_.erase(table_name);
  tables_.erase(table_id);
  next_table_id_ = catalog_meta_ -> GetNextTableId();
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  TableInfo *table_info = nullptr;;
  if(GetTable(table_name,table_info)==DB_TABLE_NOT_EXIST)
    return DB_TABLE_NOT_EXIST;
  IndexInfo *index_info = nullptr;
  GetIndex(table_name, index_name, index_info);
  if(index_info == nullptr)
    return DB_INDEX_NOT_FOUND;
  index_id_t index_id = index_info -> GetIndexId();  
  //drop the index
  Index *index = index_info -> GetIndex();
  index -> Destroy();
  //drop the index_info
  catalog_meta_->index_meta_pages_.erase(index_id);
  index_names_[table_name].erase(index_name);
  //has no index
  if(index_names_[table_name].size() == 0)
    index_names_.erase(table_name);
  indexes_.erase(index_id);
  next_index_id_ = catalog_meta_ -> GetNextIndexId();
  return DB_SUCCESS;
}

//use to write the catalog_meta_page, table_meta, index_meta to disk
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  char *buf = new char[PAGE_SIZE];
  catalog_meta_ -> SerializeTo(buf);
  //write to disk
  Page *page = buffer_pool_manager_ -> FetchPage(CATALOG_META_PAGE_ID);
  memcpy(page -> GetData(), buf, PAGE_SIZE);
  if(!buffer_pool_manager_ -> FlushPage(CATALOG_META_PAGE_ID)){
    LOG(ERROR)<<"can't flush to bpm!"<<std::endl;
    return DB_FAILED;
  }
  buffer_pool_manager_ -> FetchPage(INDEX_ROOTS_PAGE_ID);
  if(!buffer_pool_manager_ -> FlushPage(INDEX_ROOTS_PAGE_ID)){
    LOG(ERROR)<<"can't flush to bpm!"<<std::endl;
    return DB_FAILED;
  }
  //write table to disk
  for(auto table_iter : tables_){
    TableInfo *table_info = table_iter.second;
    page_id_t table_page_id = catalog_meta_ -> table_meta_pages_[table_info -> GetTableId()];
    //store table_meta_data
    char *table_buf = new char[PAGE_SIZE];
    uint32_t offset = 0;
    offset = table_info -> table_meta_-> SerializeTo(table_buf);
    if(offset == 0){
      delete[] table_buf;
      LOG(ERROR)<<"Table metadata not found!"<<std::endl;
      return DB_FAILED;
    }
    Page *table_page = buffer_pool_manager_ -> FetchPage(table_page_id);
    memcpy(table_page -> GetData(), table_buf, PAGE_SIZE);
    if(!buffer_pool_manager_ -> FlushPage(table_page_id)){
      delete[] table_buf;
      LOG(ERROR)<<"can't flush to bpm!"<<std::endl;
      return DB_FAILED;
    }
    delete[] table_buf;
  }
  //write index to disk 
  for(auto index_iter : indexes_){    
    IndexInfo *index_info = index_iter.second;
    page_id_t index_page_id = catalog_meta_ -> index_meta_pages_[index_info -> GetIndexId()];
    //store index_meta_data
    char *index_buf = new char[PAGE_SIZE];
    uint32_t offset = index_info -> meta_data_ -> SerializeTo(index_buf);
    if(offset == 0){
      delete[] index_buf;
      LOG(ERROR)<<"Index metadata not found!"<<std::endl;
      return DB_FAILED;
    }
    Page *index_page = buffer_pool_manager_ -> FetchPage(index_page_id);
    memcpy(index_page -> GetData(), index_buf, PAGE_SIZE);
    if(!buffer_pool_manager_ -> FlushPage(index_page_id)){
      delete[] index_buf;
      LOG(ERROR)<<"can't flush to bpm!"<<std::endl;
      return DB_FAILED;
    }
    delete[] index_buf;
  }
  return DB_SUCCESS;
}

//reopen : use to deserialize the catalog_meta_page from disk
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  Page *table_page = buffer_pool_manager_ -> FetchPage(page_id);
  //read page in buf
  if(table_page == nullptr)
    return DB_FAILED;
  //deseerialize to table_meta
  TableInfo *table_info = TableInfo::Create(heap_);
  TableMetadata *table_meta = nullptr;
  uint32_t offset = TableMetadata::DeserializeFrom(table_page -> GetData(), table_meta, table_info->GetMemHeap());
  if(offset == 0)
    return DB_FAILED;
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta -> GetFirstPageId(), table_meta->GetSchema(),
              log_manager_, lock_manager_,heap_);
  table_info->Init(table_meta, table_heap);
  //add to catalog_meta_
  catalog_meta_ -> table_meta_pages_[table_id] = page_id;
  next_table_id_= catalog_meta_ -> GetNextTableId();
  //add to table_names_
  table_names_[table_info -> GetTableName()] = table_id;
  //add to tables_
  tables_[table_id] = table_info;
  return DB_SUCCESS;
}

//reopen : use to deserialize the catalog_meta_page from disk
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  //root page
  Page *index_page = buffer_pool_manager_ -> FetchPage(page_id);
  //read page in buf
  if(!index_page)
    return DB_FAILED;
  //deserialize to index_meta
  IndexInfo *index_info = IndexInfo::Create(heap_);
  IndexMetadata *index_meta = nullptr;
  uint32_t offset = IndexMetadata::DeserializeFrom(index_page->GetData(), index_meta, index_info->GetMemHeap());
  if(offset == 0){
    LOG(ERROR)<<"Index metadata not found!"<<endl;
    return DB_FAILED;
  }
  table_id_t table_id = index_meta -> GetTableId();
  //get table_info
  auto table_iter = tables_.find(table_id);
  if(table_iter == tables_.end()){
    //LOG(ERROR)<<"Table not found!"<<endl;
    return DB_TABLE_NOT_EXIST;
  }
  TableInfo *table_info = tables_[table_id];
  index_info -> Init(index_meta, table_info, buffer_pool_manager_);
  //add to catalog_meta_
  catalog_meta_ -> index_meta_pages_[index_id] = page_id;
  next_index_id_= catalog_meta_ -> GetNextIndexId();
  //add to index_names_
  index_names_[table_info -> GetTableName()][index_info -> GetIndexName()] = index_id;
  //add to indexes_
  indexes_[index_id] = index_info;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto table_iter = tables_.find(table_id);
  //not find
  if(table_iter == tables_.end()){
    table_info = nullptr;
    return DB_TABLE_NOT_EXIST;
  }
  //find
  table_info = table_iter->second;
  return DB_SUCCESS;
}