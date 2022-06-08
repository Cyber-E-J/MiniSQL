#ifndef MINISQL_INDEXES_H
#define MINISQL_INDEXES_H

#include <memory>

#include "catalog/table.h"
#include "index/generic_key.h"
#include "index/b_plus_tree_index.h"
#include "record/schema.h"

class IndexMetadata {
  friend class IndexInfo;

public:
  static IndexMetadata *Create(const index_id_t index_id, const std::string &index_name,
                               const table_id_t table_id, const std::vector<uint32_t> &key_map,
                               MemHeap *heap);

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap);

  inline std::string GetIndexName() const { return index_name_; }

  inline table_id_t GetTableId() const { return table_id_; }

  uint32_t GetIndexColumnCount() const { return key_map_.size(); }

  inline const std::vector<uint32_t> &GetKeyMapping() const { return key_map_; }

  inline index_id_t GetIndexId() const { return index_id_; }

private:
  IndexMetadata() = delete;

  explicit IndexMetadata(const index_id_t index_id, const std::string &index_name,
                         const table_id_t table_id, const std::vector<uint32_t> &key_map):
        index_id_(index_id), index_name_(index_name), table_id_(table_id),
        key_map_(key_map) {}

private:
  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  index_id_t index_id_;
  std::string index_name_;
  table_id_t table_id_;
  std::vector<uint32_t> key_map_;  /** The mapping of index key to tuple key */
};

/**
 * The IndexInfo class maintains metadata about a index.
 */
class IndexInfo {
  friend class CatalogManager;
public:
  static IndexInfo *Create(MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(IndexInfo));
    return new(buf)IndexInfo();
  }

  ~IndexInfo() {
    delete heap_;
  }

  void Init(IndexMetadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager) {
    // Step1: init index metadata and table info
    this -> meta_data_ = meta_data;
    this -> table_info_ = table_info;
    // Step2: mapping index key to key schema
    this -> key_schema_ = Schema::ShallowCopySchema(table_info_->GetSchema(), meta_data_->GetKeyMapping(), heap_);
    // Step3: call CreateIndex to create the index
    this -> index_ = CreateIndex(buffer_pool_manager);
    if(index_ == nullptr) {
      LOG(INFO)<<"CreateIndex failed"<<std::endl;
    }
  }

  inline Index *GetIndex() { return index_; }

  inline index_id_t GetIndexId() const { return meta_data_->GetIndexId(); }

  inline std::string GetIndexName() { return meta_data_->GetIndexName(); }

  inline IndexSchema *GetIndexKeySchema() { return key_schema_; }

  inline MemHeap *GetMemHeap() const { return heap_; }

  inline TableInfo *GetTableInfo() const { return table_info_; }

private:
  explicit IndexInfo() : meta_data_{nullptr}, index_{nullptr}, table_info_{nullptr},
                         key_schema_{nullptr}, heap_(new SimpleMemHeap()) {}

//create 失败
  Index *CreateIndex(BufferPoolManager *buffer_pool_manager) {
    Index* index = nullptr;
    uint32_t max_len = 0;
    //LOG(INFO)<<"key_schema_->GetColumnCount() = "<<key_schema_->GetColumns().empty();//=0
    for (uint32_t i = 0; i < key_schema_->GetColumnCount(); i++)
      max_len = std::max(max_len, key_schema_->GetColumn(i)->GetLength());
    if(max_len == 0) {
      LOG(INFO)<<"no attribute in index"<<std::endl;
      using INDEX_KEY_TYPE = GenericKey<64>;
      using INDEX_COMPARATOR_TYPE = GenericComparator<64>;
      using BP_TREE_INDEX = BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>;
      index = ALLOC_P(heap_,BP_TREE_INDEX)(meta_data_->GetIndexId(), key_schema_, buffer_pool_manager);
    }
    else if(max_len <= 4) {
      using INDEX_KEY_TYPE = GenericKey<4>;
      using INDEX_COMPARATOR_TYPE = GenericComparator<4>;
      using BP_TREE_INDEX = BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>;
      index = ALLOC_P(heap_,BP_TREE_INDEX)(meta_data_->GetIndexId(), key_schema_, buffer_pool_manager);
    }
    else if(max_len <= 8) {
      using INDEX_KEY_TYPE = GenericKey<8>;
      using INDEX_COMPARATOR_TYPE = GenericComparator<8>;
      using BP_TREE_INDEX = BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>;
      index = ALLOC_P(heap_,BP_TREE_INDEX)(meta_data_->GetIndexId(), key_schema_, buffer_pool_manager);
    }
    else if(max_len <= 16) {
      using INDEX_KEY_TYPE = GenericKey<16>;
      using INDEX_COMPARATOR_TYPE = GenericComparator<16>;
      using BP_TREE_INDEX = BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>;
      index = ALLOC_P(heap_,BP_TREE_INDEX)(meta_data_->GetIndexId(), key_schema_, buffer_pool_manager);
    }
    else if(max_len <= 32) {
      using INDEX_KEY_TYPE = GenericKey<32>;
      using INDEX_COMPARATOR_TYPE = GenericComparator<32>;
      using BP_TREE_INDEX = BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>;
      index = ALLOC_P(heap_,BP_TREE_INDEX)(meta_data_->GetIndexId(), key_schema_, buffer_pool_manager);
    }
    else if(max_len <= 64) {
      using INDEX_KEY_TYPE = GenericKey<64>;
      using INDEX_COMPARATOR_TYPE = GenericComparator<64>;
      using BP_TREE_INDEX = BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>;
      index = ALLOC_P(heap_,BP_TREE_INDEX)(meta_data_->GetIndexId(), key_schema_, buffer_pool_manager);
    }
    else {
      LOG(WARNING)<<"Index key length is too long"<<std::endl;
      return nullptr;
    }
    return index;
  }

private:
  IndexMetadata *meta_data_;
  Index *index_;//索引操作对象
  TableInfo *table_info_;//索引对应的表信息
  IndexSchema *key_schema_;//索引模式信息
  MemHeap *heap_;
};

#endif //MINISQL_INDEXES_H
