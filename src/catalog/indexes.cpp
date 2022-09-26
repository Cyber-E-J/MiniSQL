#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  //INDEX_METADATA_MAGIC_NUM +
  /*
  index_id_t index_id_+
  std::string index_name_+
  table_id_t table_id_+
  std::vector<uint32_t> key_map_   The mapping of index key to tuple key 
  */
  //write the magic number
  uint32_t len = 0;
  MACH_WRITE_TO(uint32_t, buf + len, INDEX_METADATA_MAGIC_NUM);
  len += sizeof(uint32_t);
  //write the index id
  MACH_WRITE_TO(index_id_t, buf + len, index_id_);
  len += sizeof(index_id_t);
  //write the index name
  uint32_t index_name_len = index_name_.size();
  MACH_WRITE_TO(uint32_t, buf + len, index_name_len);
  len += sizeof(uint32_t);
  memcpy(buf + len, index_name_.c_str(), index_name_len);
  len += index_name_len;
  //write the corresponding table id
  MACH_WRITE_TO(table_id_t, buf + len, table_id_);
  len += sizeof(table_id_t);
  //write the mapping of index key to tuple key  
  uint32_t key_map_len = key_map_.size();
  MACH_WRITE_TO(uint32_t, buf + len, key_map_len);
  len += sizeof(uint32_t);
  for (auto key_map : key_map_) {
    MACH_WRITE_UINT32(buf + len, key_map);
    len += sizeof(uint32_t);
  }
  return GetSerializedSize();
}

uint32_t IndexMetadata::GetSerializedSize() const {
  return  sizeof(uint32_t) + //magic number
          sizeof(index_id_t) + //index id
          sizeof(uint32_t) + //index name length
          index_name_.size()+ //index name
          sizeof(table_id_t) + //table id
          sizeof(uint32_t)+ //key map length
          sizeof(uint32_t) * key_map_.size(); //key map
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  if(!buf){
    index_meta = nullptr;
    return 0;
  }
  if(index_meta != nullptr){
    LOG(ERROR) << "index_meta is not null"<<std::endl;
    index_meta = nullptr;
    return 0;
  }
  uint32_t magic_num = MACH_READ_FROM(uint32_t, buf);
  if(magic_num != INDEX_METADATA_MAGIC_NUM){
    index_meta = nullptr;
    return sizeof(uint32_t);
  } //error
  uint32_t len = sizeof(uint32_t);
  //read the index id
  index_id_t index_id = MACH_READ_FROM(index_id_t, buf + len);
  len += sizeof(index_id_t);
  //read the index name
  //read the index name length
  uint32_t index_name_len = MACH_READ_FROM(uint32_t, buf + len);
  len += sizeof(uint32_t);
  //read the index name
  char index_name_buf[index_name_len+1];
  memcpy(index_name_buf, buf + len, index_name_len);
  index_name_buf[index_name_len] = '\0';
  len += index_name_len;
  std::string index_name(index_name_buf, index_name_len);
  //read the corresponding table id
  table_id_t table_id = MACH_READ_FROM(table_id_t, buf + len);
  len += sizeof(table_id_t);
  //read the mapping of index key to tuple key
  uint32_t key_map_len = MACH_READ_FROM(uint32_t, buf + len);
  len += sizeof(uint32_t);
  std::vector<uint32_t> key_map;
  for (uint32_t i = 0; i < key_map_len; i++) {
    uint32_t key_map_i = MACH_READ_FROM(uint32_t, buf + len);
    len += sizeof(uint32_t);
    key_map.emplace_back(key_map_i);
  }
  index_meta = ALLOC_P(heap, IndexMetadata)(index_id, index_name, table_id, key_map);
  return len; 
}