#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  
    uint32_t len = 0;
    MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM );//加入魔数
    len += sizeof(uint32_t);
    MACH_WRITE_TO(size_t,buf+len,(size_t) columns_.size() );//加入列的数组大小
    len += sizeof(size_t);
    for (size_t i = 0; i < columns_.size();i++){//加入column
      len += columns_[i]->SerializeTo(buf + len);
    }
     return len;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t len = 0;
  for (size_t i = 0; i < columns_.size(); i++) {  //加入column
    len += columns_[i]->GetSerializedSize();
  }
  return sizeof(uint32_t) + sizeof(size_t) + len;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  if (schema!= nullptr) {
    LOG(WARNING) << "Pointer to schema is not null in column deserialize." << std::endl;
  }
  void *mem = heap->Allocate(sizeof(Schema));
  uint32_t len = 0;
  uint32_t MAGIC_NUM = MACH_READ_FROM(uint32_t,buf);//取出魔数
  ASSERT(MAGIC_NUM == SCHEMA_MAGIC_NUM, "index_meta Magic Number Not Equal!");
  len += sizeof(uint32_t);
  size_t vector_size = MACH_READ_UINT32(buf + len);//取出列数目
  len += sizeof(size_t);
  std::vector<Column *> columns_vector;
  for (size_t i = 0; i < vector_size; i++){
    Column *column = nullptr;
    len += Column::DeserializeFrom(buf + len, column, heap);
    columns_vector.push_back(column);
  }
  schema = new(mem)Schema(columns_vector);
  return len; 
}