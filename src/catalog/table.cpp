#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  //TABLE_METADATA_MAGIC_NUM +
  //table_id_t table_id_+
  //std::string table_name_+
  //page_id_t root_page_id_+
  //schema_

  //write the magic number
  uint32_t len = 0;
  MACH_WRITE_TO(uint32_t, buf + len, TABLE_METADATA_MAGIC_NUM);
  len += sizeof(uint32_t);
  //write the table id
  MACH_WRITE_TO(table_id_t, buf + len, table_id_);
  len += sizeof(table_id_t);
  //write the table name
  uint32_t table_name_len = table_name_.length();
  MACH_WRITE_TO(uint32_t, buf + len, table_name_len);
  len += sizeof(uint32_t);
  memcpy(buf + len, table_name_.c_str(), table_name_len);
  len += table_name_len;
  //write the root page id
  MACH_WRITE_TO(page_id_t, buf + len, root_page_id_);
  len += sizeof(page_id_t);
  //write the schema  
  schema_->SerializeTo(buf + len);
  return GetSerializedSize();
}

uint32_t TableMetadata::GetSerializedSize() const {
  return  sizeof(uint32_t) + //magic number
          sizeof(table_id_t) + //table id
          sizeof(uint32_t) + //table name length
          table_name_.length()+ //table name
          sizeof(page_id_t) + //root page id
          schema_->GetSerializedSize(); //schema
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  if (!buf) {
    table_meta = nullptr;
    return 0;
  }
  if (table_meta != nullptr) {
    LOG(INFO)<<"table_meta is not null"<<endl;
    table_meta = nullptr;
    return 0;
  }
  uint32_t magic_num = MACH_READ_FROM(uint32_t, buf);
  if(magic_num != TABLE_METADATA_MAGIC_NUM){
    table_meta = nullptr;
    return sizeof(uint32_t);
  } //error
  //read the table id
  uint32_t len = sizeof(uint32_t);
  table_id_t table_id = MACH_READ_FROM(table_id_t, buf + len);
  len += sizeof(table_id_t);
  //read the table name
  //get length of table name
  uint32_t table_name_len = MACH_READ_FROM(uint32_t, buf + len);
  len += sizeof(uint32_t);

  char table_name_buf[table_name_len + 1];
  memcpy(table_name_buf, buf + len, table_name_len);
  table_name_buf[table_name_len] = '\0';
  len += table_name_len;
  std::string table_name(table_name_buf,table_name_len);
  //read the root page id
  page_id_t root_page_id = MACH_READ_FROM(table_id_t, buf + len);
  len += sizeof(page_id_t);
  //read the schema
  Schema *schema = nullptr;
  len += schema->DeserializeFrom(buf + len, schema, heap);
  //create the table metadata
  table_meta = ALLOC_P(heap, TableMetadata)(table_id, table_name, root_page_id, schema);
  return len;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
