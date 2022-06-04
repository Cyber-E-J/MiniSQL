#include "record/column.h"
#include "record/schema.h"
#include <iostream>
Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

//按魔数 列名长度 column_name, type, col_ind, nullable, unique顺序序列化
uint32_t Column::SerializeTo(char *buf) const {
  
    uint32_t len = 0;
    MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM );//加入魔数
    len += sizeof(uint32_t);
    MACH_WRITE_UINT32(buf+len,name_.length());//加入列名长度
    len += sizeof(uint32_t);
    MACH_WRITE_STRING(buf+len, name_ );//加入列名
    len += name_.length();
    MACH_WRITE_TO(TypeId,buf+len, type_ );//加入type
    len += sizeof(TypeId);
    MACH_WRITE_UINT32(buf+len, table_ind_ );//加入col_ind_
    len += sizeof(uint32_t);
    MACH_WRITE_UINT32(buf+len, len_ );//加入len_
    len += sizeof(uint32_t);
    MACH_WRITE_TO(bool,buf+len, nullable_);//加入nullable
    len += sizeof(bool);
    MACH_WRITE_TO(bool,buf+len, unique_);//加入unique
    len += sizeof(bool);
    return len;
}

uint32_t Column::GetSerializedSize() const{
  uint32_t len = sizeof(uint32_t) * 4 + sizeof(bool) * 2 + sizeof(TypeId) + this->name_.length();
  return len;
}

uint32_t Column::DeserializeFrom(char *buf,Column *&column,MemHeap *heap){
  if (column!= nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize." << std::endl;
  }
  void *mem = heap->Allocate(sizeof(Column));
  uint32_t len = 0;
  uint32_t MAGIC_NUM = MACH_READ_FROM(uint32_t,buf);//取出魔数
  ASSERT(MAGIC_NUM == COLUMN_MAGIC_NUM, "index_meta Magic Number Not Equal!");
  len += sizeof(uint32_t);
  uint32_t column_name_len = MACH_READ_UINT32(buf + len);//取出列名长度
  len += sizeof(uint32_t);
  char *column_name = new char[column_name_len + 1];
  column_name[column_name_len] = 0;
  memcpy(column_name, buf + len, column_name_len);  //取出列名
  len += column_name_len;
  TypeId type = MACH_READ_FROM(TypeId, buf + len);//取出type
  len += sizeof(TypeId);
  uint32_t col_ind=MACH_READ_UINT32(buf + len); //取出colind
  len+=sizeof(uint32_t);
  uint32_t _len=MACH_READ_UINT32(buf+len);//取出len
  len += sizeof(uint32_t);
  bool nullable=MACH_READ_FROM(bool,buf+len); //取出nullable
  len+=sizeof(bool);
  bool unique=MACH_READ_FROM(bool,buf+len); //取出nullable
  len+=sizeof(bool);
  if(type== kTypeChar){
    column = new(mem)Column(column_name, type,_len, col_ind, nullable, unique);
  }else{
    column = new(mem)Column(column_name, type, col_ind, nullable, unique);
  }
  
  return len; 
}