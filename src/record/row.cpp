#include "record/row.h"
#include<map>
#include<iostream>
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  uint32_t len = 0;
  MACH_WRITE_TO(RowId,buf,rid_);//加入rid
  len += sizeof(RowId);
  MACH_WRITE_TO(size_t, buf + len, (size_t)fields_.size());  //加入域的数组大小
  len += sizeof(size_t);
  if (fields_.size() == 0) return len;//如果为空可以直接返回啦
  size_t offset = 2;
  size_t size = fields_.size();
  size_t bitmap_bytes = (size + 7 + offset) / 8;
  std::vector<int> nulls(bitmap_bytes, 0);
  
  for (size_t i = 0; i < fields_.size(); i++) {  //加入域
    if(fields_[i]->IsNull()){
      nulls[(i + offset) / 8] |= 1 << ((i + offset) % 8);
    }
  }
  MACH_WRITE_TO(std::vector<int>, buf + len, nulls);  //加入null bitmap的数组
  len += sizeof(nulls);
  for (size_t i = 0; i < fields_.size(); i++) {  //加入域
    len += fields_[i]->SerializeTo(buf + len);
  }
  return len;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
     uint32_t len = 0;
     rid_= MACH_READ_FROM(RowId,buf+len);
     len+=sizeof(RowId);
     size_t size = MACH_READ_FROM(size_t,buf+len);
     len+=sizeof(size_t);
     size_t offset = 2;
     size_t bitmap_bytes = (size + 7 + offset) / 8;
     std::vector<int> nulls(bitmap_bytes, 0);
     nulls= MACH_READ_FROM(std::vector<int>, buf + len);
     len += sizeof(nulls);
     uint32_t byte, bit, index;
     std::map<u_int32_t, bool> color;
     for (byte = 0; byte <= (size + offset) / 8; byte++) {
       for (bit = 0; bit < 8; bit++){
         index = 8 * byte + bit;
         if (index <= 1) continue;
         else{
           index -= offset;
           if(nulls[index]>=1<<bit){
             color[index] = true;
             nulls[index] -= 1 << bit;
           }
           else{
             color[index] = false;
           }
         }
       }
     }

     uint32_t i = 0;
     for (auto cur_col:schema->GetColumns()) {
       TypeId type_col = cur_col->GetType();
       Field* this_field;
       len+=this_field->DeserializeFrom(buf+len, type_col, &this_field, color[i], this->heap_);
       fields_.push_back(this_field);
     }

       return len; 
       }

uint32_t Row::GetSerializedSize(Schema *schema) const {
  size_t size = fields_.size();
  if (size == 0) return sizeof(RowId) + sizeof(size_t);
  size_t offset = 2;
  size_t bitmap_bytes = (size + 7 + offset) / 8;
  std::vector<int> tmp(bitmap_bytes, 0);
  uint32_t len = sizeof(RowId) + sizeof(size_t) + sizeof(tmp);
  for (size_t i = 0; i < fields_.size(); i++) {  //加入域
    len += fields_[i]->GetSerializedSize();
  }
  return len;
}

