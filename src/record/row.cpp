#include "record/row.h"
#include<map>
#include<iostream>
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  uint32_t len = 0;
  for (size_t i = 0; i < fields_.size(); i++) {  //加入域
    len += fields_[i]->SerializeTo(buf + len);
  }
  return len;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  uint32_t len = 0;
  for (auto cur_col:schema->GetColumns()) {
    TypeId type_col = cur_col->GetType();
    Field* this_field;
    len+=this_field->DeserializeFrom(buf+len, type_col, &this_field, false, this->heap_);
    fields_.push_back(this_field);
  }
  return len; 
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  uint32_t len = 0;
  for (size_t i = 0; i < fields_.size(); i++) {  //加入域
    len += fields_[i]->GetSerializedSize();
  }
  return len;
}

