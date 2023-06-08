#include "record/row.h"
#define ROW_MAGIC_NUM 10117//jy added
/**
 * TODO: Student Implement
 */
//uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
//  ASSERT(schema != nullptr, "Invalid schema before serialize.");
//  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
//  uint32_t ofs = 0;
//  MACH_WRITE_TO(uint32_t, buf, fields_nums);
//  ofs += sizeof(uint32_t);
//  MACH_WRITE_TO(uint32_t, buf + ofs, null_nums);
//  ofs += sizeof(uint32_t);
//  for (uint32_t i = 0; i < fields_.size(); i++) {
//    if (fields_[i]->IsNull()) {
//      MACH_WRITE_INT32(buf + ofs, i);
//      ofs += sizeof(uint32_t);
//    }
//  }
//  for (auto &itr : fields_) {
//    if (!itr->IsNull()) ofs += itr->SerializeTo(buf + ofs);
//  }
//  return ofs;
//}
//
//uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
//  ASSERT(schema != nullptr, "Invalid schema before serialize.");
//  ASSERT(fields_.empty(), "Non empty field in row.");
//  //jy 5.22
//  uint32_t ofs = 0, i = 0;
//  fields_nums = MACH_READ_UINT32(buf);
//  ofs += sizeof(uint32_t);
//  null_nums = MACH_READ_UINT32(buf + ofs);
//  ofs += sizeof(uint32_t);
//  std::vector<uint32_t> null_bitmap(fields_nums, 0);
//  for (i = 0; i < null_nums; i++) {
//    null_bitmap[MACH_READ_UINT32(buf + ofs)] = 1;
//    ofs += sizeof(uint32_t);
//  }
//  for (i = 0; i < fields_nums; i++) {
//    fields_.push_back(ALLOC_P(heap_, Field)(schema->GetColumn(i)->GetType()));
//    if (!null_bitmap[i]) {
//      ofs += Field::DeserializeFrom(buf + ofs, schema->GetColumn(i)->GetType(), &fields_[i], false, heap_);
//    }
//  }
//  return ofs;
//}
//
//uint32_t Row::GetSerializedSize(Schema *schema) const {
//  ASSERT(schema != nullptr, "Invalid schema before serialize.");
//  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
//  if (fields_.empty())
//    return 0;
//  else if (fields_nums == null_nums)
//    return sizeof(uint32_t) * (2 + null_nums);
//  else {
//    uint32_t ofs = 0;
//    for (auto &itr : fields_) {
//      if (!itr->IsNull()) ofs += itr->GetSerializedSize();
//    }
//    return ofs + sizeof(uint32_t) * (2 + null_nums);
//  }
//}
//
//void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
//  auto columns = key_schema->GetColumns();
//  std::vector<Field> fields;
//  uint32_t idx;
//  for (auto column : columns) {
//    schema->GetColumnIndex(column->GetName(), idx);
//    fields.emplace_back(*this->GetField(idx));
//  }
//  key_row = Row(fields);
//}
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  if(this == nullptr) return 0;
  uint32_t num=fields_.size(),ofs=0;
  uint32_t psize=num/8;
  if(num%8) psize++;
  char p[psize+1];
  MACH_WRITE_TO(uint32_t,buf+ofs,ROW_MAGIC_NUM); ofs+=sizeof(uint32_t);
  memset(p,0,psize);
  for(int i=0;i<num;i++) if(fields_[i]->IsNull()) p[i/8]|=(1<<(7-i%8));
  MACH_WRITE_TO(uint32_t, buf+ofs, num); ofs+=sizeof(uint32_t);
  memcpy(buf+ofs, p, psize); ofs+=psize;
  for(int i=0;i<num;i++) ofs+=fields_[i]->SerializeTo(buf+ofs);
  return ofs;
//   uint32_t ofs = 0;
//   MACH_WRITE_TO(uint32_t, buf, fields_nums);
//   ofs += sizeof(uint32_t);
//   MACH_WRITE_TO(uint32_t, buf + ofs, null_nums);
//   ofs += sizeof(uint32_t);
//   for (uint32_t i = 0; i < fields_.size(); i++) {
//     if (fields_[i]->IsNull()) {
//       MACH_WRITE_INT32(buf + ofs, i);
//       ofs += sizeof(uint32_t);
//     }
//   }
//   for (auto &itr : fields_) {
//     if (!itr->IsNull()) ofs += itr->SerializeTo(buf + ofs);
//   }
//   return ofs;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t num=0,ofs=0,row_magic_num;//RowId rid;
  row_magic_num= MACH_READ_FROM(uint32_t,buf+ofs); ofs+=sizeof(uint32_t);
  ASSERT(row_magic_num==10117,"Error!");
  num=MACH_READ_FROM(uint32_t,buf+ofs); ofs+=sizeof(uint32_t);
  uint32_t psize=num/8;
  if(num%8) psize++;
  char p[psize+1];
  memcpy(p,buf+ofs,psize); ofs+=psize;
  for(int i=0;i<num;i++){
    fields_.emplace_back(nullptr);
    if(p[i/8]&(1<<(7-i%8))) ofs+=Field::DeserializeFrom(buf+ofs,schema->GetColumn(i)->GetType(),&(fields_[i]),true);
    else ofs+=Field::DeserializeFrom(buf+ofs,schema->GetColumn(i)->GetType(),&(fields_[i]),false);
  }
  return ofs;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  if(this== nullptr) return 0;
  uint32_t ofs=0;ofs+=sizeof(uint32_t);
  for(int i=0;i<fields_.size();i++) ofs+=fields_[i]->GetSerializedSize();
  ofs+=fields_.size()/8;
  if(fields_.size()%8) ofs++;
  return ofs+sizeof(uint32_t);
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}

