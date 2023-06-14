#include "record/row.h"
//#define ROW_MAGIC_NUM 10117//jy added
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
//  uint32_t ofs = 0;
//
//  //write magic num
//  MACH_WRITE_TO(uint32_t,buf+ofs,ROW_MAGIC_NUM);
//  ofs += sizeof(uint32_t);
//  //end write magic num
//
//  //write field size
//  MACH_WRITE_UINT32(buf+ofs,fields_.size());
//  ofs += sizeof(uint32_t);
//  //end write field size
//
//  uint32_t bitmap_size = (fields_.size()+7) / 8;
//  char null_bitmap[bitmap_size];//(fields_.size()+7) / 8
//  memset(null_bitmap,0, sizeof(null_bitmap));
//
//  for(int i = 0; i < fields_.size();i++)
//    if(fields_.at(i)==nullptr)//set null fields to 1
//      null_bitmap[i/8] = null_bitmap[i/8]|(1<<(i%8));
//
//  //write bitmap
//  for(int i = 0; i < bitmap_size; i++){//(fields_.size()+7)/8
//    MACH_WRITE_TO(char,buf+ofs,null_bitmap[i]);
//    ofs += sizeof(char);
//  }
//  //end write bitmap
//
//  //write fields
//  for(int i = 0; i < fields_.size();i++)
//    if(fields_.at(i)!=nullptr){
//      MACH_WRITE_TO(TypeId,buf+ofs,fields_.at(i)->GetTypeId());
//      ofs = ofs + 4;
//      ofs = ofs + fields_.at(i)->SerializeTo(buf+ofs);
//    }
//  //end write fields
//  return ofs;
  if(this == nullptr) return 0;
  uint32_t ofs = 0;
  uint32_t num = fields_.size();//get field nums
  uint32_t psize = num / 8;
  if(num % 8) psize++;
  char p[psize+1]; memset(p,0,psize);//initial null bitmap to 0
  //write magic num
  MACH_WRITE_TO(uint32_t,buf+ofs,ROW_MAGIC_NUM); ofs+=sizeof(uint32_t);
  //end write magic num
  for(int i = 0 ; i < num ; i++)
    if(fields_[i]->IsNull())
      p[i/8] |= (1<<(7-i%8));//set null bitmap
  //write field num
  MACH_WRITE_TO(uint32_t, buf+ofs, num); ofs+=sizeof(uint32_t);
  //end write field num

  //write null bitmap
  memcpy(buf+ofs, p, psize); ofs+=psize;
  //end write null bitmap
  for(int i = 0 ; i < num ; i++)
    ofs += fields_[i]->SerializeTo(buf+ofs);//if fields[i]==null, ofs+=0
  return ofs;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
//  uint32_t ofs = 0;

//  //read magic num
//  uint32_t row_magic_num_read = MACH_READ_FROM(uint32_t,buf+ofs);
//  ofs += sizeof(uint32_t);
//  //end read magic num
//  ASSERT(row_magic_num_read==ROW_MAGIC_NUM,"row magic num Error when Deserializing!");
//
//  //read field size
//  uint32_t field_size_read = MACH_READ_FROM(uint32_t,buf+ofs);
//  ofs += sizeof(uint32_t);
//  //end read field size
//
//  uint32_t null_bitmap_size = (field_size_read + 7) / 8;
//  char null_bitmap[null_bitmap_size];
//
//  //read null bitmap
//  memset(null_bitmap,0,sizeof(null_bitmap));
//  for(int i = 0; i < null_bitmap_size;i++){
//    null_bitmap[i] = MACH_READ_FROM(char,buf+ofs);
//    ofs += sizeof(char);
//  }
//  //end read null bitmap
//
//  for(int i = 0; i < field_size_read; i++){
//    bool is_null = (null_bitmap[i/8])&(1<<(i%8));
//    if(is_null)
//      fields_.push_back(nullptr);
//    else{
//      fields_.push_back(nullptr);
//      TypeId type_id = MACH_READ_FROM(TypeId,buf+ofs);
//      ofs = ofs + 4;
//      ofs = ofs + Field::DeserializeFrom(buf+ofs,type_id,&fields_.at(i),false);
//    }
//  }
//  return ofs;

  uint32_t num=0,ofs=0,row_magic_num;//RowId rid;
  //read magic num
  row_magic_num= MACH_READ_FROM(uint32_t,buf+ofs); ofs+=sizeof(uint32_t);
  //end read magic num
  ASSERT(row_magic_num==10117,"Error!");
  //read field num
  num=MACH_READ_FROM(uint32_t,buf+ofs);
  ofs+=sizeof(uint32_t);
  //end read field num
  //read null bitmap
  uint32_t psize = num / 8;
  if(num % 8) psize++;
  char p[psize+1];memset(p,0,sizeof(p));
  memcpy(p,buf+ofs,psize); ofs+=psize;
  //end read null bitmap
  for(int i=0;i<num;i++){
    fields_.emplace_back(nullptr);//set to null for next step
    if(p[i/8]&(1<<(7-i%8)))//original field is null
      ofs+=Field::DeserializeFrom(buf+ofs,schema->GetColumn(i)->GetType(),&(fields_[i]),true);
    else//original field is not null
      ofs+=Field::DeserializeFrom(buf+ofs,schema->GetColumn(i)->GetType(),&(fields_[i]),false);
  }
  return ofs;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
//  uint32_t ofs = 2*sizeof(uint32_t);//magic num + field size      //4
//  ofs += (fields_.size()+7)/8;//bitmap size
//  for(int i = 0; i < fields_.size();i++)
//    if(fields_.at(i)!=nullptr)
//      ofs += fields_.at(i)->GetSerializedSize() + 4;//fields size
//  return ofs;
  if(this== nullptr) return 0;
  uint32_t ofs = 0;
  //get magic num
  ofs += sizeof(uint32_t);
  //end get magic num

  //get field num
  ofs += sizeof(uint32_t);
  //end get field num

  for(int i = 0 ; i < fields_.size() ; i++)
    ofs += fields_[i]->GetSerializedSize();
  //get null bitmap size
  ofs += fields_.size()/8;
  if(fields_.size()%8)
    ofs++;
  //end get null bitmap size
  return ofs;//+sizeof(uint32_t)
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

