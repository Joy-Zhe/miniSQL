#include "record/schema.h"
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t ofs = 0;

  //write magic num
  MACH_WRITE_TO(uint32_t, buf, SCHEMA_MAGIC_NUM);
  ofs += sizeof(uint32_t);
  //end write magic num

  //write is_manage
  MACH_WRITE_TO(bool,buf+ofs,is_manage_);
  ofs += sizeof(bool);
  //end write is_manage

  //write column size
  MACH_WRITE_UINT32(buf + ofs, columns_.size());
  ofs += sizeof(uint32_t);
  //end write column size

  //write columns
  for (auto &itr : columns_) {
    ofs += itr->SerializeTo(buf + ofs);
  }
  //end write column

  return ofs;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = 0;
  for (auto &itr : columns_) {
    size += itr->GetSerializedSize();
  }
  return size + 2 * sizeof(uint32_t) + sizeof(bool);
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t ofs = 0;

  //read magic num
  uint32_t num = MACH_READ_FROM(uint32_t, buf);
  ASSERT(num == Schema::SCHEMA_MAGIC_NUM, "Schema magic num error.");
  ofs += sizeof(uint32_t);
  //end read magic num

  //read is_manage
  bool is_manage_read = MACH_READ_FROM(bool, buf + ofs);
  ofs += sizeof(bool);
  //end read is_manage

  //read column size
  uint32_t col_size = MACH_READ_UINT32(buf + ofs);
  ofs += sizeof(uint32_t);
  //end read column size

  //read columns
  std::vector<Column *> columns;
  for (auto i = 0u; i < col_size; i++) {
//    Column *col;
//    ofs += Column::DeserializeFrom(buf + ofs, col);
//    columns.push_back(col);
    columns.push_back(nullptr);
    ofs += Column::DeserializeFrom(buf+ofs,columns.at(i));
  }
  //end read columns

  schema = new Schema(columns,is_manage_read);//new object
  return ofs;
}