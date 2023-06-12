#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t ofs = 0;
  MACH_WRITE_TO(uint32_t, buf, SCHEMA_MAGIC_NUM);
  ofs += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf + ofs, columns_.size());
  ofs += sizeof(uint32_t);
  for (auto &itr : columns_) {
    ofs += itr->SerializeTo(buf + ofs);
  }
  return ofs;
  //return 0;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t size = 0;
  for (auto &itr : columns_) {
    size += itr->GetSerializedSize();
  }
  return size + 2 * sizeof(uint32_t);
  //return 0;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  auto num = MACH_READ_FROM(uint32_t, buf);
  ASSERT(num == Schema::SCHEMA_MAGIC_NUM, "Schema magic num error.");
  uint32_t ofs = sizeof(uint32_t);
  auto col_size = MACH_READ_UINT32(buf + ofs);
  ofs += sizeof(uint32_t);
  std::vector<Column *> columns;
  for (auto i = 0u; i < col_size; i++) {
    Column *col;
    ofs += Column::DeserializeFrom(buf + ofs, col);
    columns.push_back(col);
  }
  //schema = ALLOC_P(heap, Schema)(columns);
  return ofs;
  //return 0;
}