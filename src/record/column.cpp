#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t ofs = 0, len;

  //write magic num
  MACH_WRITE_TO(uint32_t, buf, COLUMN_MAGIC_NUM);
  ofs += sizeof(uint32_t);
  //end write magic num

//  len = name_.size() * sizeof(char);  // bytes of string
//  memcpy(buf + ofs, &len, sizeof(uint32_t));
//  ofs += sizeof(uint32_t);
//  memcpy(buf + ofs, name_.c_str(), len);
//  ofs += len;


  //write len_,table_ind_,nullable_,unique_,type_
  MACH_WRITE_TO(uint32_t, buf + ofs, len_);
  ofs += sizeof(uint32_t);
  MACH_WRITE_TO(uint32_t, buf + ofs, table_ind_);
  ofs += sizeof(uint32_t);
  MACH_WRITE_TO(bool, buf + ofs, nullable_);
  ofs += sizeof(bool);
  MACH_WRITE_TO(bool, buf + ofs, unique_);
  ofs += sizeof(bool);
  MACH_WRITE_TO(TypeId, buf + ofs, type_);
  ofs += sizeof(TypeId);
  //end write len_,table_ind_,nullable_,unique_,type_

  //write name.length
  MACH_WRITE_UINT32(buf + ofs, name_.length());
  ofs += sizeof(uint32_t);
  //end write name.length

  //write name
  MACH_WRITE_STRING(buf + ofs, name_);
  ofs += name_.length();
  //end write name

  return ofs;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  //return 4 * sizeof(uint32_t) + name_.size() * sizeof(char) + sizeof(TypeId) + 2 * sizeof(bool);
    return 4 * sizeof(uint32_t) + sizeof(TypeId) + 2 * sizeof(bool) + name_.length();
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t ofs = 0;

  //read magic num
  uint32_t magic_num = MACH_READ_FROM(uint32_t, buf);
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "Column magic num error.");
  ofs += sizeof(uint32_t);
  //end read magic num

//  auto len = MACH_READ_UINT32(buf + ofs);
//  ofs += sizeof(uint32_t);
//  char tmp[len / sizeof(char) + 1];
//  memset(tmp, '\0', sizeof(tmp));
//  memcpy(tmp, buf + ofs, len);
//  auto name_(tmp);
//  ofs += len;


  //read len_,table_ind_,nullable_,unique_,type_
  uint32_t len_read = MACH_READ_FROM(TypeId, buf + ofs);
  ofs += sizeof(uint32_t);
  uint32_t table_ind_read = MACH_READ_FROM(uint32_t, buf + ofs);
  ofs += sizeof(uint32_t);
  bool nullable_read = MACH_READ_FROM(bool, buf + ofs);
  ofs += sizeof(bool);
  bool unique_read = MACH_READ_FROM(bool, buf + ofs);
  ofs += sizeof(bool);
  TypeId type_read = MACH_READ_FROM(TypeId, buf + ofs);
  ofs += sizeof(TypeId);
  //end read len_,table_ind_,nullable_,unique_,type_

  //read name.length
  uint32_t name_length_read = MACH_READ_UINT32(buf + ofs);
  ofs += sizeof(uint32_t);
  //end read name.length

  //read name
  char *temp = new char[name_length_read]();
  memcpy(temp, buf + ofs, name_length_read);
  std::string column_name(temp);
  ofs = ofs + name_length_read;
  //end read name

  if (type_read != kTypeChar)  // not char type
    column = new Column(column_name, type_read, table_ind_read, nullable_read, unique_read);
  else
    column = new Column(column_name, type_read, len_read, table_ind_read, nullable_read, unique_read);
  return ofs;

}
