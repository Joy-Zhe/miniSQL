#include "catalog/indexes.h"

IndexMetadata::IndexMetadata(const index_id_t index_id, const std::string &index_name, const table_id_t table_id,
                             const std::vector<uint32_t> &key_map, const std::string &index_type)
    : index_id_(index_id), index_name_(index_name), table_id_(table_id), key_map_(key_map), index_type_(index_type) {}

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name, const table_id_t table_id,
                                     const vector<uint32_t> &key_map, const string &index_type) {
  return new IndexMetadata(index_id, index_name, table_id, key_map, index_type);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
    char *p = buf;
    uint32_t ofs = GetSerializedSize();
    ASSERT(ofs <= PAGE_SIZE, "Failed to serialize index info.");
    // magic num
    MACH_WRITE_UINT32(buf, INDEX_METADATA_MAGIC_NUM);
    buf += 4;
    // index id
    MACH_WRITE_TO(index_id_t, buf, index_id_);
    buf += 4;
    // index name
    MACH_WRITE_UINT32(buf, index_name_.length());
    buf += 4;
    MACH_WRITE_STRING(buf, index_name_);
    buf += index_name_.length();
    // table id
    MACH_WRITE_TO(table_id_t, buf, table_id_);
    buf += 4;
    // key count
    MACH_WRITE_UINT32(buf, key_map_.size());
    buf += 4;
    // key mapping in table
    for (auto &col_index : key_map_) {
        MACH_WRITE_UINT32(buf, col_index);
        buf += 4;
    }
    // index type
    MACH_WRITE_UINT32(buf, index_type_.length());
    buf += 4;
    MACH_WRITE_STRING(buf, index_type_);
    buf += index_type_.length();

    ASSERT(buf - p == ofs, "Unexpected serialize size.");
    return ofs;
}

/**
 * TODO: Student Implement
 */
uint32_t IndexMetadata::GetSerializedSize() const {
    /* the size of INDEX_METADATA_MAGIC_NUM, index_id_, index_name_.length(), index_name_, table_id_ and key_map_,size() and key_map_*/
    uint32_t len = index_name_.length() + index_type_.length() + key_map_.size() * sizeof(uint32_t);

    return 6 * sizeof(uint32_t) + len;
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta) {
    if (index_meta != nullptr) {
      //  LOG(WARNING) << "Pointer object index info is not null in table info deserialize." << std::endl;
    }
    char *p = buf;
    // magic num
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == INDEX_METADATA_MAGIC_NUM, "Failed to deserialize index info.");
    // index id
    index_id_t index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    // index name
    uint32_t len = MACH_READ_UINT32(buf);
    buf += 4;
    std::string index_name(buf, len);
    buf += len;
    // table id
    table_id_t table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    // index key count
    uint32_t index_key_count = MACH_READ_UINT32(buf);
    buf += 4;
    // key mapping in table
    std::vector<uint32_t> key_map;
    for (uint32_t i = 0; i < index_key_count; i++) {
        uint32_t key_index = MACH_READ_UINT32(buf);
        buf += 4;
        key_map.push_back(key_index);
    }
    // index type
    len = MACH_READ_UINT32(buf);
    buf += 4;
    std::string index_type(buf, len);
    buf += len;
    // allocate space for index meta data
    index_meta = new IndexMetadata(index_id, index_name, table_id, key_map, index_type);
    return buf - p;
}

Index *IndexInfo::CreateIndex(BufferPoolManager *buffer_pool_manager, const string &index_type) {
  size_t max_size = 0;
  for (auto col : key_schema_->GetColumns()) {
    max_size += col->GetLength();
  }

  if (index_type == "bptree") {
    if (max_size <= 8)
      max_size = 16;
    else if (max_size <= 24)
      max_size = 32;
    else if (max_size <= 56)
      max_size = 64;
    else if (max_size <= 120)
      max_size = 128;
    else if (max_size <= 248)
      max_size = 256;
    else {
     // LOG(ERROR) << "GenericKey size is too large";
      return nullptr;
    }
  } else {
    return nullptr;
  }
  return new BPlusTreeIndex(meta_data_->index_id_, key_schema_, max_size, buffer_pool_manager);
}
