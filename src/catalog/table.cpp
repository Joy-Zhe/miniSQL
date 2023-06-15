#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
    char *p = buf;
    uint32_t ofs = GetSerializedSize();
    ASSERT(ofs <= PAGE_SIZE, "Failed to serialize table info.");
    // magic num
    MACH_WRITE_UINT32(buf, TABLE_METADATA_MAGIC_NUM);
    buf += 4;
    // table id
    MACH_WRITE_TO(table_id_t, buf, table_id_);
    buf += 4;
    // table name
    MACH_WRITE_UINT32(buf, table_name_.length());
    buf += 4;
    MACH_WRITE_STRING(buf, table_name_);
    buf += table_name_.length();
    // table heap root page id
    MACH_WRITE_TO(page_id_t, buf, root_page_id_);
    buf += 4;
    // table schema
    buf += schema_->SerializeTo(buf);
    // primary key
    MACH_WRITE_UINT32(buf, primary_key_.size());
    buf += 4;
    for(const auto& iter: primary_key_){
        MACH_WRITE_UINT32(buf, iter.length());
        buf += 4;
        MACH_WRITE_STRING(buf, iter);
        buf += iter.length();
    }
    ASSERT(buf - p == ofs, "Unexpected serialize size.");
    return ofs;
}


/**
 * TODO: Student Implement
 */
uint32_t TableMetadata::GetSerializedSize() const {
    /* the size of TABLE_METADATA_MAGIC_NUM, table_id_, table_name_.length(), table_name_, root_page_id_ and schema_*/
    uint32_t len = table_name_.length() + schema_->GetSerializedSize();
    for(const auto& iter: primary_key_){
        len += iter.length();
        len += sizeof(uint32_t);
    }
    len += sizeof(uint32_t);
    return  4 * sizeof(uint32_t) + len;
}

uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta) {
    if (table_meta != nullptr) {
      //  LOG(WARNING) << "Pointer object table info is not null in table info deserialize." << std::endl;
    }
    char *p = buf;
    // magic num
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == TABLE_METADATA_MAGIC_NUM, "Failed to deserialize table info.");
    // table id
    table_id_t table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    // table name
    uint32_t len = MACH_READ_UINT32(buf);
    buf += 4;
    std::string table_name(buf, len);
    buf += len;
    // table heap root page id
    page_id_t root_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    // table schema
    TableSchema *schema = nullptr;
    buf += TableSchema::DeserializeFrom(buf, schema);
    // primary key
    uint32_t key_nums = MACH_READ_UINT32(buf);
    buf += 4;
    std::vector<std::string> primary_key;
    for(uint32_t i=0; i < key_nums; i++){
        uint32_t key_length = MACH_READ_UINT32(buf);
        buf += 4;
        std::string key_name(buf, key_length);
        buf += key_length;
        primary_key.push_back(key_name);
    }
    // allocate space for table metadata
    table_meta = new TableMetadata(table_id, table_name, root_page_id, schema);
    table_meta->primary_key_ = primary_key;
    return buf - p;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name, page_id_t root_page_id,
                                     TableSchema *schema) {
  // allocate space for table metadata
  return new TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
    : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema){}
