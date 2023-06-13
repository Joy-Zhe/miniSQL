#include "catalog/catalog.h"

#include <utility>

void CatalogMeta::SerializeTo(char *buf) const {
    // ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    // ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  return 4 + 4 + 4 +
    8 * table_meta_pages_.size() +
    8 * index_meta_pages_.size();
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if(init)
  {
        catalog_meta_ = CatalogMeta::NewInstance();
        next_table_id_ = 0;
        next_index_id_ = 0;
  }
  else
  {
        Page* catalog_meta_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
        catalog_meta_ = CatalogMeta::DeserializeFrom(catalog_meta_page->GetData());
        for(auto itr : catalog_meta_->table_meta_pages_)
        {
          if(itr.second != INVALID_PAGE_ID)
            LoadTable(itr.first, itr.second);
        }
        for(auto itr : catalog_meta_->index_meta_pages_)
        {
          if(itr.second != INVALID_PAGE_ID)
            LoadIndex(itr.first, itr.second);
        }
        next_table_id_ = catalog_meta_->GetNextTableId();
        next_index_id_ = catalog_meta_->GetNextIndexId();
        buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  }
}

CatalogManager::~CatalogManager() {
 /** After you finish the code for the CatalogManager section,
 *  you can uncomment the commented code. Otherwise it will affect b+tree test
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
  **/
}

/**
* TODO: Student Implement
*/
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  page_id_t new_table_page_id;
  table_id_t new_table_id = next_table_id_;
  if(table_names_.find(table_name) != table_names_.end())
        return DB_TABLE_ALREADY_EXIST;
  auto new_page = buffer_pool_manager_->NewPage(new_table_page_id);
  if(new_page == nullptr)
        return DB_FAILED;

  catalog_meta_->table_meta_pages_[new_table_id] = new_table_page_id;
  catalog_meta_->table_meta_pages_[new_table_id + 1] = INVALID_PAGE_ID;
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  table_info = TableInfo::Create();
  Schema* new_schema = Schema::DeepCopySchema(schema);
  auto table_heap = TableHeap::Create(buffer_pool_manager_, new_schema, txn, log_manager_, lock_manager_);
  auto table_metadata =  TableMetadata::Create(new_table_id, table_name, table_heap->GetFirstPageId(), new_schema);
  table_metadata->SerializeTo(new_page->GetData());
  table_info->Init(table_metadata, table_heap);
  buffer_pool_manager_->UnpinPage(new_table_page_id, true);

  next_table_id_++;
  table_names_.emplace(table_name, new_table_id);
  tables_.emplace(new_table_id, table_info);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if(table_names_.find(table_name) == table_names_.end())
        return DB_TABLE_NOT_EXIST;
  return GetTable(table_names_[table_name], table_info);
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if(tables_.empty())
        return DB_FAILED;
  for(auto itr : tables_)
  {
        tables.emplace_back(itr.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  if(table_names_.find(table_name) == table_names_.end())
        return DB_TABLE_NOT_EXIST;
  vector<uint32_t> key_map;
  for(const auto& column_name : index_keys)
  {
        bool is_exist = false;
        for(auto column : tables_[table_names_[table_name]]->GetSchema()->GetColumns())
        {
          if(column->GetName() == column_name)
          {
            is_exist = true;
            break;
          }
        }
        if(!is_exist)
          return DB_COLUMN_NAME_NOT_EXIST;
  }
  for(auto column : tables_[table_names_[table_name]]->GetSchema()->GetColumns())
  {
        if (std::find(index_keys.begin(), index_keys.end(), column->GetName()) != index_keys.end())
          key_map.emplace_back(column->GetTableInd());
  }
  return CreateIndex(table_name, index_name, key_map, txn, index_info, index_type);
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const vector<uint32_t>& key_map, Transaction *txn,
                                    IndexInfo *&index_info, const string& index_type) {
  if(table_names_.find(table_name) == table_names_.end())
        return DB_TABLE_NOT_EXIST;
  page_id_t new_index_page_id;
  index_id_t new_index_id = next_index_id_;
  if(index_names_[table_name].find(index_name) != index_names_[table_name].end())
        return DB_INDEX_ALREADY_EXIST;
  auto new_page = buffer_pool_manager_->NewPage(new_index_page_id);
  if(new_page == nullptr)
        return DB_FAILED;

  catalog_meta_->index_meta_pages_[new_index_id] = new_index_page_id;
  catalog_meta_->index_meta_pages_[new_index_id + 1] = INVALID_PAGE_ID;
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  index_info = IndexInfo::Create();

  auto index_metadata =  IndexMetadata::Create(new_index_id, index_name, table_names_[table_name], key_map);
  index_metadata->SerializeTo(new_page->GetData());
  index_info->Init(index_metadata, tables_[table_names_[table_name]], buffer_pool_manager_, index_type);
  buffer_pool_manager_->UnpinPage(new_index_page_id, true);

  next_index_id_++;
  index_names_[table_name].emplace(index_name, new_index_id);
  indexes_.emplace(new_index_id, index_info);
  auto table_heap = tables_[table_names_[table_name]]->GetTableHeap();
  for(auto iter = table_heap->Begin(txn); iter != table_heap->End(); iter++)
  {
        vector<Field> fields;
        for(auto i : key_map)
        {
          fields.emplace_back(*iter->GetFields()[i]);
        }
        index_info->GetIndex()->InsertEntry(Row(fields), iter->GetRowId(), txn);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if(index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end())
        return DB_INDEX_NOT_FOUND;
  index_info = indexes_.at(index_names_.at(table_name).at(index_name));
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if(index_names_.find(table_name) == index_names_.end() || index_names_.at(table_name).empty())
        return DB_FAILED;
  for(const auto& index : index_names_.at(table_name))
  {
        indexes.emplace_back(indexes_.at(index.second));
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if(table_names_.find(table_name) == table_names_.end())
        return DB_TABLE_NOT_EXIST;
  for(auto iter = index_names_[table_name].begin(); iter != index_names_[table_name].end();)
  {
        DropIndex(table_name, (iter++)->first);
  }
  table_id_t table_id = table_names_[table_name];
  tables_[table_id]->GetTableHeap()->DeleteTable();
  buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[table_id]);
  catalog_meta_->table_meta_pages_.erase(table_id);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  free(tables_[table_id]);
  tables_.erase(table_id);
  table_names_.erase(table_name);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if(index_names_[table_name].find(index_name) == index_names_[table_name].end())
        return DB_INDEX_NOT_FOUND;
  index_id_t index_id = index_names_[table_name][index_name];
  indexes_[index_id]->GetIndex()->Destroy();
  buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_id]);
  catalog_meta_->index_meta_pages_.erase(index_id);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  free(indexes_[index_id]);
  indexes_.erase(index_id);
  index_names_[table_name].erase(index_name);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  if(buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID))
        return DB_SUCCESS;
  else
        return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  Page *temp_page = buffer_pool_manager_->FetchPage(page_id);
  if(temp_page == nullptr)
        return DB_FAILED;
  TableInfo *table_info = TableInfo::Create();
  TableMetadata* table_mata = nullptr;
  TableMetadata::DeserializeFrom(temp_page->GetData(), table_mata);
  if(table_mata == nullptr)
        return DB_FAILED;
  TableHeap* table_heap = TableHeap::Create(buffer_pool_manager_, table_mata->GetFirstPageId(), table_mata->GetSchema(), log_manager_, lock_manager_);
  table_info->Init(table_mata, table_heap);
  table_names_.emplace(table_mata->GetTableName(), table_id);
  tables_.emplace(table_id, table_info);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  Page *temp_page = buffer_pool_manager_->FetchPage(page_id);
  if(temp_page == nullptr)
        return DB_FAILED;
  IndexInfo *index_info = IndexInfo::Create();
  IndexMetadata* index_mata = nullptr;
  IndexMetadata::DeserializeFrom(temp_page->GetData(), index_mata);
  if(index_mata == nullptr)
        return DB_FAILED;
  index_info->Init(index_mata, tables_[index_mata->GetTableId()], buffer_pool_manager_, "bptree");
  index_names_[tables_[index_mata->GetTableId()]->GetTableName()].emplace(index_mata->GetIndexName(), index_id);
  indexes_.emplace(index_id, index_info);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if(tables_.find(table_id) == tables_.end())
        return DB_FAILED;
  table_info = tables_[table_id];
  return DB_SUCCESS;
}