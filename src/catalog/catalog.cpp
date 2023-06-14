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
  /* init */
  if(init){
        catalog_meta_ = new CatalogMeta();
        //        lock_manager_ = nullptr;
        //        log_manager_ = nullptr;
        buffer_pool_manager_ = buffer_pool_manager;
        next_table_id_ = catalog_meta_->GetNextTableId();
        next_index_id_ = catalog_meta_->GetNextIndexId();
        InitCatalogMetaPage();
  }
  /* no init */
  else {
        if (buffer_pool_manager->IsPageFree(CATALOG_META_PAGE_ID) ||
            buffer_pool_manager->IsPageFree(INDEX_ROOTS_PAGE_ID)) {
          exit(1);
        }
        //Fetch the catalog meta page and deserialize it
        Page *catalog_meta_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
        //        Page *index_meta_page = buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID);
        catalog_meta_ = catalog_meta_->DeserializeFrom(catalog_meta_page->GetData());
        next_table_id_ = catalog_meta_->GetNextTableId();
        next_index_id_ = catalog_meta_->GetNextIndexId();
        buffer_pool_manager_ = buffer_pool_manager;
        buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

        //load the tables and indexes
        for (auto &iter: catalog_meta_->table_meta_pages_) {
          dberr_t result = LoadTable(iter.first, iter.second);
          if(result != DB_SUCCESS){
            exit(1);
          }
        }
        for(auto &iter: catalog_meta_->index_meta_pages_) {
          dberr_t result = LoadIndex(iter.first, iter.second);
          if(result != DB_SUCCESS){
            exit(1);
          }
        }
  }
}

CatalogManager::~CatalogManager() {
  /** After you finish the code for the CatalogManager section,
 *  you can uncomment the commented code. Otherwise it will affect b+tree test **/
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
        delete iter.second;
  }
  for (auto iter : indexes_) {
        delete iter.second;
  }
}

/**
* TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  //if has been created
  if(table_names_.find(table_name) != table_names_.end()){
        LOG(WARNING) << "DB_TABLE_ALREADY_EXIST";
        return DB_TABLE_ALREADY_EXIST;
  }
  //init the table_heap_root page and id
  //init the table heap and table mata data and table info
  Schema *new_schema = schema->DeepCopySchema(schema);
  TableHeap *table_heap = table_heap->Create(buffer_pool_manager_, new_schema, txn, log_manager_, lock_manager_);
  TableMetadata *table_meta_data = table_meta_data->Create(next_table_id_, table_name, table_heap->GetFirstPageId(), new_schema);
  table_info = table_info->Create();
  table_info->Init(table_meta_data, table_heap);

  //init the table meta page
  page_id_t table_meta_id;
  Page *table_meta_page = buffer_pool_manager_->NewPage(table_meta_id);
  buffer_pool_manager_->UnpinPage(table_meta_id, true);
  table_meta_data->SerializeTo(table_meta_page->GetData());

  //update the data
  tables_[next_table_id_] = table_info;
  table_names_[table_name] = next_table_id_;
  catalog_meta_->table_meta_pages_[next_table_id_] = table_meta_id;
  next_table_id_ = catalog_meta_->GetNextTableId();

  //update the catalog meta page
  FlushCatalogMetaPage();

  return DB_SUCCESS;
  //  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  // if find
  if(table_names_.find(table_name) != table_names_.end()){
        table_id_t table_id = table_names_[table_name];
        table_info = tables_[table_id];
        return DB_SUCCESS;
  }
  else{// not find
        LOG(WARNING) << "Can't find the table";
        return DB_TABLE_NOT_EXIST;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  for(auto iter: tables_){
        tables.push_back(iter.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  //if not find the table
  if(table_names_.find(table_name) == table_names_.end()){
        return DB_TABLE_NOT_EXIST;
  }// if not find the index
  else if(index_names_[table_name].find(index_name) != index_names_[table_name].end()){
        return DB_INDEX_ALREADY_EXIST;
  }

  //map the index_keys into tuple
  table_id_t table_id = table_names_[table_name];
  TableInfo *table_info = tables_[table_id];
  std::vector<uint32_t> key_map;
  for(const auto & index_key : index_keys){
        uint32_t index_id;
        if(table_info->GetSchema()->GetColumnIndex(index_key, index_id) == DB_SUCCESS){
          key_map.push_back(index_id);
        }
        else{
          return DB_COLUMN_NAME_NOT_EXIST;
        }
  }

  //create index meta data and index info
  index_id_t index_id = next_index_id_;
  IndexMetadata *index_meta_data = index_meta_data->Create(index_id, index_name, table_id, key_map);
  index_info = index_info->Create();
  index_info->Init(index_meta_data, table_info, buffer_pool_manager_);

  //init the index tree
  vector<Field> key_fields;
  for(auto iter = table_info->GetTableHeap()->Begin(txn); iter != table_info->GetTableHeap()->End(); iter++){
        for(auto id: key_map){
          key_fields.emplace_back(*static_cast<Row>(*iter).GetField(id));
        }
        Row index_row(key_fields);
        index_info->GetIndex()->InsertEntry(index_row, static_cast<Row>(*iter).GetRowId(), txn);
        key_fields.clear();
  }
  //init the index meta page
  page_id_t index_meta_id;
  Page *index_meta_page = buffer_pool_manager_->NewPage(index_meta_id);
  index_meta_data->SerializeTo(index_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(index_meta_id, true);

  //update the data
  index_names_[table_name][index_name] = index_id;
  indexes_[index_id] = index_info;
  catalog_meta_->index_meta_pages_[index_id] = index_meta_id;
  next_index_id_ = catalog_meta_->GetNextIndexId();

  //update the catalog meta page
  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  if(index_names_.find(table_name) != index_names_.end()){
        if(index_names_.at(table_name).find(index_name) != index_names_.at(table_name).end()){
          index_id_t index_id = index_names_.at(table_name).at(index_name);
          index_info = indexes_.at(index_id);
          return DB_SUCCESS;
        }
        else{
          return DB_INDEX_NOT_FOUND;
        }
  }
  else{
        return DB_TABLE_NOT_EXIST;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  if(index_names_.find(table_name) != index_names_.end()){
        for(auto iter: index_names_.at(table_name)){
          indexes.push_back(indexes_.at(iter.second));
        }
        return DB_SUCCESS;
  }
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");

  if(table_names_.find(table_name) == table_names_.end() || index_names_.find(table_name) == index_names_.end()){
        LOG(WARNING) << "DB_TABLE_NOT_EXIST";
        return DB_TABLE_NOT_EXIST;
  }

  //drop all indexes in the table
  while(!index_names_[table_name].empty()){
        auto iter = index_names_[table_name].begin();
        if(DropIndex(table_name, iter->first) != DB_SUCCESS){
          return DB_FAILED;
        }
  }

  // delete the table meta page
  table_id_t table_id = table_names_[table_name];
  buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[table_id]);
  //update the catalog meta data
  catalog_meta_->table_meta_pages_.erase(table_id);
  next_table_id_ = catalog_meta_->GetNextTableId();

  //update the catalog mgr
  table_names_.erase(table_name);
  delete tables_[table_id];
  tables_.erase(table_id);

  //update the catalog meta page
  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  if(index_names_.find(table_name) == index_names_.end()){
        LOG(WARNING) << "DB_TABLE_NOT_EXIST";
        return DB_TABLE_NOT_EXIST;
  }
  else if (index_names_[table_name].find(index_name) == index_names_[table_name].end()){
        LOG(WARNING) << "DB_INDEX_NOT_FOUND";
        return DB_INDEX_NOT_FOUND;
  }

  //delete the catalog meta data about the index
  index_id_t index_id = index_names_[table_name][index_name];
  buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_id]);
  catalog_meta_->index_meta_pages_.erase(index_id);
  next_index_id_ = catalog_meta_->GetNextIndexId();

  //update the catalog mgr
  index_names_[table_name].erase(index_name);
  delete indexes_[index_id];
  indexes_.erase(index_id);

  //update the catalog meta page
  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  if(catalog_meta_page == nullptr){
        return DB_FAILED;
  }
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  if(!buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)){
        return DB_FAILED;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");

  Page *table_meta_page = buffer_pool_manager_->FetchPage(page_id);
  buffer_pool_manager_->UnpinPage(page_id, false);

  TableMetadata *table_meta_data = nullptr;
  table_meta_data->DeserializeFrom(table_meta_page->GetData(), table_meta_data);
  if(table_id != table_meta_data->GetTableId()) {
        LOG(WARNING) << "Catalog Manager Init Wrong!";
        return DB_FAILED;
  }
  TableHeap *table_heap = table_heap->Create(buffer_pool_manager_, table_meta_data->GetFirstPageId(), table_meta_data->GetSchema(),
                                             nullptr, nullptr);
  TableInfo *table_info = table_info->Create();
  table_info->Init(table_meta_data, table_heap);
  table_names_[table_meta_data->GetTableName()] = table_id;
  tables_[table_id] = table_info;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  Page *index_meta_page = buffer_pool_manager_->FetchPage(page_id);
  buffer_pool_manager_->UnpinPage(page_id, false);
  IndexMetadata *index_meta_data = nullptr;
  index_meta_data->DeserializeFrom(index_meta_page->GetData(), index_meta_data);
  if(index_id != index_meta_data->GetIndexId()){
        LOG(ERROR) << "Catalog Manager Init Wrong!";
        return DB_FAILED;
  }
  if(tables_.find(index_meta_data->GetTableId()) != tables_.end()){
        auto table = tables_.find(index_meta_data->GetTableId());
        IndexInfo *index_info = index_info->Create();
        index_info->Init(index_meta_data, table->second, buffer_pool_manager_);

        //init the index tree
        vector<Field> key_fields;
        for(auto iter = table->second->GetTableHeap()->Begin(nullptr); iter != table->second->GetTableHeap()->End(); iter++){
          for(auto id: index_meta_data->GetKeyMapping()){
            key_fields.emplace_back(*static_cast<Row>(*iter).GetField(id));
          }
          Row index_row(key_fields);
          index_info->GetIndex()->InsertEntry(index_row, static_cast<Row>(*iter).GetRowId(), nullptr);
          key_fields.clear();
        }

        index_names_[table->second->GetTableName()][index_meta_data->GetIndexName()] = index_id;
        indexes_[index_id] = index_info;
  }
  else{
        LOG(WARNING) << "Can't find the table_id";
        return DB_TABLE_NOT_EXIST;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if(tables_.find(table_id) != tables_.end()){
        table_info = tables_[table_id];
        return  DB_SUCCESS;
  }
  else{
        LOG(WARNING) << "DB_TABLE_NOT_EXIST";
        return DB_TABLE_NOT_EXIST;
  }
}