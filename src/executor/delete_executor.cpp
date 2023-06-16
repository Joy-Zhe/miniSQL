#include "executor/executors/delete_executor.h"

/**
* TODO: Student Implement
*/

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row update_row{};
  RowId update_rowid{};
  if(child_executor_->Next(&update_row,&update_rowid))
  {
    TableInfo *table_info = nullptr;//find table information
    if(exec_ctx_->GetCatalog()->GetTable(plan_->table_name_,table_info) != DB_SUCCESS)//fail to get tuple
      return false;
    vector<IndexInfo *> indexes;//find if duplicate key exists
    if(exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(),indexes) == DB_SUCCESS)
    {
      for(auto index: indexes){
        vector<uint32_t>column_ids;
        vector<Column *>index_columns = index->GetIndexKeySchema()->GetColumns();
        //init the column id in the original table
        for(auto index_column: index_columns){
          uint32_t index_column_id;
          if (table_info->GetSchema()->GetColumnIndex(index_column->GetName(), index_column_id) == DB_SUCCESS){
            column_ids.emplace_back(index_column_id);
          }
        }
        vector<Field> fields;//init the fields for index
        for(auto id: column_ids){
          fields.emplace_back(*update_row.GetField(id));
        }
        Row index_row(fields);
        vector<RowId> temp_result;
        index->GetIndex()->RemoveEntry(index_row, update_rowid, exec_ctx_->GetTransaction());
      }
    }
    table_info->GetTableHeap()->MarkDelete(update_rowid, exec_ctx_->GetTransaction());
    table_info->GetTableHeap()->ApplyDelete(update_rowid, exec_ctx_->GetTransaction());
    return true;
  }
  return false;
}