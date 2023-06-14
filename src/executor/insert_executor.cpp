//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row insert_row;
  RowId insert_rid;
  if(child_executor_->Next(&insert_row, &insert_rid))
  {
    TableInfo *table_info = nullptr;
    if(exec_ctx_->GetCatalog()->GetTable(plan_->table_name_, table_info) != DB_SUCCESS)
    {
      return false;
    }
    //find if has the duplicate key
    vector<IndexInfo *>indexes;
    if(exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(),indexes) == DB_SUCCESS)
    {
      //traverse the indexes
      for(auto index: indexes)
      {
        vector<uint32_t>column_ids;
        vector<Column *>index_columns = index->GetIndexKeySchema()->GetColumns();
        //init the column id in the original table
        for(auto index_column: index_columns)
        {
          uint32_t index_column_id;
          if (table_info->GetSchema()->GetColumnIndex(index_column->GetName(), index_column_id) == DB_SUCCESS)
          {
            column_ids.emplace_back(index_column_id);
          }
        }
        //init the fields for index
        vector<Field> fields;
        for(auto id: column_ids)
        {
          fields.emplace_back(*insert_row.GetField(id));
        }
        Row index_row(fields);
        vector<RowId> temp_result;

        //if find the duplicate key
        if(index->GetIndex()->ScanKey(index_row, temp_result, exec_ctx_->GetTransaction(), "=") == DB_SUCCESS)
        {
          //if tree is empty
          if(temp_result.empty())
          {
            break;
          }
          //not empty
          cout << "Duplicate entry for key '" << index->GetIndexName() <<  "'." << endl;
          return false;
        }
      }
    }

    //insert the row
    table_info->GetTableHeap()->InsertTuple(insert_row, exec_ctx_->GetTransaction());
    insert_rid = insert_row.GetRowId();

    //if has index, update the index
    if(exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(),indexes) == DB_SUCCESS)
    {
      for(auto index: indexes)
      {
        vector<uint32_t>column_ids;
        vector<Column *>index_columns = index->GetIndexKeySchema()->GetColumns();
        //init the column id in the original table
        for(auto index_column: index_columns)
        {
          uint32_t index_column_id;
          if (table_info->GetSchema()->GetColumnIndex(index_column->GetName(), index_column_id) == DB_SUCCESS)
          {
            column_ids.emplace_back(index_column_id);
          }
        }
        //init the fields for index
        vector<Field> fields;
        for(auto id: column_ids)
        {
          fields.emplace_back(*insert_row.GetField(id));
        }
        Row index_row(fields);
        index->GetIndex()->InsertEntry(index_row, insert_rid, exec_ctx_->GetTransaction());
      }
    }
    return true;
  }
  else
  {
    return false;
  }
}