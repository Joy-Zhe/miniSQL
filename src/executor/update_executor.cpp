//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"
#include "planner/expressions/constant_value_expression.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**
* TODO: Student Implement
*/
void UpdateExecutor::Init() {
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row update_row{};
  RowId update_rowid{};
  if(child_executor_->Next(&update_row, &update_rowid))
  {
    Row new_row = GenerateUpdatedTuple(update_row);
    new_row.SetRowId(update_rowid);

    //find the table info
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
        //init the column ids in table
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
          if(plan_->GetUpdateAttr().find(id) != plan_->GetUpdateAttr().end())
          {
            if(!new_row.GetField(id)->CompareEquals(*update_row.GetField(id)))
              fields.emplace_back(*new_row.GetField(id));
          }
        }
        if(fields.empty())
        {
          continue;
        }
        Row index_row(fields);
        vector<RowId> temp_result;
        //if find the duplicate key
        if(index->GetIndex()->ScanKey(index_row, temp_result, exec_ctx_->GetTransaction(), "=") == DB_SUCCESS)
        {
          cout << "Duplicate entry for key '" << index->GetIndexName() <<  "'." << endl;
          return false;
        }
      }
    }

    //if no duplicate key, we update the index
    if(!indexes.empty())
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
          fields.emplace_back(*new_row.GetField(id));
        }
        Row index_row(fields);
        //if find the duplicate key
        index->GetIndex()->RemoveEntry(index_row, new_row.GetRowId(), exec_ctx_->GetTransaction());
        index->GetIndex()->InsertEntry(index_row, new_row.GetRowId(), exec_ctx_->GetTransaction());
      }
    }
    table_info->GetTableHeap()->UpdateTuple(new_row, new_row.GetRowId(), exec_ctx_->GetTransaction());

    return true;
  }
  else
  {
    return false;
  }
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  auto updateAttr = plan_->GetUpdateAttr();
  vector<Field> new_fields;
  for(size_t i=0; i < src_row.GetFieldCount(); i++)
  {
    bool ifUpdate = false;
    for(const auto& attr: updateAttr)
    {
      if (i == attr.first)
      {
        auto &const_value = reinterpret_cast<ConstantValueExpression *>(attr.second.get())->val_;
        new_fields.push_back(const_value);
        ifUpdate = true;
        break;
      }
    }
    if(!ifUpdate)
    {
      new_fields.push_back(*src_row.GetField(i));
    }
  }
  if(new_fields.size() != src_row.GetFieldCount())
  {
    // LOG(ERROR) << "Error generate tuple.";
  }
  Row new_row(new_fields);
  return new_row;
}