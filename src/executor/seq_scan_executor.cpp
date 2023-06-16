//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"
#include "planner/expressions/column_value_expression.h"
#include "planner/expressions/comparison_expression.h"
#include "algorithm"

/**
* TODO: Student Implement
 */
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){}

void SeqScanExecutor::Init() {
  if(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info) != DB_SUCCESS)
  {
    // LOG(WARNING) << "Get table name fail.";
    exit(1);
  }
  iter = table_info->GetTableHeap()->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  const Row &temp_row = *iter;
  TableHeap *table_heap = table_info->GetTableHeap();
  if(plan_->GetPredicate() != nullptr)
  {
    for(; iter != table_heap->End(); iter++)
    {
      Field field = plan_->GetPredicate()->Evaluate(&temp_row);
      if (field.CompareEquals(Field(kTypeInt, 1)))
      {
        break;
      }
    }
  }

  if(iter == table_heap->End())
  {
    return false;
  }
  vector<Field> fields;
  const Schema *schema = plan_->OutputSchema();
  for(auto column: schema->GetColumns())
    for(auto old_column: table_info->GetSchema()->GetColumns())
    {
      if(column->GetName() == old_column->GetName())
        fields.push_back(*temp_row.GetField(old_column->GetTableInd()));
    }

  *row = Row(fields);
  *rid = RowId(temp_row.GetRowId());
  iter++;
  return true;
}
