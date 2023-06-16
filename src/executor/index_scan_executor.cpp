#include "executor/executors/index_scan_executor.h"
#include <algorithm>
/**
* TODO: Student Implement
*/
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

//static bool CompareRowId(RowId &a, RowId &b){
//  if(a.GetPageId() < b.GetPageId() || (a.GetPageId() == b.GetPageId() && a.GetSlotNum() < b.GetSlotNum())) return true;
//  else return false;
//}

void IndexScanExecutor::Init() {
  vector<shared_ptr<AbstractExpression>> plan_ptr;
  if(plan_->GetPredicate() != nullptr){
    auto first_plan_ptr = plan_->GetPredicate();
    if(first_plan_ptr->GetType() == ExpressionType::ComparisonExpression){
      plan_ptr.push_back(first_plan_ptr);
    }
    else if(first_plan_ptr->GetType() == ExpressionType::LogicExpression){
      plan_ptr = plan_->GetPredicate()->GetChildren();
    }

    while(plan_ptr[0]->GetType() == ExpressionType::LogicExpression){
      // comparison
      if(plan_ptr[1]->GetType() == ExpressionType::ComparisonExpression){
        //column and const_value
        auto operator_value = reinterpret_cast<ComparisonExpression *>(plan_ptr[0].get())->GetComparisonType();
        auto column = reinterpret_cast<ComparisonExpression *>(plan_ptr[0].get())->GetChildAt(0);
        auto const_num = reinterpret_cast<ComparisonExpression *>(plan_ptr[0].get())->GetChildAt(1);
        uint32_t col_id = reinterpret_cast<ColumnValueExpression *>(column.get())->GetColIdx();
        //find the correct index
        for(auto index: plan_->indexes_){
          vector<RowId> new_result;
          if(index->GetIndexKeySchema()->GetColumn(0)->GetTableInd() == col_id){
            auto &num_value = reinterpret_cast<ConstantValueExpression *>(const_num.get())->val_;
            Row *key = new Row(*new vector<Field>(1, num_value));
            auto bp_tree = reinterpret_cast<BPlusTreeIndex *>(index->GetIndex());
            bp_tree->ScanKey(*key, new_result, exec_ctx_->GetTransaction(), operator_value);

            sort(new_result.begin(), new_result.end());
            sort(result.begin(), result.end());
            result.resize(min(result.size(), new_result.size()));
            auto it = set_intersection(result.begin(),result.end(),new_result.begin(),new_result.end(), result.begin(), CompareRowId);
            result.resize(it - result.begin());
            break;
          }
        }
      }
    }

    if(plan_ptr[0]->GetType() == ExpressionType::ComparisonExpression){
      //column and const_value
      auto operator_value = reinterpret_cast<ComparisonExpression *>(plan_ptr[0].get())->GetComparisonType();
      auto column = reinterpret_cast<ComparisonExpression *>(plan_ptr[0].get())->GetChildAt(0);
      auto const_num = reinterpret_cast<ComparisonExpression *>(plan_ptr[0].get())->GetChildAt(1);
      uint32_t col_id = reinterpret_cast<ColumnValueExpression *>(column.get())->GetColIdx();
      //find the correct index
      for(auto index: plan_->indexes_){
        vector<RowId> new_result;
        if(index->GetIndexKeySchema()->GetColumn(0)->GetTableInd() == col_id){
          auto &num_value = reinterpret_cast<ConstantValueExpression *>(const_num.get())->val_;
          Row *key = new Row(*new vector<Field>(1, num_value));
          auto bp_tree = reinterpret_cast<BPlusTreeIndex *>(index->GetIndex());
          bp_tree->ScanKey(*key, new_result, exec_ctx_->GetTransaction(), operator_value);

          if(result.empty()){
            result.assign(new_result.begin(), new_result.end());
          }
          else{
            sort(new_result.begin(), new_result.end());
            //            sort(old_result.begin(), old_result.end());
            sort(result.begin(), result.end());
            result.resize(min(result.size(), new_result.size()));
            auto it = set_intersection(result.begin(),result.end(),new_result.begin(),new_result.end(), result.begin(), CompareRowId);
            result.resize(it - result.begin());
          }
        }
      }
    }

    if(plan_ptr.size() == 2){
      if(plan_ptr[1]->GetType() == ExpressionType::ComparisonExpression){
        //column and const_value
        auto operator_value = reinterpret_cast<ComparisonExpression *>(plan_ptr[1].get())->GetComparisonType();
        auto column = reinterpret_cast<ComparisonExpression *>(plan_ptr[1].get())->GetChildAt(0);
        auto const_num = reinterpret_cast<ComparisonExpression *>(plan_ptr[1].get())->GetChildAt(1);
        uint32_t col_id = reinterpret_cast<ColumnValueExpression *>(column.get())->GetColIdx();
        //find the correct index
        for(auto index: plan_->indexes_){
          vector<RowId> new_result;
          if(index->GetIndexKeySchema()->GetColumn(0)->GetTableInd() == col_id){
            auto &num_value = reinterpret_cast<ConstantValueExpression *>(const_num.get())->val_;
            Row *key = new Row(*new vector<Field>(1, num_value));
            auto bp_tree = reinterpret_cast<BPlusTreeIndex *>(index->GetIndex());
            bp_tree->ScanKey(*key, new_result, exec_ctx_->GetTransaction(), operator_value);

            if(result.empty()){
              result.assign(new_result.begin(), new_result.end());
            }
            else{
              sort(new_result.begin(), new_result.end());
              //            sort(old_result.begin(), old_result.end());
              sort(result.begin(), result.end());
              result.resize(min(result.size(), new_result.size()));
              auto it = set_intersection(result.begin(),result.end(),new_result.begin(),new_result.end(), result.begin(), CompareRowId);
              result.resize(it - result.begin());
            }
          }
        }
      }
    }
  }

  Row *new_row;
  while(cursor != result.size()){
    RowId row_id = result[cursor];
    new_row = new Row(row_id);
    TableInfo *table_info = nullptr;
    exec_ctx_->GetCatalog()->GetTable(plan_->table_name_, table_info);
    if(table_info->GetTableHeap()->GetTuple(new_row, exec_ctx_->GetTransaction())){
      if(plan_->need_filter_){
        Field field = plan_->GetPredicate()->Evaluate(new_row);
        if (!field.CompareEquals(Field(kTypeInt, 1))) {
          result.erase(result.begin()+cursor);
        }
        else{
          cursor++;
        }
      }
      else{
        break;
      }
    }
  }
  cursor = 0;
}

bool IndexScanExecutor::Next(Row *row, RowId *rid){

  Row *new_row = nullptr;
  TableInfo *table_info = nullptr;
  if(cursor != result.size()) {
    RowId row_id = result[cursor];
    new_row = new Row(row_id);
    exec_ctx_->GetCatalog()->GetTable(plan_->table_name_, table_info);
    if (!table_info->GetTableHeap()->GetTuple(new_row, exec_ctx_->GetTransaction())) {
      return false;
    }
  }
  else{
    return false;
  }

  vector<Field> fields;
  const Schema *schema = plan_->OutputSchema();
  for(auto column: schema->GetColumns())
    for(auto old_column: table_info->GetSchema()->GetColumns()){
      if(column->GetName() == old_column->GetName())
        fields.push_back(*new_row->GetField(old_column->GetTableInd()));
    }
  *row = Row(fields);
  *rid = RowId(new_row->GetRowId());
  delete new_row;
  cursor++;
  return true;
}
