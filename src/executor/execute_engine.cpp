#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}//added
ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  pSyntaxNode ast_database = ast->child_;
  if(ast_database->id_ != 0){
    return DB_FAILED;
  }
  std::string db_name = ast_database->val_;
  if(dbs_.find(db_name) != dbs_.end()){
    cout << "Can't create database '" + db_name << "'." << endl; ;
    return DB_ALREADY_EXIST;
  }
  dbs_[db_name] = new DBStorageEngine(db_name, true);
  cout << "Database '" + db_name + "' created." << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  pSyntaxNode ast_database = ast->child_;
  if(ast_database->id_ != 0){
    return DB_FAILED;
  }
  std::string db_name = ast_database->val_;
  if(dbs_.find(db_name) == dbs_.end()){
    return DB_NOT_EXIST;
  }
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if(current_db_ == db_name){
    current_db_.clear();
  }

  std::string db_file_name = "./databases/" + db_name;
  remove(db_file_name.c_str());

  cout << "Database '" + db_name + "' dropped." << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  //empty
  if(dbs_.empty()){
    cout << "Empty set." <<endl;
    return DB_SUCCESS;
  }
  //not empty
  vector<uint32_t> column_length;
  string title = "Database";
  column_length.push_back(title.length());
  for(auto iter: dbs_){
    column_length[0] = max(column_length[0] , uint32_t(iter.first.length()));
  }
  PrintLine(column_length);
  cout << setiosflags(ios::left);
  cout << "| " << setw(column_length[0]) << title << " |" << endl;
  PrintLine(column_length);
  for(auto iter: dbs_){
    cout << "| " << setw(column_length[0]) << iter.first << " |" << endl;
  }
  PrintLine(column_length);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  pSyntaxNode ast_database = ast->child_;
  if(ast_database->id_ != 0){
    return DB_FAILED;
  }
  std::string db_name = ast_database->val_;
  if(dbs_.find(db_name) == dbs_.end()){
    return DB_NOT_EXIST;
  }
  current_db_ = db_name;
  cout << "Database changed." << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if(current_db_.empty()){
    return DB_NOT_SELECT;
  }
  vector<TableInfo *> tables;
  if(context->GetCatalog()->GetTables(tables) != DB_SUCCESS)
    return DB_FAILED;
  //empty

  if(tables.empty()){
    cout << "Empty set." << endl;
    return DB_SUCCESS;
  }
  //not empty
  string title = "Tables_in_" + current_db_;
  vector<uint32_t> column_length;
  column_length.push_back(title.length());

  for(auto iter: tables){
    column_length[0] = max(column_length[0], uint32_t(iter->GetTableName().length()));
  }
  PrintLine(column_length);
  cout << setiosflags(ios::left);
  cout << "| " << setw(column_length[0]) << title << " |" << endl;
  PrintLine(column_length);
  for(auto iter: tables){
    cout << "| " << setw(column_length[0]) << iter->GetTableName() << " |" << endl;
  }
  PrintLine(column_length);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if(current_db_.empty()){
    return DB_NOT_SELECT;
  }

  //get the table name
  pSyntaxNode create_table_ast = ast->child_;
  string table_name = create_table_ast->val_;
  pSyntaxNode ast_ptr = create_table_ast->next_->child_;

  vector<Column *> columns;
  vector<string> primary_key;
  uint32_t index = 0;

  while(ast_ptr != nullptr){
    string column_name;
    TypeId type;
    uint32_t length;
    bool nullable = true;
    bool unique = false;

    // constraint
    if(ast_ptr->val_ != nullptr) {
      string constraint_type = ast_ptr->val_;
      //if need to lower case
      //transform(constraint_type.begin(), constraint_type.end(), constraint_type.begin(), ::tolower);
      if (constraint_type == "unique") {
        unique = true;
      }
      else if (constraint_type == "not null"){
        nullable = false;
      }
      else if (constraint_type == "primary keys"){
        pSyntaxNode temp_key_ptr = ast_ptr->child_;
        while(temp_key_ptr != nullptr){
          string key = temp_key_ptr->val_;
          primary_key.push_back(key);
          temp_key_ptr = temp_key_ptr->next_;
        }
        break;
      }
    }

    // not constraint
    pSyntaxNode temp_ptr = ast_ptr->child_; // the child of definition
    while(temp_ptr != nullptr){
      string temp_type; // the type of column type
      switch(temp_ptr->type_){
        case kNodeIdentifier:
          column_name = temp_ptr->val_;
          break;
        case kNodeColumnType:
          temp_type = temp_ptr->val_;
          //if need to lower case
          //transform(temp_type.begin(),temp_type.end(),temp_type.begin(),::tolower);
          if( temp_type == "int" ){
            type = kTypeInt;
          }
          else if( temp_type == "char"){
            type = kTypeChar;
            pSyntaxNode temp_char_ptr = temp_ptr->child_;
            string temp_string = temp_char_ptr->val_;
            if(temp_string.find('-', 0) != string::npos){
              cout << "Check the syntax to use in '" + temp_string + "'." << endl;
              return DB_FAILED;
            }
            length = strtoul(temp_char_ptr->val_, nullptr, 10);
          }
          else if( temp_type == "float"){
            type = kTypeFloat;
          }
          break;
        default:
          break;
      }
      temp_ptr = temp_ptr->next_;
    }

    if(type != kTypeChar)
      columns.push_back(new Column(column_name, type, index, nullable, unique));
    else
      columns.push_back(new Column(column_name, type, length, index, nullable, unique));

    index++;//the index of column
    ast_ptr = ast_ptr->next_;
  }

  //create schema and table
  auto schema = make_shared<Schema>(columns, true);
  TableInfo *table_info = nullptr;
  dberr_t result =  context->GetCatalog()->CreateTable(table_name, schema.get(), context->GetTransaction(), table_info);
  if(result == DB_SUCCESS){
    //update the primary key
    if(!primary_key.empty()){
      table_info->GetTableMetaData()->primary_key_.assign(primary_key.begin(), primary_key.end());
      //create index for primary key
      IndexInfo *index_primary_info = nullptr;
      result = context->GetCatalog()->CreateIndex(table_name, "PRIMARY", primary_key, context->GetTransaction(), index_primary_info, "bptree");
    }
    if(result == DB_SUCCESS){
      //create index for unique column
      for(const auto &column: columns){
        if(column->IsUnique()){
          IndexInfo *index_unique_info = nullptr;
          result = context->GetCatalog()->CreateIndex(table_name, column->GetName(), vector<string>(1, column->GetName()), context->GetTransaction(), index_unique_info, "bptree");
          if(result != DB_SUCCESS) return result;
        }
      }
    }
    cout << "Table '" + table_name << "' created." << endl;
  }
  return result;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if(current_db_.empty()){
    return DB_NOT_SELECT;
  }
  string table_name = ast->child_->val_;
  dberr_t result = context->GetCatalog()->DropTable(table_name);
  if(result == DB_SUCCESS){
    cout << "Table '" + table_name + "' dropped." << endl;
  }
  return result;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if(current_db_.empty()){
    return DB_NOT_SELECT;
  }

  vector<string> title;
  vector<uint32_t> column_length;
  unordered_map<string, vector<IndexInfo *>> indexes;

  //title
  {
    title.emplace_back("Table");
    title.emplace_back("Key_name");
    title.emplace_back("Seq_in_index");
    title.emplace_back("Column_name");
    title.emplace_back("Index_type");
  }

  //update the length
  for(const auto & i : title){
    column_length.push_back(i.length());
  }

  vector<TableInfo *> tables;
  if(context->GetCatalog()->GetTables(tables) != DB_SUCCESS)
    return DB_FAILED;

  for(auto iter: tables){
    string table_name = iter->GetTableName();
    vector<IndexInfo *> temp_indexes;
    //get indexes of the table
    if(context->GetCatalog()->GetTableIndexes(table_name, temp_indexes) != DB_SUCCESS) {
      cout << "Empty set." << endl;
      return DB_SUCCESS;
    }
    if(!temp_indexes.empty())
      indexes[table_name] = temp_indexes;
  }

  if(indexes.empty()){
    cout << "Empty set." << endl;
    return DB_SUCCESS;
  }

  //update the length
  for(const auto &iter: indexes){
    for(const auto &index: iter.second){
      column_length[0] = max(column_length[0], uint32_t(iter.first.length()));
      column_length[1] = max(column_length[1], uint32_t(index->GetIndexName().length()));
      for(const auto &column: index->GetIndexKeySchema()->GetColumns()){
        column_length[3] = max(column_length[3], uint32_t(column->GetName().length()));
      }
      column_length[4] = max(column_length[4], uint32_t(index->GetIndexType().length()));
    }
  }

  //show indexes
  cout << setiosflags(ios::left);
  PrintLine(column_length);
  for(int i=0; i < title.size(); i++) {
    cout << "| " << setw(column_length[i]) << title[i] << " ";
  }
  cout << "|" << endl;
  PrintLine(column_length);

  for(const auto &iter: indexes){
    for(const auto &index: iter.second){
      for(int i=0; i < index->GetIndexKeySchema()->GetColumns().size(); i++){
        cout << "| " << setw(column_length[0]) << iter.first << " | " << setw(column_length[1]) << index->GetIndexName() << " | " <<
            setw(column_length[2]) << i+1 << " | " << setw(column_length[3]) << index->GetIndexKeySchema()->GetColumns()[i]->GetName() << " | " <<
            setw(column_length[4]) << index->GetIndexType() << " |" << endl;
      }
    }
  }
  PrintLine(column_length);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(current_db_.empty()){
    return DB_NOT_SELECT;
  }

  //get the index name
  pSyntaxNode index_name_ptr = ast->child_;
  string index_name = index_name_ptr->val_;

  //get the index table
  pSyntaxNode index_table_ptr = index_name_ptr->next_;
  string table_name = index_table_ptr->val_;

  //get the index key
  vector<string> index_keys;
  pSyntaxNode index_key_ptr = index_table_ptr->next_;
  pSyntaxNode ptr = index_key_ptr->child_;
  while(ptr != nullptr){
    index_keys.emplace_back(ptr->val_);
    ptr = ptr->next_;
  }

  TableInfo *table_info = nullptr;
  context->GetCatalog()->GetTable(table_name, table_info);
  if(index_keys.size() == 1){
    uint32_t col_index;
    if(table_info->GetSchema()->GetColumnIndex(index_keys[0], col_index) == DB_SUCCESS){
      const Column *column = table_info->GetSchema()->GetColumn(col_index);
    }
  }
  else{
    for(const auto& index : index_keys){
      uint32_t col_index;
      if(table_info->GetSchema()->GetColumnIndex(index, col_index) == DB_SUCCESS){
        const Column *column = table_info->GetSchema()->GetColumn(col_index);
        if(column->IsUnique()){
          break;
        }
        if(index == index_keys[index_keys.size()-1]){
          cout << "The indexes don't have unique index." << endl;
          return DB_FAILED;
        }
      }
    }
  }

  //get the index type
  pSyntaxNode index_type_ptr = index_key_ptr->next_;
  string index_type = "bptree";
  if(index_type_ptr != nullptr){
    index_type = index_type_ptr->child_->val_;
  }

  //create index
  IndexInfo *index_info = nullptr;
  dberr_t result = context->GetCatalog()->CreateIndex(table_name, index_name, index_keys, context->GetTransaction(), index_info, index_type);
  if(result == DB_SUCCESS){
    cout << "Index '" + index_name + "' created." << endl;
  }
  return result;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  //drop all the same name index of all tables
  string index_name = ast->child_->val_;
  vector<TableInfo *> tables;
  if(context->GetCatalog()->GetTables(tables) == DB_SUCCESS){
    for(const auto &table: tables){
      IndexInfo *index = nullptr;
      if(context->GetCatalog()->GetIndex(table->GetTableName(), index_name, index) == DB_SUCCESS){
        dberr_t result = context->GetCatalog()->DropIndex(table->GetTableName(), index_name);
        if(result != DB_SUCCESS){
          return result;
        }
      }
      else{
        return DB_FAILED;
      }
    }
    cout << "Index '" + index_name + "' dropped." << endl;
    return DB_SUCCESS;
  }
  else{
    return DB_FAILED;
  }
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string file_name = ast->child_->val_;
  const int buf_size = 1024;
  char file[buf_size];
  string line;
  string new_line;
  memset(file, 0, buf_size);
  ifstream in(file_name, ios::binary);
  if(! in.is_open()){
    LOG(WARNING) << "Error opening file.";
    cout << "Open file '" + file_name + "' failed." <<endl;
    return DB_FAILED;
  }
  while (!in.eof()) {
    // read from buffer
    in.getline(file, buf_size);
    line = file;
    //delete the '\r'
    if(find(line.begin(),line.end(), ';') == line.end()){
      auto iter = find(line.begin(), line.end(), '\r');
      line.assign(line.begin(), iter);
      new_line += line;
      continue;
    }
    else{
      auto iter = find(line.begin(), line.end(), '\r');
      line.assign(line.begin(), iter);
      new_line += line;
    }

    // create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(new_line.c_str());
    new_line.clear();
    line.clear();
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);
    // init parser module
    MinisqlParserInit();
    // parse
    yyparse();
    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    } else {
      // Comment them out if you don't need to debug the syntax tree
      printf("[INFO] Sql syntax parse ok!\n");
    }
    auto result = Execute(MinisqlGetParserRootNode());
    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();
    // quit condition
    ExecuteInformation(result);
    if (result == DB_QUIT) {
      return  DB_QUIT;
    }
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  //context->GetBufferPoolManager()->FlushAllPages();
  for(auto it : context->GetBufferPoolManager()->GetPageTable())//flush all pages
    context->GetBufferPoolManager()->FlushPage(it.first);
  return DB_QUIT;
}
void ExecuteEngine::PrintLine(vector<uint32_t> &column_length){
  for (auto &iter : column_length) {
    // responding to "| "
    cout << "+-";
    cout << setw(iter) << setfill('-') << "-";
    cout << "-";
  }
  cout << setfill(' ');
  // responding to "|"
  cout << "+" << endl;
}
