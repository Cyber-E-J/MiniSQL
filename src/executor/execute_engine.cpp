#include "executor/execute_engine.h"
#include "glog/logging.h"
#include <vector>

ExecuteEngine::ExecuteEngine() {

}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  if(ast->type_ != kNodeCreateDB) return DB_FAILED;
  std::string value = ast->child_->val_;
  if(dbs_.find(value) != dbs_.end())
  {
      std::cout<<"The Database named "<<value<<" already exists"<<std::endl;
      return DB_FAILED;
  }
  DBStorageEngine * dbhead = new DBStorageEngine(value);
  dbs_[value] = dbhead;
  puts("Successfully create dbs\n");
  //return DB_FAILED;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  std::string value = ast->child_->val_;
  delete dbs_[value];
  dbs_.erase(value);
  puts("Successfully delete dbs\n");
  //return DB_FAILED;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  puts("----- All databases are shown below -----\n");
  for(auto iter = dbs_.begin(); iter != dbs_.end(); ++iter)
      std::cout<<iter->first<<"\n";
  //return DB_FAILED;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  std::string value = ast->child_->val_;
  if(dbs_.count(value) == 0)
      puts("Invalid Input\n");
  else current_db_ = value, cur_db = dbs_[value];
  //return DB_FAILED;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  std::cout<<"----- Show Tables -----"<<std::endl;
  vector<TableInfo* > Tables;
  cur_db->catalog_mgr_->GetTables(Tables);
  for(auto iter = Tables.begin(); iter < Tables.end(); ++iter)
      std::cout<<(*iter)->GetTableName()<<std::endl;
  //return DB_FAILED;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  //return DB_FAILED;
  pSyntaxNode  p_column = ast->child_->next_->child_;
  string table_name = ast->child_->val_;
  vector<Column* > vec_column;
  while(p_column != nullptr && p_column->type_ == kNodeColumnDefinition)
  {
      bool unique = 0;
      if(p_column->val_ != nullptr)
          if(strcmp(p_column->val_, "unique") == 0) unique = 1;
      string name_column = p_column->child_->val_;
      string type_column = p_column->child_->next_->val_;
      Column * cur; int count = 0;
      if(type_column == "int") cur = new Column(name_column, kTypeInt, count, 1, unique);
      else if(type_column == "float") cur = new Column(name_column, kTypeFloat, count, 1, unique);
      else if(type_column == "char")
      {
          long unsigned int is_deci = -1;
          string len_type = p_column->child_->next_->child_->val_;
          if(len_type.find('.') != is_deci)
          {
              std::cout<<"String length can't be a decimal"<<std::endl;
              return DB_FAILED;
          }
          int is_nega = atoi(p_column->child_->next_->child_->val_);
          if(is_nega < 0)
          {
              std::cout<<"String length can't be a negative"<<std::endl;
              return DB_FAILED;
          }
          cur = new Column(name_column, kTypeChar, is_nega, count, 1, unique);
      }
      else
      {
          std::cout<<"Type Error"<<std::endl;
          return DB_FAILED;
      }
      vec_column.push_back(cur); p_column = p_column->next_;
      ++count;
  }

  TableInfo * table_info_ = nullptr;
  Schema * sch_= new Schema(vec_column);
  dberr_t is_Exist = cur_db->catalog_mgr_->CreateTable(table_name, sch_, nullptr, table_info_);

  if(is_Exist == DB_TABLE_ALREADY_EXIST)
  {
      std::cout<<"Table already exist"<<std::endl;
      return is_Exist;
  }
  if(p_column != nullptr)
  {
      pSyntaxNode  p_key = p_column->child_;
      vector<string> primary_key;
      while(p_key != nullptr)
      {
          string name_key = p_key->val_;
          primary_key.push_back(name_key);
          std::cout<<"key name : "<<name_key<<std::endl;
          p_key = p_key->next_;
      }

      CatalogManager * cur_cata = cur_db->catalog_mgr_;
      IndexInfo * index_info = nullptr;
      string name_index = table_name + "_pk";
      std::cout<<"index name : "<<name_index<<std::endl;
      cur_cata->CreateIndex(table_name, name_index, primary_key, nullptr, index_info);
  }
  //return DB_SUCCESS;
  return is_Exist;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  //return DB_FAILED;
  dberr_t is_Drop = cur_db->catalog_mgr_->DropTable(ast->child_->val_);
  if(is_Drop == DB_TABLE_NOT_EXIST) std::cout<<"Table isn't exist"<<std::endl;
  return is_Drop;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  std::cout<<"----- Show Indexes -----"<<std::endl;
  vector<TableInfo* > Tables;
  cur_db->catalog_mgr_->GetTables(Tables);
  for(auto iter = Tables.begin(); iter < Tables.end(); ++iter)
  {
      std::cout<<"Index "<<(*iter)->GetTableName()<<" : "<<std::endl;
      vector<IndexInfo* > Indexes;
      cur_db->catalog_mgr_->GetTableIndexes((*iter)->GetTableName(), Indexes);
      for(auto iter2 = Indexes.begin(); iter2 < Indexes.end(); ++iter2)
          cout<<(*iter2)->GetIndexName()<<std::endl;
  }
  //return DB_FAILED;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  return DB_FAILED;
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

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
