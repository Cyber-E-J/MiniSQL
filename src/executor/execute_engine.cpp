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
  vector<string> key_indexes;
  pSyntaxNode  key_index = ast->child_->next_->next_->child_;
  for(; key_index != nullptr; key_index = key_index->next_)
      key_indexes.push_back(key_index->val_);
  CatalogManager * cur_ca = cur_db->catalog_mgr_;
  IndexInfo * indexInfo = nullptr;
  dberr_t isExist = cur_ca->CreateIndex(ast->child_->next_->val_, ast->child_->val_, key_indexes, nullptr, indexInfo);
  if(isExist == DB_TABLE_NOT_EXIST) std::cout<<"This table not exist"<<std::endl;
  if(isExist == DB_INDEX_ALREADY_EXIST) std::cout<<"This index already exist"<<std::endl;
  return isExist;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  vector<TableInfo* > Tables;
  cur_db->catalog_mgr_->GetTables(Tables);
  for(auto iter = Tables.begin(); iter < Tables.end(); ++iter)
  {
      vector<IndexInfo* > Indexes;
      cur_db->catalog_mgr_->GetTableIndexes((*iter)->GetTableName(), Indexes);
      string name = ast->child_->val_;
      for(auto iter2 = Indexes.begin(); iter2 < Indexes.end(); ++iter2)
          if((*iter2)->GetIndexName() == name)
          {
              dberr_t isDrop = cur_db->catalog_mgr_->DropIndex((*iter)->GetTableName(), name);
              if(isDrop == DB_TABLE_NOT_EXIST) std::cout<<"This table not exist"<<std::endl;
              if(isDrop == DB_INDEX_NOT_FOUND) std::cout<<"This index not found"<<std::endl;
              return isDrop;
          }
  }
  std::cout<<"This index not exist"<<std::endl;
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  string tablename=ast->child_->next_->val_;
  TableInfo* tableInfo;
  SyntaxNodeType select_type=ast->child_->type_;
  dberr_t gettableres=dbs_[current_db_]->catalog_mgr_->GetTable(tablename,tableInfo);
  TableHeap* tableheap=tableInfo->GetTableHeap();
  ASSERT(gettableres==DB_SUCCESS,"get table failed");
  Schema* schema=tableInfo->GetSchema();
  // condition part
  pSyntaxNode condition=ast->child_->next_->next_;
  map<char*, char*> judge;//first atrribute second value
  vector<char*> isnull;
  vector<char*> notnull;
  map<char*,bool> andor;//char* represent attribute,bool==true means and false means or
  map<char*,uint32_t>comparecol;
  bool flagcompare=false;
  // true if we need to do compare later
  if(condition && condition->type_==kNodeConditions)
  {
      flagcompare=true;
      while(condition->child_->child_)
      {

          condition=condition->child_;
          // set if the sibling compare condition is "and" or "or"
          if(condition->type_==kNodeConnector)
          {
              if(strcmp("and",condition->val_)==0)
                  andor[condition->child_->next_->child_->val_]=true;
              else if(strcmp("or",condition->val_)==0)
                  andor[condition->child_->next_->child_->val_]=false;
              // need to be finish depending on the connector is and or or
              // first deal with the sibling
              pSyntaxNode sibling=condition->child_->next_;
              pSyntaxNode attribute=sibling->child_;
              if(strcmp("=",sibling->val_)==0) judge[attribute->val_]=attribute->next_->val_;
              else if(strcmp("is",sibling->val_)==0) isnull.push_back(attribute->val_);
              else if(strcmp("not",sibling->val_)==0) notnull.push_back(attribute->val_);
              schema->GetColumnIndex(attribute->val_,comparecol[attribute->val_]);
              // then recursively deal with the child
              pSyntaxNode last=condition->child_;
              pSyntaxNode lastattribute=last->child_;
              if(last->type_==kNodeCompareOperator)
              {
                  if(strcmp("=",last->val_)==0) judge[lastattribute->val_]=lastattribute->next_->val_;
                  else if(strcmp("is",last->val_)==0) isnull.push_back(lastattribute->val_);
                  else if(strcmp("not",condition->child_->val_)==0) notnull.push_back(lastattribute->val_);
                  schema->GetColumnIndex(condition->child_->val_,comparecol[condition->child_->val_]);
                  break;
              }
      }
      else if(condition->type_==kNodeCompareOperator)
      {
          if(strcmp("=",condition->val_)==0)
              judge[condition->val_]=condition->child_->next_->val_;
          else if(strcmp("is",condition->val_)==0)
              isnull.push_back(condition->child_->next_->val_);
          else if(strcmp("not",condition->val_)==0)
              notnull.push_back(condition->child_->next_->val_);

          schema->GetColumnIndex(condition->child_->val_,comparecol[condition->child_->val_]);
          break;
      }
    }
    // else if(condition->child_->type_==kNodeConnector&&condition->child_->val_=="and"){
    // do nothing
    // }
  }
  map<string,uint32_t> selcol;
  // print out the top line
  std::cout<<"+---------------------------------+"<< std::endl<<" | ";
  // first column name ,second index
  if(select_type==kNodeAllColumns)
  {
      // all the columns are selected
      for(auto p:schema->GetColumns())
      {
        // set the selcol
          dberr_t getcolres=schema->GetColumnIndex(p->GetName(),selcol[p->GetName()]);
          if(getcolres==DB_COLUMN_NAME_NOT_EXIST)
              return getcolres;
          std::cout<<p->GetName()<<" | ";
      }
  }
  else if(select_type==kNodeColumnList)
  {
      // part of the columns are selected
      for(auto p=ast->child_->child_;p!=nullptr;p=p->next_)
      {
          dberr_t getcolres=schema->GetColumnIndex(p->val_,selcol[p->val_]);
          if(getcolres==DB_COLUMN_NAME_NOT_EXIST)
              return getcolres;
          std::cout<<p->val_<<" | ";
      }
  }
  std::cout<< std::endl;
  std::cout<<"+---------------------------------+"<< std::endl;
  // print out each row
  for(TableIterator p=tableheap->Begin(nullptr);p!=tableheap->End();++p)
  {
      bool flagprint=true;
      if(flagcompare){
        // deal with and part
        for(auto i:judge)
        {
          if(andor[i.first]&&(*p).GetField(comparecol[i.first])->GetData()!=i.second)
            flagprint=false;
        }
        for(auto i:isnull)
        {
          if(andor[i]&&(*p).GetField(comparecol[i])->IsNull()!=true)
            flagprint=false;
        }
        for(auto i:notnull)
        {
          if(andor[i]&&(*p).GetField(comparecol[i])->IsNull()!=false)
            flagprint=false;
        }
        //deal with or part
        for(auto i:judge)
        {
          if(!andor[i.first]&&(*p).GetField(comparecol[i.first])->GetData()==i.second)
            flagprint=true;
        }
        for(auto i:isnull)
        {
          if(!andor[i]&&(*p).GetField(comparecol[i])->IsNull()==true)
            flagprint=true;
        }
        for(auto i:notnull)
        {
          if(!andor[i]&&(*p).GetField(comparecol[i])->IsNull()==false)
            flagprint=true;
        }
    }
    if(flagprint)
    {
        std::cout<<"|";
        for(auto q:selcol)
        {
            Field *tmpfield=(*p).GetField(q.second);
            std::cout<<tmpfield->GetData()<<" | ";
        }
        std::cout<< std::endl;
    }
  }
    std::cout<<"+---------------------------------+"<< std::endl;
    return DB_SUCCESS;
    //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  string table_name = ast->child_->val_;
  TableInfo * tableInfo = nullptr;
  dberr_t tmp = cur_db->catalog_mgr_->GetTable(table_name, tableInfo);
  if(tmp == DB_TABLE_NOT_EXIST)
  {
      std::cout<<"Table not exist"<<std::endl;
      return DB_FAILED;
  }
  vector<Field> field;
  pSyntaxNode p_column = ast->child_->next_->child_;// head pointer
  int count = tableInfo->GetSchema()->GetColumnCount();

  for(int i=0; i<count; ++i)
  {
      TypeId type_id = tableInfo->GetSchema()->GetColumn(i)->GetType();
      if(p_column == nullptr)
      {
          for(int j=0; j < count-i; ++j)
          {
              Field new_field(type_id);
              field.push_back(new_field);
          }
          break;
      }
      if(p_column->val_ == nullptr)
      {
          Field new_field(type_id);
          field.push_back(new_field);
      }
      else
      {
          if(type_id == kTypeInt)
          {
              int new_int = atoi(p_column->val_);
              Field new_field(type_id, new_int);
              field.push_back(new_field);
          }
          else if(type_id == kTypeChar)
          {
              string new_char = p_column->val_;
              Field new_field(type_id, p_column->val_, new_char.length(), true);
              field.push_back(new_field);
          }
          else
          {
              float new_float = atof(p_column->val_);
              Field new_field(type_id, new_float);
              field.push_back(new_field);
          }
      }
      p_column = p_column->next_;
  }
  if(p_column != nullptr)
  {
      std::cout<<"Column count not match"<<std::endl;
      return DB_FAILED;
  }

  Row row(field);
  ASSERT(tableInfo!=nullptr, "TableInfo is NULL");
  TableHeap * tableHeap = tableInfo->GetTableHeap();
  bool isInsert = tableHeap->InsertTuple(row, nullptr);
  if(isInsert == false)
  {
      std::cout<<"Not insert"<<std::endl;
      return DB_FAILED;
  }
  else
  {
      vector <IndexInfo*> index;
      cur_db->catalog_mgr_->GetTableIndexes(table_name, index);
      for(auto iter = index.begin(); iter<index.end(); ++iter)
      {
          dberr_t isExist = (*iter)->GetIndex()->InsertEntry(row, row.GetRowId(), nullptr);
          if(isExist == DB_FAILED)// atomic
          {
              std::cout<<"Insert into index failed"<<std::endl;
              for(auto iter2 = index.begin(); iter2<index.end(); ++iter2)
                (*iter2)->GetIndex()->RemoveEntry(row, row.GetRowId(), nullptr);
              tableHeap->MarkDelete(row.GetRowId(), nullptr);
              return DB_FAILED;
          }
          else std::cout<<"Insert index successfully"<<std::endl;
      }
      std::cout<<"Insert successfully! yeah!"<<std::endl;
      return DB_SUCCESS;
  }
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
