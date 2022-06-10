#include "executor/execute_engine.h"
#include "glog/logging.h"
#include "parser/minisql_lex.h"
#include "parser/parser.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"
#include <vector>
#include <iomanip>
#include <cstring>
#include <time.h>
#include <algorithm>

ExecuteEngine::ExecuteEngine() {
  //freopen("dbs.txt", "r", stdin);
  FILE * fq;
  fq = fopen("dbs.txt", "r");
  int cnt;
  fscanf(fq, "%d", &cnt);
  for(int i=0; i<cnt; ++i)
  {
    char tmp[1005];
    fscanf(fq, "%s", tmp);
    DBStorageEngine * dbhead = new DBStorageEngine(tmp,false);
    dbs_[tmp] = dbhead;
  }
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
 // puts("Successfully create database ");
  std::cout<<"Successfully create database "<<value<<std::endl;
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
  puts("Successfully delete database\n");
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
  std::cout<<"Now using databse "<<value<<std::endl;
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
      string name_index = table_name + "_index";
      std::cout<<"index name : "<<name_index<<std::endl;
      cur_cata->CreateIndex(table_name, name_index, primary_key, nullptr, index_info);
  }
  //return DB_SUCCESS;
  /*for(auto iter = vec_column.begin(); iter < vec_column.end(); ++iter)
  {
      if((*iter)->IsUnique())
      {
          string unique_index = table_name + "_" + (*iter)->GetName() + " unique";
          CatalogManager * cur_catalog = cur_db->catalog_mgr_;
          vector <string> unique_attribute = {(*iter)->GetName()};
          IndexInfo * indexInfo = nullptr;
          cur_catalog->CreateIndex(table_name, unique_index, unique_attribute, nullptr, indexInfo);
      }
  }*/
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
  string table_name = ast->child_->next_->val_;
  CatalogManager * cur_ca = cur_db->catalog_mgr_;
  IndexInfo * indexInfo = nullptr;
  TableInfo * tableInfo = nullptr;
  cur_ca->GetTable(table_name, tableInfo);
  pSyntaxNode  key_name = ast->child_->next_->next_->child_;
  for(; key_name != nullptr; key_name = key_name->next_)
  {
      uint32_t  key_index;
      dberr_t isIn = tableInfo->GetSchema()->GetColumnIndex(key_name->val_, key_index);
      if(isIn == DB_COLUMN_NAME_NOT_EXIST)
      {
          std::cout<<"The attribute isn't in this table"<<std::endl;
          return DB_FAILED;
      }
      const Column * px = tableInfo->GetSchema()->GetColumn(key_index);
      if(px->IsUnique() == 0)
      {
          std::cout<<"Non-unique key"<<std::endl;
          return DB_FAILED;
      }
  }
  pSyntaxNode key_index = ast->child_->next_->next_->child_;
  for(; key_index != nullptr; key_index = key_index->next_) key_indexes.push_back(key_index->val_);

  dberr_t isExist = cur_ca->CreateIndex(ast->child_->next_->val_, ast->child_->val_, key_indexes, nullptr, indexInfo);
  if(isExist == DB_TABLE_NOT_EXIST) std::cout<<"This table not exist"<<std::endl;
  if(isExist == DB_INDEX_ALREADY_EXIST) std::cout<<"This index already exist"<<std::endl;

  TableHeap * tableHeap = tableInfo->GetTableHeap();
  vector<uint32_t> col_index;
  for(auto iter = key_indexes.begin(); iter != key_indexes.end(); ++iter)
  {
      uint32_t index;
      tableInfo->GetSchema()->GetColumnIndex(*iter, index);
      col_index.push_back(index);
  }
  vector<Field> fields;
  for(auto iter = tableHeap->Begin(nullptr); iter != tableHeap->End(); iter++)
  {
     // Row &it_row = *iter;
      const Row &nrow = *iter;
      vector<Field> index_field;
      for(auto iter2 = col_index.begin(); iter2 != col_index.end(); ++iter2)
          index_field.push_back(*(nrow.GetField(*iter2)));
      Row row_index(index_field);
      indexInfo->GetIndex()->InsertEntry(row_index, nrow.GetRowId(), nullptr);
  }
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

vector<Row*> rec_sel(pSyntaxNode sn, std::vector<Row*>& r, TableInfo* t, CatalogManager* c){
  if(sn == nullptr) return r;
  if(sn->type_ == kNodeConnector){

    vector<Row*> ans;
    if(strcmp(sn->val_,"and") == 0){
      auto r1 = rec_sel(sn->child_,r,t,c);
      ans = rec_sel(sn->child_->next_,r1,t,c);
      return ans;
    }
    else if(strcmp(sn->val_,"or") == 0){
      auto r1 = rec_sel(sn->child_,r,t,c);
      auto r2 = rec_sel(sn->child_->next_,r,t,c);
      for(uint32_t i=0;i<r1.size();i++){
        ans.push_back(r1[i]);
      }
      for(uint32_t i=0;i<r2.size();i++){
        int flag=1;//û���ظ�
        for(uint32_t j=0;j<r1.size();j++){
          int f=1;
          for(uint32_t k=0;k<r1[i]->GetFieldCount();k++){
            if(!r1[i]->GetField(k)->CompareEquals(*r2[j]->GetField(k))){
              f=0;break;
            }
          }
          if(f==1){
            flag=0;//���ظ�
            break;}
        }
        if(flag==1) ans.push_back(r2[i]);
      }
      return ans;
    }
  }
  if(sn->type_ == kNodeCompareOperator){
    string op = sn->val_;//operation type
    string col_name = sn->child_->val_;//column name
    string val = sn->child_->next_->val_;//compare value
    uint32_t keymap;
    vector<Row*> ans;
    if(t->GetSchema()->GetColumnIndex(col_name, keymap)!=DB_SUCCESS){
      cout<<"column not found"<<endl;
      return ans;
    }
    const Column* key_col = t->GetSchema()->GetColumn(keymap);
    TypeId type =  key_col->GetType();

    if(op == "="){
      if(type==kTypeInt)
      {
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        vector<Field> vect_benchmk;
        vect_benchmk.push_back(benchmk);

        vector <IndexInfo*> indexes;
        c->GetTableIndexes(t->GetTableName(),indexes);
        for(auto p=indexes.begin();p<indexes.end();p++){
          if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
            if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){
              // cout<<"--select using index--"<<endl;
              Row tmp_row(vect_benchmk);
              vector<RowId> result;
              (*p)->GetIndex()->ScanKey(tmp_row,result,nullptr);
              for(auto q:result){
                if(q.GetPageId()<0) continue;
                Row *tr = new Row(q);
                t->GetTableHeap()->GetTuple(tr,nullptr);
                ans.push_back(tr);
              }
              return ans;
            }
          }
        }
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }

      else if(type==kTypeFloat)
      {
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }

      else if(type==kTypeChar){
        char *ch = new char[val.size()];
        strcpy(ch,val.c_str());//input compare object
        // cout<<"ch "<<sizeof(ch)<<endl;
        Field benchmk = Field(TypeId::kTypeChar, const_cast<char *>(ch), val.size(), true);
        // Field benchmk(kTypeChar,ch,key_col->GetLength(),true);
        vector<Field> vect_benchmk;
        vect_benchmk.push_back(benchmk);

        vector <IndexInfo*> indexes;
        c->GetTableIndexes(t->GetTableName(),indexes);
        for(auto p=indexes.begin();p<indexes.end();p++){
          if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
            if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){

              // cout<<"--select using index--"<<endl;
              clock_t s,e;
              s=clock();
              Row tmp_row(vect_benchmk);
              vector<RowId> result;
              (*p)->GetIndex()->ScanKey(tmp_row,result,nullptr);
              e=clock();
              cout<<"name index select takes "<<double(e-s)/CLOCKS_PER_SEC<<"s to Execute."<<endl;
              for(auto q:result){
                if(q.GetPageId()<0) continue;
                // cout<<"index found"<<endl;
                Row *tr = new Row(q);
                t->GetTableHeap()->GetTuple(tr,nullptr);
                ans.push_back(tr);
              }
              return ans;
            }
          }
        }

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();

          if(strcmp(test,ch)==0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == "<"){
      if(type==kTypeInt)
      {
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()+2];
        strcpy(ch,val.c_str());
        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          if(strcmp(test,ch)<0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == ">"){
      if(type==kTypeInt)
      {
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//�Ƚ�Ŀ�����ch��

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();

          if(strcmp(test,ch)>0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == "<="){
      if(type==kTypeInt)
      {
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//�Ƚ�Ŀ�����ch��

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();

          if(strcmp(test,ch)<=0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == ">="){
      if(type==kTypeInt)
      {
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//�Ƚ�Ŀ�����ch��

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();

          if(strcmp(test,ch)>=0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == "<>"){
      if(type==kTypeInt)
      {
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareNotEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareNotEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//�Ƚ�Ŀ�����ch��

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();

          if(strcmp(test,ch)!=0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    return ans;
  }
  return r;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  /*string tablename=ast->child_->next_->val_;
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
    //return DB_FAILED;*/
  pSyntaxNode range = ast->child_;
  vector<uint32_t> columns;
  string table_name=range->next_->val_;
  TableInfo *tableinfo = nullptr;
  dberr_t GetRet = cur_db->catalog_mgr_->GetTable(table_name, tableinfo);
  if (GetRet==DB_TABLE_NOT_EXIST)
  {
    cout<<"Table Not Exist!"<<endl;
    return DB_FAILED;
  }
  if(range->type_ == kNodeAllColumns)
  {
    // cout<<"select all"<<endl;
    for(uint32_t i=0;i<tableinfo->GetSchema()->GetColumnCount();i++)
      columns.push_back(i);
  }
  else if(range->type_ == kNodeColumnList){

    pSyntaxNode col = range->child_;
    while(col!=nullptr){
      uint32_t pos;
      if(tableinfo->GetSchema()->GetColumnIndex(col->val_,pos)==DB_SUCCESS){
        columns.push_back(pos);
      }
      else{
        cout<<"column not found"<<endl;
        return DB_FAILED;
      }
      col = col->next_;
    }
  }
  cout<<"--------------------"<<endl;
  //cout<<endl;
  for(auto i:columns){
    cout<<tableinfo->GetSchema()->GetColumn(i)->GetName()<<"   ";
  }
  cout<<endl;
  cout<<"--------------------"<<endl;
  if(range->next_->next_==nullptr)
  {
    int cnt=0;
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      for(uint32_t j=0;j<columns.size();j++){
        if(it->GetField(columns[j])->IsNull()){
          cout<<"null";
        }
        else
          it->GetField(columns[j])->fprint();
        cout<<"  ";
        
      }
      cout<<endl;
      cnt++;
    }
    cout<<"Select successfully, in total "<<cnt<<" records!"<<endl;
    return DB_SUCCESS;
  }
  else if(range->next_->next_->type_ == kNodeConditions)
  {
    pSyntaxNode cond = range->next_->next_->child_;
    vector<Row*> origin_rows;
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      origin_rows.push_back(tp);
    }    
    auto ptr_rows  = rec_sel(cond, *&origin_rows,tableinfo,cur_db->catalog_mgr_);
    
    for(auto it=ptr_rows.begin();it!=ptr_rows.end();it++){
      for(uint32_t j=0;j<columns.size();j++){
        (*it)->GetField(columns[j])->fprint();
        cout<<"  ";
      }
      cout<<endl;
    }
    cout<<"Select successfully, "<<ptr_rows.size()<<" records in total!"<<endl;
  }
  
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
    /*
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
  }*/
    string table_name=ast->child_->val_;
    //puts("gg");
    TableInfo *tableinfo = nullptr;
    dberr_t GetRet = cur_db->catalog_mgr_->GetTable(table_name, tableinfo);
    if (GetRet==DB_TABLE_NOT_EXIST)
    {
        cout<<"Table Not Exist!"<<endl;
        return DB_FAILED;
    }
    vector<Field> fields;
    pSyntaxNode column_pointer= ast->child_->next_->child_;
    int cnt = tableinfo->GetSchema()->GetColumnCount();//number of column
    for (int i=0; i<cnt; ++i)
    {
        TypeId now_type_id = tableinfo->GetSchema()->GetColumn(i)->GetType();
        if(column_pointer==nullptr)
        {//tht end of all insert values
              for(int j=i; j<cnt; ++j)
              {
                  Field new_field(tableinfo->GetSchema()->GetColumn(j)->GetType());
                  fields.push_back(new_field);
              }
              break;
        }
        if(column_pointer->val_==nullptr )
        {
            Field new_field(now_type_id);
            fields.push_back(new_field);
        }
        else
          {
            //cout<<"a number"<<endl;
            if (now_type_id==kTypeInt)
            {
                int x = atoi(column_pointer->val_);
                Field new_field (now_type_id,x);
                fields.push_back(new_field);
            }
            else if(now_type_id==kTypeFloat)
            {
                float f = atof(column_pointer->val_);
                Field new_field (now_type_id,f);
                fields.push_back(new_field);
            }
            else
            {
                string s = column_pointer->val_;
                char *c = new char[s.size()];
                strcpy(c,s.c_str());
                Field new_field = Field(TypeId::kTypeChar, const_cast<char *>(c), s.size(), true);
                fields.push_back(new_field);
            }
        }
        column_pointer = column_pointer->next_;
    }
    if (column_pointer!=nullptr)
    {
        std::cout<<"Column Count doesn't match!"<<std::endl;
        return DB_FAILED;
    }

    Row row(fields);
 //   ASSERT(tableinfo!=nullptr,"TableInfo is Null!");

    TableHeap* tableheap=tableinfo->GetTableHeap();
    bool Is_Insert=tableheap->InsertTuple(row,nullptr);//insert with tableheap

    if(Is_Insert==false)
    {
        std::cout<<"Insert Failed"<<std::endl;
        return DB_FAILED;
    }
    else
    {
        vector <IndexInfo*> indexes;
        cur_db->catalog_mgr_->GetTableIndexes(table_name,indexes);

        for(auto p=indexes.begin();p<indexes.end();p++)
        {
            IndexSchema* index_schema = (*p)->GetIndexKeySchema();
            vector<Field> index_fields;
            for(auto it:index_schema->GetColumns())
            {
                index_id_t tmp;
                if(tableinfo->GetSchema()->GetColumnIndex(it->GetName(),tmp)==DB_SUCCESS)
                    index_fields.push_back(fields[tmp]);
            }
            Row index_row(index_fields);
            dberr_t IsInsert=(*p)->GetIndex()->InsertEntry(index_row,row.GetRowId(),nullptr);
            //cout<<"RowID: "<<row.GetRowId().Get()<<endl;
            if(IsInsert==DB_FAILED)
            {
                cout<<"Insert Failed"<<endl;
                for(auto q=indexes.begin();q!=p;q++)//remove the entries
                {
                    IndexSchema* index_schema_already = (*q)->GetIndexKeySchema();
                    vector<Field> index_fields_already;
                    for(auto it:index_schema_already->GetColumns())
                    {
                        index_id_t tmp_already;
                        if(tableinfo->GetSchema()->GetColumnIndex(it->GetName(),tmp_already)==DB_SUCCESS)
                            index_fields_already.push_back(fields[tmp_already]);
                    }
                    Row index_row_already(index_fields_already);
                    (*q)->GetIndex()->RemoveEntry(index_row_already,row.GetRowId(),nullptr);
                }
                tableheap->MarkDelete(row.GetRowId(),nullptr);
                return IsInsert;
            }
            //else
            //  std::cout<<"Insert Into Index Sccess"<<std::endl;
        }
       // std::cout<<"Insert Success"<<std::endl;
        return DB_SUCCESS;
    }
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
    if(ast->type_!=kNodeDelete)
      return DB_FAILED;
    string table_name=ast->child_->val_;
    TableInfo *tableinfo = nullptr;
    dberr_t GetRet = cur_db->catalog_mgr_->GetTable(table_name, tableinfo);
    if (GetRet==DB_TABLE_NOT_EXIST){
      cout<<"Table Not Exist!"<<endl;
      return DB_FAILED;
    }
    TableHeap* tableheap=tableinfo->GetTableHeap();//锟矫碉拷锟斤拷锟斤拷应锟斤拷锟侥硷拷锟斤拷
    auto del = ast->child_;
    vector<Row*> tar;

    if(del->next_==nullptr){//锟斤拷取锟斤拷锟斤拷选锟斤拷锟斤拷锟斤拷锟斤拷row锟斤拷锟斤拷锟斤拷vector<Row*> tar锟斤拷
      for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
        Row* tp = new Row(*it);
        tar.push_back(tp);
      }
    }
    else{
      vector<Row*> origin_rows;
      for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
        Row* tp = new Row(*it);
        origin_rows.push_back(tp);
      }
      tar  = rec_sel(del->next_->child_, *&origin_rows,tableinfo,cur_db->catalog_mgr_);
    }
    for(auto it:tar){
      tableheap->ApplyDelete(it->GetRowId(),nullptr);
    }
    cout<<"Delete Success, Affects "<<tar.size()<<" Record!"<<endl;
    vector <IndexInfo*> indexes;//锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷锟絠ndexinfo
    cur_db->catalog_mgr_->GetTableIndexes(table_name,indexes);
    for(auto p=indexes.begin();p<indexes.end();p++){
      for(auto j:tar){
        vector<Field> index_fields;
        for(auto it:(*p)->GetIndexKeySchema()->GetColumns()){
          index_id_t tmp;
          if(tableinfo->GetSchema()->GetColumnIndex(it->GetName(),tmp)==DB_SUCCESS){
            index_fields.push_back(*j->GetField(tmp));
          }
        }
        Row index_row(index_fields);
        (*p)->GetIndex()->RemoveEntry(index_row,j->GetRowId(),nullptr);
      }
    }
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  //std::cout<<"This function doesn't work"<<std::endl;
  string table_name=ast->child_->val_;
  TableInfo *tableinfo = nullptr;
  dberr_t GetRet = cur_db->catalog_mgr_->GetTable(table_name, tableinfo);
  if (GetRet==DB_TABLE_NOT_EXIST)
  {
      cout<<"Table Not Exist!"<<endl;
      return DB_FAILED;
  }
  TableHeap* tableheap=tableinfo->GetTableHeap();
  auto updates = ast->child_->next_;
  vector<Row*> tar;

  if(updates->next_==nullptr)
  {
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      tar.push_back(tp);
    }
    // cout<<"---- all "<<tar.size()<<" ----"<<endl;
  }
  else{
    vector<Row*> origin_rows;
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      origin_rows.push_back(tp);
    }
    tar  = rec_sel(updates->next_->child_, *&origin_rows,tableinfo,cur_db->catalog_mgr_);
    // cout<<"---- part "<<tar.size()<<" ----"<<endl;
  }
  updates = updates->child_;
  while(updates && updates->type_ == kNodeUpdateValue){
    string col = updates->child_->val_;
    string upval = updates->child_->next_->val_;
    uint32_t index;
    tableinfo->GetSchema()->GetColumnIndex(col,index);
    TypeId tid = tableinfo->GetSchema()->GetColumn(index)->GetType();
    if(tid == kTypeInt){
      Field* newval = new Field(kTypeInt,stoi(upval));
      for(auto it:tar){
        it->GetFields()[index] = newval;
      }
    }
    else if(tid == kTypeFloat){
      Field* newval = new Field(kTypeFloat,stof(upval));
      for(auto it:tar){
        it->GetFields()[index] = newval;
      }
    }
    else if(tid == kTypeChar){
      uint32_t len = tableinfo->GetSchema()->GetColumn(index)->GetLength();
      char* tc = new char[len];
      strcpy(tc,upval.c_str());
      Field* newval = new Field(kTypeChar,tc,len,true);
      for(auto it:tar){
        it->GetFields()[index] = newval;
      }
    }
    updates = updates->next_;
  }
  for(auto it:tar){
    tableheap->UpdateTuple(*it,it->GetRowId(),nullptr);
  }
  cout<<"Update Success, Affects "<<tar.size()<<" Record!"<<endl;
  return DB_SUCCESS;
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
  string name = ast->child_->val_;
  // the path of account_data
  string file_name = "/mnt/e/minisql-newdev/data/"+name;

  ifstream infile;
  infile.open(file_name.data());
  if (infile.is_open())
  { //if open fails,return false
      string s;
      while(getline(infile,s))
      {//read line by line
          YY_BUFFER_STATE bp = yy_scan_string(s.c_str());
          if (bp == nullptr)
          {
              LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
              exit(1);
          }
          yy_switch_to_buffer(bp);
          // init parser module
          MinisqlParserInit();
          // parse

          yyparse();

          ExecuteContext context;
          Execute(MinisqlGetParserRootNode(), &context);
      }
      return DB_SUCCESS;
  }
  else
  {
      cout<<"Failed In Opening File!"<<endl;
      return DB_FAILED;
  }
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
