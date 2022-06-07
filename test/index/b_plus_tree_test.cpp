#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 1000;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
    //tree.PrintTree(mgr[0]);
    
  }
  ASSERT_TRUE(tree.Check());
  // Print tree
  tree.PrintTree(mgr[0]);
  // Search keys
  vector<int> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(i, ans);
    ASSERT_EQ(kv_map[i], ans[i]);
  }
  ASSERT_TRUE(tree.Check());
  



  /*debug section start*/

  // delete_seq[0] = 20;
  // delete_seq[1] = 5;
  // delete_seq[2] = 25;
  // delete_seq[3] = 13;
  // delete_seq[4] = 29;
  // delete_seq[5] = 6;
  // delete_seq[6] = 22;
  // delete_seq[7] = 28;
  // delete_seq[8] = 10;
  // delete_seq[9] = 18;
  // delete_seq[10] = 26;
  // delete_seq[11] = 9;
  // delete_seq[12] = 23;
  // delete_seq[13] = 21;
  // delete_seq[14] = 27; 

  // for (int i = 0; i < 13 ; i++) {
  //   tree.Remove(delete_seq[i]);
  // }
  // tree.PrintTree(mgr[1]);


  //  tree.Remove(delete_seq[13]);
  //  tree.PrintTree(mgr[2]);

  // tree.Remove(delete_seq[13]);

  // for (int i = 14; i < n ; i++) {
  //   tree.Remove(delete_seq[i]);
  //   tree.PrintTree(mgr[0]);
  // }
  
  /*debug section end*/



  // for(int i=0;i<n;i++){
  //   cout << delete_seq[i]<< " ";
  // }
  // cout << endl;
  //20 5 25 13 29 6 22 28 10 18 26 9 23 21 27 0 1 14 4 16 11 7 15 2 3 24 19 8 12 17 

  //Delete half keys
  for (int i = 0; i < n/2 ; i++) {
    tree.Remove(delete_seq[i]);
    //tree.PrintTree(mgr[0]);
  }

  //tree.PrintTree(mgr[1]);


  // Check valid
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}