#ifndef MINISQL_INDEX_ITERATOR_H
#define MINISQL_INDEX_ITERATOR_H

#include "page/b_plus_tree_leaf_page.h"

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>


INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator(LeafPage *lp,int x,BufferPoolManager *bpm);
  explicit IndexIterator(){}
  ~IndexIterator();

  /** Return the key/value pair this iterator is currently pointing at. */
  const MappingType &operator*();

  /** Move to the next key/value pair.*/
  IndexIterator &operator++();

  /** Return whether two iterators are equal */
  bool operator==(const IndexIterator &itr) const;

  /** Return whether two iterators are not equal. */
  bool operator!=(const IndexIterator &itr) const;

private:
  // add your own private member variables here
  
  LeafPage *leaf_page;//在哪�?叶结点中
  int index;//在叶结点�?�?�?几个key [0,leaf_page->GetSize())
  BufferPoolManager *buffer_pool_manager;
};


#endif //MINISQL_INDEX_ITERATOR_H
