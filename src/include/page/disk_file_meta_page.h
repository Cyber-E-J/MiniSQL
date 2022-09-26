#ifndef MINISQL_DISK_FILE_META_PAGE_H
#define MINISQL_DISK_FILE_META_PAGE_H

#include <cstdint>
#include <string.h>

#include "page/bitmap_page.h"

static constexpr page_id_t MAX_VALID_PAGE_ID = (PAGE_SIZE - 8) / 4 * BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();

//(PAGE_SIZE - 8) -> bytes of num_allocated_pages_ and uint32_t num_extents_
//every page need 4 bytes to store information

class DiskFileMetaPage {
public:
  uint32_t GetExtentNums() {//# allocated extents
    return num_extents_;
  }

  uint32_t GetAllocatedPages() {//# allocated pages
    return num_allocated_pages_;
  }

  uint32_t GetExtentUsedPage(uint32_t extent_id) {
    if (extent_id >= num_extents_) {
      return 0;
    }
    return extent_used_page_[extent_id];
  }

  //ctor
  explicit DiskFileMetaPage(char* meta_data){
    memcpy(&num_allocated_pages_, meta_data, sizeof(uint32_t));
    memcpy(&num_extents_, meta_data + sizeof(uint32_t), sizeof(uint32_t));
    memcpy(extent_used_page_, meta_data + 2 * sizeof(uint32_t), (PAGE_SIZE - 8) / 4 * sizeof(uint32_t));
  }
  DiskFileMetaPage(){}


public:
  uint32_t num_allocated_pages_;
  uint32_t num_extents_;   // each extent consists with a bit map and BIT_MAP_SIZE pages
  uint32_t extent_used_page_[(PAGE_SIZE - 8) / 4]={0}; //record the #used pages of the extent_id extent
};

#endif //MINISQL_DISK_FILE_META_PAGE_H
