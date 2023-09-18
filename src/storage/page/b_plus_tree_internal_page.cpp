//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <iostream>
#include <iterator>
#include <sstream>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key;}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) ->void { array_[index].second = value; }


/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(int value) -> int {
  auto iter = std::find_if(array_, array_ + GetSize(), [&value](const auto &pair) {
    return pair.second == value;
  });
  return std::distance(array_, iter);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookUp(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  auto taeget = std::lower_bound(array_ + 1, array_ + GetSize(), key, [&comparator](const auto &pair1, auto key) {
    return comparator(pair1.first, key);
  });
  // why?
  if(taeget == array_ + GetSize()) {
    return ValueAt(GetSize() - 1);
  }
  if(comparator(taeget->first, key) == 0) {
    return taeget->second;
  }
  return std::prev(taeget)->second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage* new_internal, BufferPoolManager* buffer_pool_manager) -> void {
  int new_size = GetMinSize();
  new_internal->CopyData(array_ + new_size, GetSize() - new_size, buffer_pool_manager);
  SetSize(new_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyData(MappingType* items, int size, BufferPoolManager *buffer_pool_manager) -> void {
  std::copy(items, items + size, array_);
  IncreaseSize(size);
  for(int i = 0; i < size; i++) {
    Page *page = buffer_pool_manager->FetchPage(ValueAt(i));
    auto* internal_page = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    internal_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(page_id_t new_page_id, const KeyType & key, page_id_t old_page_id) -> void {
  int index = ValueAt(old_page_id) + 1;
  std::move_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
  IncreaseSize(1);
  array_[index].first = key;
  array_[index].second = new_page_id;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
