#include <endian.h>
#include <cassert>
#include <cstddef>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "storage/page/page.h"
#include "type/limits.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  std::cout << "leaf_max_size: " << leaf_max_size << std::endl;
  std::cout << "internal_max_size: " << internal_max_size << std::endl;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key) -> Page * {
  BUSTUB_ASSERT(root_page_id_ != INVALID_PAGE_ID, "Invalid root page id.");

  // LOG_INFO("FindLeaf, root_page_id_ = %d", root_page_id_);
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!tree_page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(tree_page);
    auto page_id = internal_page->LookUp(key, comparator_);
    page = buffer_pool_manager_->FetchPage(page_id);
    tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return page;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  root_page_id_latch_.RLock();

  // std::cout << "Get value, key: " << key << std::endl;

  Page *page = FindLeaf(key);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value;
  bool is_exist = leaf_page->LookUp(key, &value, comparator_);
  // LOG_INFO("GetValue, page_id : %d", page->GetPageId());
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  if (is_exist) {
    result->push_back(value);
  }

  root_page_id_latch_.RUnlock();
  return is_exist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_page_id_latch_.WLock();

  std::cout << "Insert operation, Key: " << key.ToString() << ", value: " << value.GetSlotNum() << std::endl;
  if (IsEmpty()) {
    Page *new_page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (new_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Allocate new page failed.");
    }
    auto *new_leaf = reinterpret_cast<LeafPage *>(new_page->GetData());
    new_leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    new_leaf->Insert(key, value, comparator_);
    // std::cout << "GetSize() : " << new_leaf->GetSize() << std::endl;
    new_leaf->SetNextPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId(1);

    root_page_id_latch_.WUnlock();
    return true;
  }

  Page *page = FindLeaf(key);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int old_size = leaf_page->GetSize();
  int new_size = leaf_page->Insert(key, value, comparator_);
  if (new_size == old_size) {
    // std::cout << "Insert operation, b plus tree has the key, return false." << std::endl;
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

    root_page_id_latch_.WUnlock();
    return false;
  }

  if (new_size <= leaf_max_size_) {
    // std::cout << "Insert opeartion, normal insert." << std::endl;
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);

    root_page_id_latch_.WUnlock();
    return true;
  }

  // std::cout << "Insert operation, split into new leaf." << std::endl;

  auto *new_leaf = reinterpret_cast<LeafPage *>(Split(leaf_page));
  new_leaf->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf->GetPageId());

  InsertToParent(leaf_page, new_leaf, new_leaf->KeyAt(0));
  // std::cout << "haved InsertToParent" << std::endl;
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);

  root_page_id_latch_.WUnlock();
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(BPlusTreePage *page) -> BPlusTreePage * {
  // LOG_INFO(" Start to split, page_id = %d", page->GetPageId());

  page_id_t page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&page_id);
  if (new_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "New page failed.");
  }
  // two split ways
  if (page->IsLeafPage()) {
    auto *new_leaf = reinterpret_cast<LeafPage *>(new_page->GetData());
    auto *leaf_page = reinterpret_cast<LeafPage *>(page);
    new_leaf->Init(page_id, leaf_page->GetParentPageId(), leaf_max_size_);
    leaf_page->MoveHalfTo(new_leaf);
  } else {  // Internal Page
    auto *internal_page = reinterpret_cast<InternalPage *>(page);
    auto *new_internal = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_internal->Init(page_id, internal_page->GetParentPageId(), internal_max_size_);
    internal_page->MoveHalfTo(new_internal, buffer_pool_manager_);
  }
  return reinterpret_cast<BPlusTreePage *>(new_page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertToParent(BPlusTreePage *old_page, BPlusTreePage *split_page, const KeyType &split_key)
    -> void {
  // LOG_INFO(" Start to InsertToParent, old_page_id = %d, new_page_id = %d", old_page->GetPageId(),
  //          split_page->GetPageId());

  if (old_page->IsRootPage()) {
    // std::cout << "Build a new root page" << std::endl;
    Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
    auto *root = reinterpret_cast<InternalPage *>(page->GetData());
    root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    root->PrintAllKV();

    root->SetKeyAt(1, split_key);
    root->SetValueAt(1, split_page->GetPageId());
    root->SetValueAt(0, old_page->GetPageId());
    root->SetSize(2);

    old_page->SetParentPageId(root_page_id_);
    split_page->SetParentPageId(root_page_id_);

    reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(static_cast<page_id_t>(6)))->PrintAllKV();
    // std::cout << "UpdateRootPageId" << std::endl;
    UpdateRootPageId(0);

    reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(static_cast<page_id_t>(6)))->PrintAllKV();
    buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
    return;
  }

  int parent_id = old_page->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if (parent->GetSize() < internal_max_size_) {
    parent->InsertNodeAfter(split_page->GetPageId(), split_key, old_page->GetPageId());
    buffer_pool_manager_->UnpinPage(parent_id, true);
    return;
  }

  parent->InsertNodeAfter(split_page->GetPageId(), split_key, old_page->GetPageId());
  parent->PrintAllKV();
  auto *new_parent_page = reinterpret_cast<InternalPage *>(Split(parent));
  new_parent_page->PrintAllKV();
  InsertToParent(parent, new_parent_page, new_parent_page->KeyAt(0));
  buffer_pool_manager_->UnpinPage(parent_id, true);
  buffer_pool_manager_->UnpinPage(new_parent_page->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_page_id_latch_.WLock();

  std::cout << "Remove opearation, key: " << key.ToString() << std::endl;
  if (IsEmpty()) {
    root_page_id_latch_.WUnlock();
    return;
  }

  Page *page = FindLeaf(key);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  bool ret = leaf_page->Remove(key, comparator_);
  if (!ret) {
    root_page_id_latch_.WUnlock();
    return;
  }
  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    root_page_id_latch_.WUnlock();
    return;
  }

  ResdistributeOrMerge(leaf_page);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);

  root_page_id_latch_.WUnlock();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ResdistributeOrMerge(BPlusTreePage *remove_page) -> void {
  // std::cout << "ResdistributeOrMerge" << std::endl;

  if (remove_page->IsRootPage()) {
    if (!remove_page->IsLeafPage() && remove_page->GetSize() == 1) {
      auto *root_page = reinterpret_cast<InternalPage *>(remove_page);
      auto only_chirld_page = buffer_pool_manager_->FetchPage(root_page->ValueAt(0));
      auto *only_chirld = reinterpret_cast<BPlusTreePage *>(only_chirld_page->GetData());
      only_chirld->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = only_chirld->GetPageId();
      UpdateRootPageId(0);
      buffer_pool_manager_->UnpinPage(only_chirld->GetPageId(), true);
      return;
    }
    if (remove_page->IsLeafPage() && remove_page->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      return;
    }
    return;
  }
  Page *page = buffer_pool_manager_->FetchPage(remove_page->GetParentPageId());
  auto *parent_page = reinterpret_cast<InternalPage *>(page->GetData());
  int index = parent_page->ValueIndex(remove_page->GetPageId());

  if (index > 0) {
    Page *left_buffer_page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(index - 1));
    auto *left_page = reinterpret_cast<BPlusTreePage *>(left_buffer_page->GetData());
    if (left_page->GetSize() > left_page->GetMinSize()) {
      DistributeLeft(left_page, remove_page, parent_page, index);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(left_page->GetPageId(), true);
      return;
    }
    buffer_pool_manager_->UnpinPage(left_page->GetPageId(), false);
  }

  if (index < parent_page->GetSize() - 1) {
    Page *right_buffer_page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(index + 1));
    auto *right_page = reinterpret_cast<BPlusTreePage *>(right_buffer_page->GetData());
    if (right_page->GetSize() > right_page->GetMinSize()) {
      DistributeRight(right_page, remove_page, parent_page, index);
      buffer_pool_manager_->UnpinPage(right_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(remove_page->GetPageId(), true);
      return;
    }
    buffer_pool_manager_->UnpinPage(right_page->GetPageId(), false);
  }

  if (index > 0) {
    Page *left_buffer_page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(index - 1));
    auto *left_page = reinterpret_cast<BPlusTreePage *>(left_buffer_page->GetData());
    // std::cout << "Merge left" << std::endl;
    Merge(left_page, remove_page, parent_page, index);
    buffer_pool_manager_->UnpinPage(left_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(remove_page->GetPageId(), true);
    return;
  }

  if (index < parent_page->GetSize() - 1) {
    Page *right_buffer_page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(index + 1));
    auto *right_page = reinterpret_cast<BPlusTreePage *>(right_buffer_page->GetData());
    // std::cout << "Merge right" << std::endl;
    Merge(remove_page, right_page, parent_page, index + 1);
    buffer_pool_manager_->UnpinPage(right_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(remove_page->GetPageId(), true);
    return;
  }

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DistributeLeft(BPlusTreePage *distribute_page, BPlusTreePage *remove_page,
                                    InternalPage *parent_page, int index) -> void {
  // yao chongxin fen pei de na ge jie dian
  // std::cout << "DistributeLeft" << std::endl;

  KeyType key;
  if (distribute_page->IsLeafPage()) {
    auto *distribute_leaf_page = reinterpret_cast<LeafPage *>(distribute_page);
    auto *remove_leaf_page = reinterpret_cast<LeafPage *>(remove_page);
    int leaf_index = distribute_leaf_page->GetSize() - 1;
    key = distribute_leaf_page->KeyAt(leaf_index);
    remove_leaf_page->Insert(key, distribute_leaf_page->ValueAt(leaf_index), comparator_);
    distribute_page->IncreaseSize(-1);
  } else {
    auto *distribute_internal_page = reinterpret_cast<InternalPage *>(distribute_page);
    auto *remove_internal_page = reinterpret_cast<InternalPage *>(remove_page);
    int internal_index = distribute_internal_page->GetSize() - 1;
    key = distribute_internal_page->KeyAt(internal_index);
    remove_internal_page->InsertStart(key, distribute_internal_page->ValueIndex(internal_index), buffer_pool_manager_);
    distribute_page->IncreaseSize(-1);
  }
  parent_page->SetKeyAt(index, key);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DistributeRight(BPlusTreePage *distribute_page, BPlusTreePage *merge_page,
                                     InternalPage *parent_page, int index) -> void {
  // std::cout << "DistributeRight" << std::endl;
  KeyType key;
  if (distribute_page->IsLeafPage()) {
    auto *distribute_leaf_page = reinterpret_cast<LeafPage *>(distribute_page);
    auto *merge_leaf_page = reinterpret_cast<LeafPage *>(merge_page);
    int leaf_index = 0;
    key = distribute_leaf_page->KeyAt(leaf_index);
    merge_leaf_page->Insert(key, distribute_leaf_page->ValueAt(leaf_index), comparator_);
    distribute_leaf_page->Remove(key, comparator_);
    parent_page->SetKeyAt(index + 1, distribute_leaf_page->KeyAt(leaf_index));
  } else {
    auto *distribute_internal_page = reinterpret_cast<InternalPage *>(distribute_page);
    auto *merge_leaf_page = reinterpret_cast<InternalPage *>(merge_page);
    int internal_page_index = 1;
    key = distribute_internal_page->KeyAt(internal_page_index);
    merge_leaf_page->InsertEnd(key, distribute_internal_page->ValueAt(internal_page_index), buffer_pool_manager_);
    distribute_internal_page->Remove(1);
    parent_page->SetKeyAt(index + 1, distribute_internal_page->KeyAt(internal_page_index));
  }
  parent_page->SetKeyAt(index, key);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Merge(BPlusTreePage *left_page, BPlusTreePage *remove_page, InternalPage *parent_page, int index)
    -> void {
  if (left_page->IsLeafPage()) {
    auto *left_leaf_page = reinterpret_cast<LeafPage *>(left_page);
    auto *remove_leaf_page = reinterpret_cast<LeafPage *>(remove_page);
    remove_leaf_page->MoveAllTo(left_leaf_page);
  } else {
    auto *left_internal_page = reinterpret_cast<InternalPage *>(left_page);
    auto *remove_internal_page = reinterpret_cast<InternalPage *>(remove_page);
    remove_internal_page->MoveAllTo(left_internal_page, buffer_pool_manager_);
  }
  parent_page->Remove(index);
  if (parent_page->GetSize() < parent_page->GetMinSize()) {
    ResdistributeOrMerge(parent_page);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Releaselatch(Transaction *transaction) -> void {
  while (!transaction->GetPageSet()->empty()) {
    auto *page = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    if (page == nullptr) {
      root_page_id_latch_.WUnlock();
    } else {
      root_page_id_latch_.WUnlock();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  root_page_id_latch_.RLock();

  if (root_page_id_ == INVALID_PAGE_ID) {
    root_page_id_latch_.RUnlock();
    return INDEXITERATOR_TYPE(nullptr, nullptr, 0);
  }

  // std::cout << "Get the begin of the plus tree." << std::endl;

  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
  while (!tree_page->IsLeafPage()) {
    auto *internal_tree_page = reinterpret_cast<InternalPage *>(tree_page);
    page_id_t page_id = internal_tree_page->ValueAt(0);
    tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
    buffer_pool_manager_->UnpinPage(page_id, false);
  }
  auto *leaf = reinterpret_cast<LeafPage *>(tree_page);

  root_page_id_latch_.RUnlock();
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  root_page_id_latch_.RLock();

  if (root_page_id_ == INVALID_PAGE_ID) {
    root_page_id_latch_.RUnlock();
    return INDEXITERATOR_TYPE(nullptr, nullptr, 0);
  }

  // std::cout << "Get thr begin of the plus tree of specific key." << std::endl;

  Page *leaf_page = FindLeaf(key);
  auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int index = leaf->KeyIndex(key, comparator_);

  root_page_id_latch_.RUnlock();
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  root_page_id_latch_.RLock();

  if (root_page_id_ == INVALID_PAGE_ID) {
    root_page_id_latch_.RUnlock();
    return INDEXITERATOR_TYPE(nullptr, nullptr, 0);
  }
  // std::cout << "Get thr end of plus tree." << std::endl;

  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  // LOG_INFO("BPLUSTREE_TYPE::End(), root_page_id = %d", root_page->GetPageId());
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
  while (!tree_page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(tree_page);
    int index = internal_page->GetSize() - 1;
    page_id_t page_id = internal_page->ValueAt(index);
    tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
    // LOG_INFO("BPLUSTREE_TYPE::End(), tree_page_id = %d", tree_page->GetPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
  }
  auto *leaf = reinterpret_cast<LeafPage *>(tree_page);

  root_page_id_latch_.RUnlock();
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf, leaf->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
