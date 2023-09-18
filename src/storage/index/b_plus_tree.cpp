#include <cassert>
#include <future>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "storage/page/page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key) const -> Page * {
  BUSTUB_ASSERT(root_page_id_ != INVALID_PAGE_ID, "Invalid root page id.");
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while(!tree_page->IsLeafPage()) {
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
  std::cout << "Get value, ket: " << key << std::endl;
  Page *page = FindLeaf(key);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value;
  bool is_exist = leaf_page->LookUp(key, &value, comparator_);
  if(is_exist) {
    result->push_back(value);
  }
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
  std::cout << "Insert operation, Key: " << key.ToString() << ", value: " << value.GetSlotNum() << std::endl;
  if(IsEmpty()) {
    Page *new_page = buffer_pool_manager_->NewPage(&root_page_id_);
    if(new_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Allocate new page failed.");
    }
    auto *new_leaf = reinterpret_cast<LeafPage *>(new_page->GetData());
    new_leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    new_leaf->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId(1);
    return true;
  }

  Page *page = FindLeaf(key);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int old_size = leaf_page->GetSize();
  int new_size = leaf_page->Insert(key, value, comparator_);
  if(new_size == old_size) {
    std::cout << "Insert operation, b plus tree has the key, return false." << std::endl;
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  }

  if(new_size <= leaf_max_size_) {
    std::cout << "Insert opeartion, normal insert." << std::endl;
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }

  std::cout << "Insert operation, split into new leaf." << std::endl;

  auto *new_leaf = reinterpret_cast<LeafPage*>(Split(leaf_page));
  leaf_page->SetNextPageId(new_leaf->GetPageId());
  new_leaf->SetNextPageId(leaf_page->GetPageId());
  
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  return false; 
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(BPlusTreePage *page) -> BPlusTreePage * {
  page_id_t page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&page_id);
  if(new_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "New page failed.");
  }
  // two split ways
  if(page->IsLeafPage()) {
    auto *new_leaf = reinterpret_cast<LeafPage *>(new_page->GetData());
    auto *leaf_page = reinterpret_cast<LeafPage *>(page);
    new_leaf->Init(page_id, leaf_page->GetParentPageId(), leaf_max_size_);
    leaf_page->MoveHalfTo(new_leaf);
  } else { // Internal Page
    auto *internal_page = reinterpret_cast<InternalPage *>(page);
    auto *new_internal = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_internal->Init(page_id, internal_page->GetParentPageId(), leaf_max_size_);
    internal_page->MoveHalfTo(new_internal, buffer_pool_manager_);
  }
  return reinterpret_cast<BPlusTreePage *>(new_page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertToParent(BPlusTreePage* old_page, BPlusTreePage* split_page, const KeyType &split_key) -> void{
  if(old_page->IsRootPage()) {
    Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
    auto *root = reinterpret_cast<InternalPage *>(page->GetData());
    root->Init(root_page_id_);
    
    root->SetKeyAt(1, split_key);
    root->SetValueAt(1, split_page->GetPageId());
    root->SetValueAt(0, old_page->GetPageId());
    root->SetSize(1);

    old_page->SetParentPageId(root->GetPageId());
    split_page->SetParentPageId(root->GetPageId());
    UpdateRootPageId(0);

    buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
    return;
  }

  int parent_id = old_page->GetParentPageId();
  Page* parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto* parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if(parent->GetSize() < internal_max_size_) {
    parent->InsertNodeAfter(split_page->GetPageId(), split_key, old_page->GetPageId());
    buffer_pool_manager_->UnpinPage(parent_id, true);
    return;
  }

  parent->InsertNodeAfter(split_page->GetPageId(), split_key, old_page->GetPageId());
  auto *new_parent_page = reinterpret_cast<InternalPage *>(Split(parent));
  InsertToParent(parent, new_parent_page, split_key);
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
  std::cout << "Remove opearation, key: " << key.ToString() << std:: endl;
  if(IsEmpty()) {
    return;
  }

  Page *page = FindLeaf(key);
  auto* leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  bool ret = leaf_page->Remove(key, comparator_);
  if(!ret) {
    return;
  }
  if(leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    return;
  }
  ResdistributeOrMerge(leaf_page);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ResdistributeOrMerge(BPlusTreePage* remove_page) -> void {
  Page *page = buffer_pool_manager_->FetchPage(remove_page->GetParentPageId());
  auto *parent_page = reinterpret_cast<InternalPage*>(page->GetData());
  int index = parent_page->ValueIndex(remove_page->GetPageId());
  if(index > 0) {
    Page *left_buffer_page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(index-1));
    auto *left_page = reinterpret_cast<InternalPage*>(left_buffer_page);
    if(left_page->GetSize() > left_page->GetMinSize()) {
      DistributeLeft(left_page, remove_page, parent_page, index);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(left_page->GetPageId(), true);
      return;
    }
  }

}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DistributeLeft(BPlusTreePage* distribute_page, BPlusTreePage* remove_page, BPlusTreePage* parent_page, int index) -> void {

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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return 0; }

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
