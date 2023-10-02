#include "../../src/include/primer/trie.h"
#include <map>
#include <memory>
#include <string_view>
#include "../../src/include/common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  auto root = this->GetRoot();

  for (char k : key) {
    if (root == nullptr) {
      return nullptr;
    }
    if (root->children_.find(k) == root->children_.end()) {
      return nullptr;
    }
    root = (root->children_).find(k)->second;
  }
  if (root == nullptr) {
    return nullptr;
  }
  auto target = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(root);
  if (target == nullptr) {
    return nullptr;
  }
  return &(*(target->value_));

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  auto v = std::make_shared<T>(std::move(value));
  auto temp = this->root_;
  std::shared_ptr<TrieNode> new_root;
  if (temp == nullptr) {
    new_root = std::make_shared<TrieNode>();
  } else {
    auto root_temp = temp->Clone();
    new_root = std::shared_ptr<TrieNode>(std::move(root_temp));
  }
  if (key.empty()) {
    new_root = std::make_shared<TrieNodeWithValue<T>>(new_root->children_, v);
  }
  auto curr = new_root;
  size_t index = 1;
  size_t length = key.size();
  for (char c : key) {
    std::shared_ptr<TrieNode> child_curr;
    if (temp != nullptr && temp->children_.find(c) != temp->children_.end()) {
      auto child = temp->children_.find(c)->second->Clone();
      child_curr = std::shared_ptr<TrieNode>(std::move(child));
      if (index == length) {
        auto leaf = std::make_shared<TrieNodeWithValue<T>>(child_curr->children_, v);
        child_curr = leaf;
      }
      curr->children_[c] = child_curr;
      temp = temp->children_.find(c)->second;
    } else {
      if (index == length) {
        child_curr = std::make_shared<TrieNodeWithValue<T>>(v);
      } else {
        child_curr = std::make_shared<TrieNode>();
      }
      curr->children_[c] = child_curr;
      temp = nullptr;
    }
    curr = child_curr;
    index += 1;
  }
  return Trie(new_root);
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  auto temp = this->root_;
  if (temp == nullptr) {
    return *this;
  }
  auto root_temp = temp->Clone();
  auto new_root = std::shared_ptr<TrieNode>(std::move(root_temp));
  auto curr = new_root;
  size_t length = key.size();
  size_t index = 1;
  char k;
  if (key.empty()) {
    if (temp->is_value_node_) {
      new_root = std::make_shared<TrieNode>(temp->children_);
      return Trie(new_root);
    }
    return *this;
  }
  std::shared_ptr<TrieNode> non_empty = nullptr;
  for (char c : key) {
    if (temp == nullptr) {
      return *this;
    }
    if (temp->children_.size() > 1 || temp->is_value_node_) {
      non_empty = curr;
      k = c;
    }
    std::shared_ptr<TrieNode> child_curr;
    if (temp->children_.find(c) != temp->children_.end()) {
      auto child = temp->children_.find(c)->second->Clone();
      child_curr = std::shared_ptr<TrieNode>(std::move(child));
      if (index == length) {
        if (child_curr->children_.empty()) {
          child_curr = nullptr;
        } else {
          child_curr = std::make_shared<TrieNode>(TrieNode(child_curr->children_));
        }
        curr->children_[c] = child_curr;
      } else {
        curr->children_[c] = child_curr;
        temp = temp->children_.find(c)->second;
      }
    } else {
      return *this;
    }
    curr = child_curr;
    index += 1;
  }
  Trie new_t;
  if (curr == nullptr) {
    if (non_empty == nullptr) {
      new_t = Trie();
    } else {
      non_empty->children_.erase(k);
      new_t = Trie(new_root);
    }
    return new_t;
  }
  return Trie(new_root);
  // return Trie(new_root);
  //  You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  //  you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
