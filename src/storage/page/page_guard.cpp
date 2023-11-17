#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (bpm_ == nullptr) {
    page_ = nullptr;
    return;
  }
  if (page_ == nullptr) {
    bpm_ = nullptr;
    return;
  }
  bpm_->UnpinPage(this->PageId(), this->is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (page_ != nullptr && bpm_ != nullptr) {
    bpm_->UnpinPage(this->PageId(), is_dirty_);
  }
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  page_->RLatch();
  auto rpg = ReadPageGuard(bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;
  return rpg;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  page_->WLatch();
  auto wpg = WritePageGuard(bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;
  return wpg;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { guard_ = BasicPageGuard(std::move(that.guard_)); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this->guard_.page_ != nullptr && this->guard_.bpm_ != nullptr) {
    guard_.bpm_->UnpinPage(this->PageId(), guard_.is_dirty_);
    guard_.page_->RUnlatch();
  }
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.bpm_ == nullptr) {
    guard_.page_ = nullptr;
    return;
  }
  if (guard_.page_ == nullptr) {
    guard_.bpm_ = nullptr;
    return;
  }
  guard_.bpm_->UnpinPage(this->PageId(), guard_.is_dirty_);
  guard_.page_->RUnlatch();
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = BasicPageGuard(std::move(that.guard_)); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this->guard_.page_ != nullptr && this->guard_.bpm_ != nullptr) {
    guard_.bpm_->UnpinPage(this->PageId(), guard_.is_dirty_);
    guard_.page_->WUnlatch();
  }
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.bpm_ == nullptr) {
    if (guard_.page_ != nullptr) {
      guard_.page_->WUnlatch();
    }
    guard_.page_ = nullptr;
    return;
  }
  if (guard_.page_ == nullptr) {
    guard_.bpm_ = nullptr;
    return;
  }
  guard_.bpm_->UnpinPage(this->PageId(), guard_.is_dirty_);
  guard_.page_->WUnlatch();
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
