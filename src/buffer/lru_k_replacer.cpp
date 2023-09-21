//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <exception>
#include "common/config.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  *frame_id = -1;
  for (const auto &[k, v] : mp_) {
    if (v.enable_evit_) {
      if (*frame_id == -1 || Judge(k, *frame_id)) {
        *frame_id = k;
      }
    }
  }
  if (*frame_id != -1) {
    mp_.erase(*frame_id);
    curr_size_--;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  mp_[frame_id].time_.push(++current_timestamp_);
  if (mp_[frame_id].time_.size() > k_) {
    mp_[frame_id].time_.pop();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  if (mp_[frame_id].enable_evit_ != set_evictable) {
    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }
    mp_[frame_id].enable_evit_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (mp_.count(frame_id) == 0U) {
    return;
  }

  if (!mp_[frame_id].enable_evit_) {
    throw std::exception();
  }

  mp_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::Judge(frame_id_t s, frame_id_t t) -> bool {
  if (mp_[s].time_.size() < k_ && mp_[t].time_.size() == k_) {
    return true;
  }
  if (mp_[s].time_.size() == k_ && mp_[t].time_.size() < k_) {
    return false;
  }
  return mp_[s].time_.front() < mp_[t].time_.front();
}

}  // namespace bustub
