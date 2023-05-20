#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
  POOL_SIZE = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(!lru_list_.empty()) {//have pages
    *frame_id = lru_list_.back();//get the least used page
    lru_list_.pop_back();//replaced
    return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  auto to_pin_block = find(lru_list_.begin(), lru_list_.end(), frame_id);
  if(to_pin_block != lru_list_.end()) { //found
    lru_list_.erase(find(lru_list_.begin(), lru_list_.end(), frame_id));
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  auto pinned_block = find(lru_list_.begin(), lru_list_.end(), frame_id);
  if (pinned_block == lru_list_.end()) {  // not found, still pinned
    if (lru_list_.size() == POOL_SIZE) {
      lru_list_.pop_back();                // pop the unpinned page
    }
    lru_list_.insert(lru_list_.begin(), frame_id);
  }
}

size_t LRUReplacer::Size() {
  return lru_list_.size();
}