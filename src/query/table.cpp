#include "query/table.h"

#include <limits>
#include <stdexcept>

namespace quorumdb {

Table::Table(BPlusTree *index) : index_(index) {
  if (index_ == nullptr) {
    throw std::invalid_argument("index must not be null");
  }
  buffer_pool_manager_ = index_->GetBufferPoolManager();
}

bool Table::Insert(const Record &record) {
  value_t existing_page_id = 0;
  if (index_->Search(record.id, &existing_page_id)) {
    return false;
  }

  page_id_t record_page_id = INVALID_PAGE_ID;
  Page *page = buffer_pool_manager_->NewPage(&record_page_id);
  if (page == nullptr) {
    return false;
  }
  SerializeRecord(record, page->GetData());
  buffer_pool_manager_->UnpinPage(record_page_id, true);

  // The B+ tree's int64 value stores the page ID of this serialized record.
  if (index_->Insert(record.id, static_cast<value_t>(record_page_id))) {
    return true;
  }
  buffer_pool_manager_->DeletePage(record_page_id);
  return false;
}

bool Table::Select(std::int64_t id, Record *out) {
  if (out == nullptr) {
    return false;
  }
  value_t record_page_id = 0;
  return index_->Search(id, &record_page_id) && ReadRecord(record_page_id, out);
}

std::vector<Record> Table::SelectWhere(const std::function<bool(const Record &)> &predicate) {
  std::vector<std::pair<key_t, value_t>> entries;
  index_->RangeScan(std::numeric_limits<key_t>::min(), std::numeric_limits<key_t>::max(), &entries);

  std::vector<Record> records;
  for (const auto &entry : entries) {
    Record record{};
    if (ReadRecord(entry.second, &record) && predicate(record)) {
      records.push_back(std::move(record));
    }
  }
  return records;
}

bool Table::Update(std::int64_t id, const std::function<void(Record &)> &mutator) {
  value_t record_page_id = 0;
  if (!index_->Search(id, &record_page_id)) {
    return false;
  }
  Page *page = buffer_pool_manager_->FetchPage(static_cast<page_id_t>(record_page_id));
  if (page == nullptr) {
    return false;
  }
  Record record = DeserializeRecord(page->GetData());
  mutator(record);
  record.id = id;
  SerializeRecord(record, page->GetData());
  buffer_pool_manager_->UnpinPage(static_cast<page_id_t>(record_page_id), true);
  return true;
}

bool Table::Delete(std::int64_t id) {
  value_t record_page_id = 0;
  if (!index_->Search(id, &record_page_id)) {
    return false;
  }
  if (!index_->Delete(id)) {
    return false;
  }
  return buffer_pool_manager_->DeletePage(static_cast<page_id_t>(record_page_id));
}

bool Table::ReadRecord(value_t record_page_id, Record *out) {
  const auto page_id = static_cast<page_id_t>(record_page_id);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    return false;
  }
  *out = DeserializeRecord(page->GetData());
  buffer_pool_manager_->UnpinPage(page_id, false);
  return true;
}

}  // namespace quorumdb
