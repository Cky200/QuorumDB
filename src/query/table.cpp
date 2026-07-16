#include "query/table.h"

#include <limits>
#include <stdexcept>

namespace quorumdb {

Table::Table(BPlusTree *index, WALManager *wal_manager) : index_(index), wal_manager_(wal_manager) {
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
  if (wal_manager_ != nullptr) {
    wal_manager_->AppendInsert(record.id, RecordImage(record));
    wal_manager_->Flush();
  }
  const bool inserted = InsertWithoutWAL(record);
  if (inserted && wal_manager_ != nullptr) {
    wal_manager_->AppendCommit();
  }
  return inserted;
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
  Record record{};
  if (!Select(id, &record)) {
    return false;
  }
  const std::string before_image = RecordImage(record);
  mutator(record);
  record.id = id;
  if (wal_manager_ != nullptr) {
    wal_manager_->AppendUpdate(id, before_image, RecordImage(record));
    wal_manager_->Flush();
  }
  const bool updated = UpdateWithoutWAL(id, record);
  if (updated && wal_manager_ != nullptr) {
    wal_manager_->AppendCommit();
  }
  return updated;
}

bool Table::Delete(std::int64_t id) {
  Record record{};
  if (!Select(id, &record)) {
    return false;
  }
  if (wal_manager_ != nullptr) {
    wal_manager_->AppendDelete(id, RecordImage(record));
    wal_manager_->Flush();
  }
  const bool deleted = DeleteWithoutWAL(id);
  if (deleted && wal_manager_ != nullptr) {
    wal_manager_->AppendCommit();
  }
  return deleted;
}

void Table::Recover() {
  if (wal_manager_ == nullptr) {
    return;
  }
  for (const LogRecord &record : wal_manager_->ReadAllRecords()) {
    switch (record.type) {
      case LogRecordType::INSERT:
        InsertWithoutWAL(RecordFromImage(record.after_image));
        break;
      case LogRecordType::UPDATE:
        UpdateWithoutWAL(record.record_id, RecordFromImage(record.after_image));
        break;
      case LogRecordType::DELETE:
        DeleteWithoutWAL(record.record_id);
        break;
      case LogRecordType::COMMIT:
        break;
    }
  }
}

bool Table::InsertWithoutWAL(const Record &record) {
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

bool Table::UpdateWithoutWAL(std::int64_t id, const Record &record) {
  value_t record_page_id = 0;
  if (!index_->Search(id, &record_page_id)) {
    return InsertWithoutWAL(record);
  }
  return WriteRecord(record_page_id, record);
}

bool Table::WriteRecord(value_t record_page_id, const Record &record) {
  Page *page = buffer_pool_manager_->FetchPage(static_cast<page_id_t>(record_page_id));
  if (page == nullptr) {
    return false;
  }
  SerializeRecord(record, page->GetData());
  buffer_pool_manager_->UnpinPage(static_cast<page_id_t>(record_page_id), true);
  return true;
}

bool Table::DeleteWithoutWAL(std::int64_t id) {
  value_t record_page_id = 0;
  if (!index_->Search(id, &record_page_id)) {
    return true;
  }
  if (!index_->Delete(id)) {
    return false;
  }
  return buffer_pool_manager_->DeletePage(static_cast<page_id_t>(record_page_id));
}

std::string Table::RecordImage(const Record &record) {
  std::string image(RECORD_SIZE, '\0');
  SerializeRecord(record, image.data());
  return image;
}

Record Table::RecordFromImage(const std::string &image) {
  if (image.size() != RECORD_SIZE) {
    throw std::runtime_error("invalid record image in WAL");
  }
  return DeserializeRecord(image.data());
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
