#include <algorithm>
#include <cassert>
#include <cstdio>
#include <string>
#include <vector>

#include "query/query_engine.h"

namespace {

std::string TestFilename(const char *name) {
  return std::string("test_query_engine_") + name + ".db";
}

void RemoveFile(const std::string &filename) { std::remove(filename.c_str()); }

struct QueryFixture {
  explicit QueryFixture(const std::string &filename)
      : disk_manager(filename), buffer_pool(16, &disk_manager), tree(&buffer_pool), engine(&tree) {}

  quorumdb::DiskManager disk_manager;
  quorumdb::BufferPoolManager buffer_pool;
  quorumdb::BPlusTree tree;
  quorumdb::QueryEngine engine;
};

void TestInsertAndSelect() {
  const auto filename = TestFilename("insert_select");
  RemoveFile(filename);
  {
    QueryFixture fixture(filename);
    assert(fixture.engine.Insert(1, "Ada", 100));
    assert(fixture.engine.Insert(2, "Grace", 200));
    assert(fixture.engine.Insert(3, "Linus", 300));
    for (const auto &expected : std::vector<quorumdb::Record>{{1, "Ada", 100}, {2, "Grace", 200},
                                                               {3, "Linus", 300}}) {
      const auto result = fixture.engine.SelectById(expected.id);
      assert(result.has_value());
      assert(result->id == expected.id);
      assert(result->name == expected.name);
      assert(result->value == expected.value);
    }
  }
  RemoveFile(filename);
}

void TestDuplicatePreservesOriginal() {
  const auto filename = TestFilename("duplicate");
  RemoveFile(filename);
  {
    QueryFixture fixture(filename);
    assert(fixture.engine.Insert(7, "original", 10));
    assert(!fixture.engine.Insert(7, "replacement", 20));
    const auto result = fixture.engine.SelectById(7);
    assert(result.has_value());
    assert(result->name == "original");
    assert(result->value == 10);
  }
  RemoveFile(filename);
}

void TestSelectWhere() {
  const auto filename = TestFilename("where");
  RemoveFile(filename);
  {
    QueryFixture fixture(filename);
    assert(fixture.engine.Insert(1, "low", 25));
    assert(fixture.engine.Insert(2, "high", 150));
    assert(fixture.engine.Insert(3, "higher", 250));
    const auto result = fixture.engine.SelectWhere(
        [](const quorumdb::Record &record) { return record.value > 100; });
    assert(result.size() == 2);
    assert(result[0].id == 2 && result[1].id == 3);
  }
  RemoveFile(filename);
}

void TestUpdateValue() {
  const auto filename = TestFilename("update");
  RemoveFile(filename);
  {
    QueryFixture fixture(filename);
    assert(fixture.engine.Insert(9, "unchanged-name", 1));
    assert(fixture.engine.UpdateValue(9, 999));
    const auto result = fixture.engine.SelectById(9);
    assert(result.has_value());
    assert(result->id == 9);
    assert(result->name == "unchanged-name");
    assert(result->value == 999);
  }
  RemoveFile(filename);
}

void TestDelete() {
  const auto filename = TestFilename("delete");
  RemoveFile(filename);
  {
    QueryFixture fixture(filename);
    assert(fixture.engine.Insert(1, "keep", 10));
    assert(fixture.engine.Insert(2, "remove", 20));
    assert(fixture.engine.DeleteById(2));
    assert(!fixture.engine.SelectById(2).has_value());
    const auto all = fixture.engine.SelectWhere([](const quorumdb::Record &) { return true; });
    assert(all.size() == 1);
    assert(all.front().id == 1);
  }
  RemoveFile(filename);
}

}  // namespace

int main() {
  TestInsertAndSelect();
  TestDuplicatePreservesOriginal();
  TestSelectWhere();
  TestUpdateValue();
  TestDelete();
  return 0;
}
