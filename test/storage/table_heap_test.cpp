#include "storage/table_heap.h"
#include <cstdlib>
#include <unordered_map>
#include <vector>
#include <iostream>
#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 100;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);
  for (int i = 0; i < row_nums; i++) {
    //if(i%129) continue;
    int32_t len = 10;//RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    //std::cout<<"new Fields passed"<<std::endl;
    Row row(*fields);
    table_heap->InsertTuple(row, nullptr);//ASSERT_TRUE()
    //std::cout<<"table_heap->InsertTuple(row, nullptr) passed"<<std::endl;
    if (row_values.find(row.GetRowId().Get()) != row_values.end()) {
      std::cout << row.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    } else {
      row_values.emplace(row.GetRowId().Get(), fields);
      size++;
      //std::cout<<size<<std::endl;
    }
    delete[] characters;
  }


//    for (int i = 0; i < row_nums/10; i++) {
//    //if(i%129) continue;
//    int32_t len = 10;//RandomUtils::RandomInt(0, 64);
//    char *characters = new char[len];
//    RandomUtils::RandomString(characters, len);
//    Fields *fields =
//        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
//                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
//    std::cout<<"new Fields passed"<<std::endl;
//    Row row(*fields);
//      table_heap->InsertTuple(row, nullptr);//ASSERT_TRUE()
//    std::cout<<"table_heap->InsertTuple(row, nullptr) passed"<<std::endl;
//    if (row_values.find(row.GetRowId().Get()) != row_values.end()) {
//      std::cout << row.GetRowId().Get() << std::endl;
//      ASSERT_TRUE(false);
//    } else {
//        row_values.emplace(row.GetRowId().Get(), fields);
//        size++;
//      std::cout<<size<<std::endl;
//    }
//    delete[] characters;
//  }

  ASSERT_EQ(row_nums, row_values.size());
  ASSERT_EQ(row_nums, size);
  for (auto row_kv : row_values) {
    size--;
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  ASSERT_EQ(size, 0);
}
