/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "common/value.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

UpdateStmt::UpdateStmt(Table *table, const FieldMeta *field_meta, const Value &value, FilterStmt *filter_stmt)
    : table_(table), field_meta_(field_meta), value_(value), filter_stmt_(filter_stmt)
{}

UpdateStmt::~UpdateStmt()
{
  if (filter_stmt_ != nullptr) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  stmt = nullptr;
  RC rc = RC::SUCCESS;
  const char *table_name = update.relation_name.c_str();
  const char *field_name = update.attribute_name.c_str();
  if (db == nullptr || common::is_blank(table_name) || common::is_blank(field_name)) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, field_name=%p", db, table_name, field_name);
    return RC::INVALID_ARGUMENT;
  }

  Table *table = db->find_table(table_name);
  if (table == nullptr) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  const TableMeta &table_meta = table->table_meta();
  const FieldMeta *field_meta = table_meta.field(field_name);
  if (field_meta == nullptr) {
    LOG_WARN("no such field. table=%s, field=%s", table_name, field_name);
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }
  if (!field_meta->visible()) {
    LOG_WARN("cannot update invisible(system) field. table=%s, field=%s", table_name, field_name);
    return RC::INVALID_ARGUMENT;
  }

  // 类型检查与转换（参考 database_project）：保证 SET 的值与列类型兼容
  Value value_to_set = update.value;
  if (field_meta->type() != value_to_set.attr_type()) {
    Value casted;
    rc = Value::cast_to(value_to_set, field_meta->type(), casted);
    if (OB_FAIL(rc)) {
      LOG_WARN("field type mismatch. table=%s, field=%s, field_type=%d, value_type=%d",
          table_name, field_name, (int)field_meta->type(), (int)value_to_set.attr_type());
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
    value_to_set = std::move(casted);
  }

  unordered_map<string, Table *> table_map;
  table_map.emplace(string(table_name), table);

  FilterStmt *filter_stmt = nullptr;
  rc          = FilterStmt::create(db,
      table,
      &table_map,
      update.conditions.data(),
      static_cast<int>(update.conditions.size()),
      filter_stmt);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create filter statement. rc=%s", strrc(rc));
    return rc;
  }

  stmt = new UpdateStmt(table, field_meta, value_to_set, filter_stmt);
  return RC::SUCCESS;
}
