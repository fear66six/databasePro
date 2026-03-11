/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/update_physical_operator.h"

#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "sql/expr/tuple.h"

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, const FieldMeta *field_meta, const Value &value)
    : table_(table), field_meta_(field_meta), value_(value), trx_(nullptr)
{}

RC UpdatePhysicalOperator::open(Trx *trx)
{
  trx_ = trx;
  if (children_.empty()) {
    LOG_WARN("UpdatePhysicalOperator::open has no child, table=%s", table_ ? table_->name() : "null");
    return RC::SUCCESS;
  }

  RC rc = children_[0]->open(trx);
  if (OB_FAIL(rc)) {
    LOG_WARN("UpdatePhysicalOperator: failed to open child. rc=%s", strrc(rc));
    return rc;
  }

  // 与 DELETE 一致：在 open 中遍历子算子，收集待更新记录（拷贝数据避免 close 后悬空），再逐条 delete + insert
  vector<Record> old_records;
  while (OB_SUCC(rc = children_[0]->next())) {
    Tuple *tuple = children_[0]->current_tuple();
    if (tuple == nullptr) {
      LOG_WARN("child current tuple is null");
      return RC::INTERNAL;
    }
    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record   &record   = row_tuple->record();

    Record rec_copy;
    rc = rec_copy.copy_data(record.data(), record.len());
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to copy record data. rc=%s", strrc(rc));
      return rc;
    }
    rec_copy.set_rid(record.rid());
    old_records.emplace_back(std::move(rec_copy));
  }

  children_[0]->close();

  if (rc != RC::RECORD_EOF) {
    LOG_WARN("child operator error. rc=%s", strrc(rc));
    return rc;
  }

  for (Record &old_record : old_records) {
    Record new_record;
    rc = build_new_record(old_record, new_record);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to build new record. rc=%s", strrc(rc));
      return rc;
    }
    rc = trx_->delete_record(table_, old_record);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to delete old record. rc=%s", strrc(rc));
      return rc;
    }
    rc = trx_->insert_record(table_, new_record);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to insert new record. rc=%s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  if (!children_.empty()) {
    RC rc = children_[0]->close();
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to close child operator. rc=%s", strrc(rc));
    }
  }
  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  schema.clear();
  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::build_new_record(const Record &old_record, Record &new_record) const
{
  if (table_ == nullptr || field_meta_ == nullptr) {
    return RC::INTERNAL;
  }

  const TableMeta &table_meta = table_->table_meta();
  const int        sys_fields = table_meta.sys_field_num();
  const int        user_fields = table_meta.field_num() - sys_fields;

  RowTuple tuple;
  tuple.set_schema(table_, table_meta.field_metas());
  tuple.set_record(const_cast<Record *>(&old_record));

  vector<Value> values;
  values.reserve(user_fields);

  for (int i = 0; i < user_fields; i++) {
    const FieldMeta *field = table_meta.field(i + sys_fields);
    if (field == nullptr) {
      return RC::INTERNAL;
    }
    if (strcmp(field->name(), field_meta_->name()) == 0) {
      values.emplace_back(value_);
    } else {
      Value cell;
      RC rc = tuple.cell_at(i + sys_fields, cell);
      if (OB_FAIL(rc)) {
        return rc;
      }
      values.emplace_back(std::move(cell));
    }
  }

  return table_->make_record(static_cast<int>(values.size()), values.data(), new_record);
}
