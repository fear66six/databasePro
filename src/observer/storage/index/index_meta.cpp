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
// Created by Wangyunlai.wyl on 2021/5/18.
//

#include "storage/index/index_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/field/field_meta.h"
#include "storage/table/table_meta.h"
#include "json/json.h"

const static Json::StaticString FIELD_NAME("name");
const static Json::StaticString FIELD_FIELD_NAME("field_name");
const static Json::StaticString FIELD_FIELD_NAMES("field_names");

RC IndexMeta::init(const char *name, const FieldMeta &field)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }

  name_   = name;
  fields_ = {field.name()};
  return RC::SUCCESS;
}

RC IndexMeta::init(const char *name, const vector<const FieldMeta *> &fields)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }
  if (fields.empty()) {
    LOG_ERROR("Failed to init index, no fields.");
    return RC::INVALID_ARGUMENT;
  }

  name_.clear();
  fields_.clear();
  name_ = name;
  for (const FieldMeta *f : fields) {
    fields_.push_back(f->name());
  }
  return RC::SUCCESS;
}

void IndexMeta::to_json(Json::Value &json_value) const
{
  json_value[FIELD_NAME] = name_;
  if (fields_.size() == 1) {
    json_value[FIELD_FIELD_NAME] = fields_[0];  // 兼容旧格式
  }
  Json::Value arr(Json::arrayValue);
  for (const string &f : fields_) {
    arr.append(f);
  }
  json_value[FIELD_FIELD_NAMES] = arr;
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
{
  const Json::Value &name_value = json_value[FIELD_NAME];
  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  // 新格式：field_names 数组
  const Json::Value &field_names_value = json_value[FIELD_FIELD_NAMES];
  if (field_names_value.isArray() && !field_names_value.empty()) {
    vector<const FieldMeta *> fields;
    for (Json::ArrayIndex i = 0; i < field_names_value.size(); i++) {
      const Json::Value &fv = field_names_value[i];
      if (!fv.isString()) {
        LOG_ERROR("Field name of index [%s] is not a string", name_value.asCString());
        return RC::INTERNAL;
      }
      const FieldMeta *field = table.field(fv.asCString());
      if (nullptr == field) {
        LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), fv.asCString());
        return RC::SCHEMA_FIELD_MISSING;
      }
      fields.push_back(field);
    }
    return index.init(name_value.asCString(), fields);
  }

  // 兼容旧格式：field_name 单字段
  const Json::Value &field_value = json_value[FIELD_FIELD_NAME];
  if (!field_value.isString()) {
    LOG_ERROR("Field name of index [%s] is not a string. json value=%s",
        name_value.asCString(), field_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  const FieldMeta *field = table.field(field_value.asCString());
  if (nullptr == field) {
    LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_value.asCString());
    return RC::SCHEMA_FIELD_MISSING;
  }

  return index.init(name_value.asCString(), *field);
}

const char *IndexMeta::name() const { return name_.c_str(); }

const char *IndexMeta::field() const { return fields_.empty() ? "" : fields_[0].c_str(); }

const char *IndexMeta::field(int i) const
{
  if (i < 0 || i >= static_cast<int>(fields_.size())) {
    return "";
  }
  return fields_[i].c_str();
}

void IndexMeta::desc(ostream &os) const
{
  os << "index name=" << name_ << ", fields=";
  for (size_t i = 0; i < fields_.size(); i++) {
    if (i > 0) os << ",";
    os << fields_[i];
  }
}