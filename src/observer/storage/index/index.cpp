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
// Created by wangyunlai.wyl on 2021/5/19.
//

#include <climits>

#include "storage/index/index.h"
#include "common/value.h"

RC Index::init(const IndexMeta &index_meta, const FieldMeta &field_meta)
{
  index_meta_ = index_meta;
  field_metas_.clear();
  field_metas_.push_back(field_meta);
  return RC::SUCCESS;
}

RC Index::init(const IndexMeta &index_meta, const vector<const FieldMeta *> &field_metas)
{
  index_meta_ = index_meta;
  this->field_metas_.clear();
  for (const FieldMeta *fm : field_metas) {
    this->field_metas_.push_back(*fm);
  }
  return RC::SUCCESS;
}

int Index::build_prefix_range_keys(const Value &first_field_value, char *left_key, char *right_key, int key_buf_size)
{
  if (field_metas_.size() <= 1) {
    return 0;
  }
  int total_len = 0;
  for (const FieldMeta &fm : field_metas_) {
    total_len += fm.len();
  }
  if (total_len > key_buf_size) {
    return 0;
  }
  int offset = 0;
  memcpy(left_key, first_field_value.data(), field_metas_[0].len());
  memcpy(right_key, first_field_value.data(), field_metas_[0].len());
  offset += field_metas_[0].len();
  for (size_t i = 1; i < field_metas_.size(); i++) {
    const FieldMeta &fm = field_metas_[i];
    AttrType         t  = fm.type();
    if (t == AttrType::INTS) {
      int32_t min_val = INT32_MIN;
      int32_t max_val = INT32_MAX;
      memcpy(left_key + offset, &min_val, 4);
      memcpy(right_key + offset, &max_val, 4);
    } else if (t == AttrType::FLOATS) {
      float min_val = -1e38f;
      float max_val = 1e38f;
      memcpy(left_key + offset, &min_val, 4);
      memcpy(right_key + offset, &max_val, 4);
    } else if (t == AttrType::CHARS) {
      memset(left_key + offset, 0, fm.len());
      memset(right_key + offset, 0xFF, fm.len());
    } else if (t == AttrType::DATES) {
      int32_t min_val = 19700101;
      int32_t max_val = 99991231;
      memcpy(left_key + offset, &min_val, 4);
      memcpy(right_key + offset, &max_val, 4);
    } else {
      memset(left_key + offset, 0, fm.len());
      memset(right_key + offset, 0xFF, fm.len());
    }
    offset += fm.len();
  }
  return total_len;
}
