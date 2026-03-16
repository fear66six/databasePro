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
// Created by Wangyunlai on 2022/12/26.
//

#include "sql/optimizer/conjunction_simplification_rule.h"
#include "common/log/log.h"
#include "common/value.h"
#include "sql/expr/expression.h"

RC try_to_get_bool_constant(unique_ptr<Expression> &expr, bool &constant_value)
{
  if (expr->type() == ExprType::VALUE && expr->value_type() == AttrType::BOOLEANS) {
    auto value_expr = static_cast<ValueExpr *>(expr.get());
    constant_value  = value_expr->get_value().get_boolean();
    return RC::SUCCESS;
  }
  // 支持可折叠为布尔常量的比较表达式，如 1=0、1=1
  if (expr->type() == ExprType::COMPARISON) {
    auto *cmp_expr = static_cast<ComparisonExpr *>(expr.get());
    Value value;
    if (cmp_expr->try_get_value(value) == RC::SUCCESS && value.attr_type() == AttrType::BOOLEANS) {
      constant_value = value.get_boolean();
      return RC::SUCCESS;
    }
  }
  return RC::INTERNAL;
}
RC ConjunctionSimplificationRule::rewrite(unique_ptr<Expression> &expr, bool &change_made)
{
  RC rc = RC::SUCCESS;
  if (expr->type() != ExprType::CONJUNCTION) {
    return rc;
  }

  change_made                                                = false;
  auto                                      conjunction_expr = static_cast<ConjunctionExpr *>(expr.get());

  vector<unique_ptr<Expression>> &child_exprs      = conjunction_expr->children();

  // 先看看有没有能够直接去掉的表达式。比如AND时恒为true的表达式可以删除
  // 或者是否可以直接计算出当前表达式的值。比如AND时，如果有一个表达式为false，那么整个表达式就是false
  for (auto iter = child_exprs.begin(); iter != child_exprs.end();) {
    bool constant_value = false;

    rc                  = try_to_get_bool_constant(*iter, constant_value);
    if (rc != RC::SUCCESS) {
      rc = RC::SUCCESS;
      ++iter;
      continue;
    }

    if (conjunction_expr->conjunction_type() == ConjunctionExpr::Type::AND) {
      if (constant_value == true) {
        child_exprs.erase(iter);
      } else {
        // AND with constant false: whole expression is false
        child_exprs.clear();
        expr = unique_ptr<Expression>(new ValueExpr(Value(false)));
        change_made = true;
        return rc;
      }
    } else {
      // conjunction_type == OR
      if (constant_value == true) {
        // OR with constant true: whole expression is true
        child_exprs.clear();
        expr = unique_ptr<Expression>(new ValueExpr(Value(true)));
        change_made = true;
        return rc;
      } else {
        child_exprs.erase(iter);
      }
    }
  }
  if (child_exprs.size() == 1) {
    LOG_TRACE("conjunction expression has only 1 child");
    unique_ptr<Expression> child_expr = std::move(child_exprs.front());
    child_exprs.clear();
    expr = std::move(child_expr);

    change_made = true;
  }

  return rc;
}
