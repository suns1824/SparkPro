package com.happy.spark.raysurf.entalyst.aggregatefunction

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, If, IsNull, Literal, Or}
import org.apache.spark.sql.types.{DataType, LongType}

/*
DeclarativeAggregate函数是一类直接由Catalyst中的表达式(Expressions)构建的聚合函数，主要逻辑由四个表达式完成
 */

case class Count(children: Seq[Expression]) extends DeclarativeAggregate{
  override def nullable: Boolean = false
  override def dataType: DataType = LongType
  private lazy val count = AttributeReference("count", LongType, nullable = false)()

  // 聚合缓冲区初始化表达式
  override lazy val initialValues: Seq[Expression] = Seq(Literal(0L))
  // 聚合缓冲区更新表达式
  override lazy val updateExpressions: Seq[Expression] = {
    val nullableChildren = children.filter(_.nullable)
    if (nullableChildren.isEmpty) {
      Seq(
        count + 1
      )
    } else {
      Seq(
        If(nullableChildren.map(IsNull).reduce(Or), count, count + 1L)
      )
    }
  }
  //聚合缓冲区合并表达式
  override lazy val mergeExpressions: Seq[Expression] = Seq(count.left + count.right)
  //最终结果生成表达式
  override lazy val evaluateExpression: Expression = count

  override def defaultResult: Option[Literal] = Option(Literal(0L))

  override def aggBufferAttributes: Seq[AttributeReference] = count :: Nil
}

object Count {
  def apply(child: Expression): Count = Count(child :: Nil)
}
