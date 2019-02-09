package com.happy.spark.raysurf.entalyst.join

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.ParseTree
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedGenerator}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserUtils, SqlBaseBaseVisitor, SqlBaseParser}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{ExpressionContext, FromClauseContext, LateralViewContext, QualifiedNameContext}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Generate, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin

import scala.collection.JavaConverters._

class AstUnresolvedLogic extends SqlBaseBaseVisitor[AnyRef] with Logging{
  import ParserUtils._
  override def visitFromClause(ctx: FromClauseContext): LogicalPlan = withOrigin(ctx) {
    //ctx.relation得到的是RelationContext列表，其中有一个RelationPrimaryContext(主要的数据表)和多个需要Join连接的表
    //
    val from = ctx.relation.asScala.foldLeft(null: LogicalPlan) { (left, relation) =>
      val right = plan(relation.relationPrimary)
      //将已经生成得到逻辑计划与新的RelationContext中的主要数据表结合得到Join算子
      val join = right.optionalMap(left)(Join(_, _, Inner, None))
      withJoinRelations(join, relation)
    }
     ctx.lateralView.asScala.foldLeft(from)(withGenerate)
  }

  def withOrigin[T](ctx: ParserRuleContext)(f: => T): T = {
    val current = CurrentOrigin.get
    CurrentOrigin.set(position(ctx.getStart))
    try {
      f
    } finally {
      CurrentOrigin.set(current)
    }
  }

  protected def plan(tree: ParserRuleContext): LogicalPlan = typedVisit(tree)

  protected def expression(ctx: ParserRuleContext): Expression = typedVisit(ctx)

  protected def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  private def withJoinRelations(base: LogicalPlan, ctx: SqlBaseParser.RelationContext): LogicalPlan = {
    ctx.joinRelation.asScala.foldLeft(base) { (left, join) =>
      withOrigin(join) {
        val baseJoinType = join.joinType() match {
          case null => Inner
          case jt if jt.CROSS != null => Cross
          case jt if jt.FULL != null => FullOuter
          case jt if jt.SEMI != null => LeftSemi
          case jt if jt.ANTI != null => LeftAnti
          case jt if jt.LEFT != null => LeftOuter
          case jt if jt.RIGHT != null => RightOuter
          case _ => Inner
        }
        //JoinCriteriaContext节点对应Join条件的细节
        val (joinType, condition) = Option(join.joinCriteria()) match {
          case Some(c) if c.USING != null =>
            (UsingJoin(baseJoinType, c.identifier.asScala.map(_.getText)), None)
          case Some(c) if c.booleanExpression() != null =>
            (baseJoinType, Option(expression(c.booleanExpression())))
          case None if join.NATURAL != null =>
            if (baseJoinType == Cross) {
              throw new ParseException("NATURAL CROSS JOIN is not supported", ctx)
            }
            (NaturalJoin(baseJoinType), None)
          case None =>
            (baseJoinType, None)
        }
        Join(left, plan(join.right), joinType, condition)
      }

    }
  }

  /**
    * Add a [[Generate]] (Lateral View) to a logical plan.
    */
  private def withGenerate(
                            query: LogicalPlan,
                            ctx: LateralViewContext): LogicalPlan = withOrigin(ctx) {
    val expressions = expressionList(ctx.expression)
    Generate(
      UnresolvedGenerator(visitFunctionName(ctx.qualifiedName), expressions),
      unrequiredChildIndex = Nil,
      outer = ctx.OUTER != null,
      Some(ctx.tblName.getText.toLowerCase),
      ctx.colName.asScala.map(_.getText).map(UnresolvedAttribute.apply),
      query)
  }

  /**
    * Create sequence of expressions from the given sequence of contexts.
    */
  private def expressionList(trees: java.util.List[ExpressionContext]): Seq[Expression] = {
    trees.asScala.map(expression)
  }

  /**
    * Create a function database (optional) and name pair.
    */
  protected def visitFunctionName(ctx: QualifiedNameContext): FunctionIdentifier = {
    ctx.identifier().asScala.map(_.getText) match {
      case Seq(db, fn) => FunctionIdentifier(fn, Option(db))
      case Seq(fn) => FunctionIdentifier(fn, None)
      case other => throw new ParseException(s"Unsupported function name '${ctx.getText}'", ctx)
    }
  }

}
