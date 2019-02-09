Join将多个关系数据表按照一定的条件连接在一起。分布式环境下，因为涉及数据在不同节点间Shuffle，Join可以算是代价最昂贵的操作。
### Join查询概述
```text
select name, score from student join exam where student.id = exam.studentId
```
#### 文法定义与抽象语法树
理解该查询所生成的AST!
#### Join查询逻辑计划
与Join算子相关的部分在From子句中，逻辑计划生成过程由AstBuilder类定义的visistFromClause方法开始：
```text
override def visitFromClause(ctx: FromClauseContext): LogicalPlan = withOrigin(ctx) {
  //ctx.relation得到的是RelationContext列表，其中有一个RelationPrimaryContext(主要的数据表)和多个需要Join连接的表
  //
  val from = ctx.relation.asScala.foldLeft(null: LogicalPLan) { (left, relation) =>
    val right = plan(relation.relationPrimary)
    //将已经生成得到逻辑计划与新的RelationContext中的主要数据表结合得到Join算子
    val join = right.optionalMap(left)(Join(_, _, Inner, None))。；
    withJoinRelations(join, relation)
  }
  ctx.lateralView.asScala.foldLeft(from)(withGenerate)
}
```