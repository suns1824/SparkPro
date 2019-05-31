package com.happy.spark.raysurf.entalyst.aggregatefunction

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{SpecifiedWindowFrame, UnspecifiedFrame, WindowExpression, WindowFunction, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

class ResolveWindowFrame extends Rule[LogicalPlan]{
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case logical: LogicalPlan => logical transformExpressions {
      case WindowExpression(wf: WindowFunction, WindowSpecDefinition(_, _, f: SpecifiedWindowFrame)) if wf.frame != UnspecifiedFrame && wf.frame != f =>
        failAnalysis(s"Window Frame $f must match the required frame ${wf.frame}")
      case WindowExpression(wf: WindowFunction, s @ WindowSpecDefinition(_, o, UnspecifiedFrame)) if wf.frame != UnspecifiedFrame =>
        WindowExpression(wf, s.copy(frameSpecification = wf.frame))
      case we @ WindowExpression(e, s @ WindowSpecDefinition(_, o, UnspecifiedFrame)) if e.resolved =>
        val frame = SpecifiedWindowFrame.defaultWindowFrame(o.nonEmpty, acceptWindowFrame = true)
        we.copy(windowSpec = s.copy(frameSpecification = frame))
    }
  }

  protected def failAnalysis(msg: String): Nothing = {
    //throw new AnalysisException(msg)
    throw new Exception(msg)
  }
}





































