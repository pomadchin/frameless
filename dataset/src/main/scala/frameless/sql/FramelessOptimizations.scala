package frameless.sql

import frameless.sql.rules.LiteralRule
import org.apache.spark.sql.SparkSessionExtensions

class FramelessOptimizations extends (SparkSessionExtensions => Unit) {
  def apply(extensions: SparkSessionExtensions): Unit =
    extensions.injectOptimizerRule( _ => LiteralRule)
}
