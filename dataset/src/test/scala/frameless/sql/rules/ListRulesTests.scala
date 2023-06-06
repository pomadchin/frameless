package frameless.sql.rules

import frameless._
import frameless.sql.FramelessOptimizations
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{currentTimestamp, microsToInstant}
import org.apache.spark.sql.sources.{Filter, GreaterThanOrEqual}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

trait ListRulesTests extends AnyFunSuite with PushDownTests {
  test("sql.Timestamp push-down") {
    val now = currentTimestamp()
    val sqlts = java.sql.Timestamp.from(microsToInstant(now))
    val ts = SQLTimestamp(now)
    val expected = sqlts

    GreaterThanOrEqualTest(ts, expected)
  }

  test("Instant push-down") {
    val payload = Instant.now()
    val expected = java.sql.Timestamp.from(payload)

    GreaterThanOrEqualTest(payload, expected)
  }
}

class ExtraOptimizationsRulesLitTests extends TypedDatasetSuite with ListRulesTests {
  def withoutOptimization[A](thunk: => A): A = thunk

  def withOptimization[A](thunk: => A): A = {
    val orig = session.sqlContext.experimental.extraOptimizations
    try {
      session.sqlContext.experimental.extraOptimizations ++= Seq(LiteralRule)
      thunk
    } finally {
      session.sqlContext.experimental.extraOptimizations = orig
    }
  }

}

class FramelessOptimizationsInjectorLitTests extends TypedDatasetSuite with ListRulesTests {

  var s: SparkSession = null

  override implicit def session: SparkSession = s

  def withoutOptimization[A](thunk: => A): A =
    try {
      s = SparkSession.builder().config(conf).getOrCreate()
      thunk
    } finally stopSpark()

  def withOptimization[A](thunk: => A): A =
    try {
      s = SparkSession.builder().config(conf.clone().set("spark.sql.extensions", classOf[FramelessOptimizations].getName)).getOrCreate()
      thunk
    } finally stopSpark()

  def stopSpark(): Unit =
    if (s != null) {
      s.stop()
      s = null
    }

  override def beforeAll(): Unit =
    stopSpark()

  override def afterAll(): Unit =
    stopSpark()

}

trait PushDownTests extends Matchers { self =>
  import Job.framelessSparkDelayForJob
  
  private val output = s"./target/${self.getClass.getName}"

  implicit def session: SparkSession

  def withoutOptimization[A](thunk: => A): A
  def withOptimization[A](thunk: => A): A

  def GreaterThanOrEqualTest[A: TypedEncoder: CatalystOrdered](payload: A, expected: Any): Assertion = {
    withoutOptimization {
      TypedDataset.create(Seq(X1(payload))).write.mode("overwrite").parquet(output)
      val dataset = TypedDataset.createUnsafe[X1[A]](session.read.parquet(output))

      val pushDowns = getPushDowns(dataset.filter(dataset('a) >= payload))

      pushDowns should not contain GreaterThanOrEqual("a", expected)
    }

    withOptimization {
      TypedDataset.create(Seq(X1(payload))).write.mode("overwrite").parquet(output)
      val dataset = TypedDataset.createUnsafe[X1[A]](session.read.parquet(output))

      val ds = dataset.filter(dataset('a) >= payload)
      // ds.explain(true)
      val pushDowns = getPushDowns(ds)
      // prove the push-down worked
      pushDowns should contain(GreaterThanOrEqual("a", expected))

      val collected = ds.collect().run().toVector.head
      // prove the serde isn't affected
      collected should be(X1(payload))
    }
  }

  def getPushDowns(dataset: TypedDataset[_]): Seq[Filter] = {
    val sparkPlan = dataset.queryExecution.executedPlan

    val initialPlan =
      if (sparkPlan.children.isEmpty) // assume it's AQE
        sparkPlan match {
          case aq: AdaptiveSparkPlanExec => aq.initialPlan
          case _ => sparkPlan
        }
      else
        sparkPlan

    initialPlan.collect {
      case fs: FileSourceScanExec =>
        import scala.reflect.runtime.{universe => ru}

        val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
        val instanceMirror = runtimeMirror.reflect(fs)
        val getter = ru.typeOf[FileSourceScanExec].member(ru.TermName("pushedDownFilters")).asTerm.getter
        val m = instanceMirror.reflectMethod(getter.asMethod)
        
        m.apply(fs).asInstanceOf[Seq[Filter]]
    }.flatten
  }
}