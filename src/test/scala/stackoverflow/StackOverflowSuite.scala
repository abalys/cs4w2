package stackoverflow

import StackOverflow._
import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  val lines: RDD[String] = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
  val raw: RDD[Question] = rawPostings(lines)
  val grouped: RDD[(QID, Iterable[(Question, Answer)])] = groupedPostings(raw)
  val scored2: RDD[(Question, HighScore)] = scoredPostings(grouped).cache()
  val scored = scored2.filter(_._1.id < 175).collect().toSet

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("scoredPostings, check if scored contains given tuples") {
    val css = (Posting(1, 6,   None, None, 140, Some("CSS")),  67)
    val php = (Posting(1, 42,  None, None, 155, Some("PHP")),  89)
    val ruby = (Posting(1, 72,  None, None, 16,  Some("Ruby")), 3)
    val java = (Posting(1, 126, None, None, 33,  Some("Java")), 30)
    val csharp = (Posting(1, 174, None, None, 38,  Some("C#")),   20)

    assert(List(css, php, ruby, java, csharp).forall(scored.contains))

  }

  test("vector postings") {
    val retrievedVectors: RDD[(LangIndex, HighScore)] = vectorPostings(scored2)
    val collected: List[(Int, Int)] = retrievedVectors.collect().toList

    val shouldContain: List[(Int, Int)] = List(
      (350000, 67),
      (100000, 89),
      (300000, 3),
      (50000,  30),
      (200000, 20)
    )
    assert(retrievedVectors.count === 2121822, "Incorrect number of vectors: " + retrievedVectors.count())
    assert(shouldContain.forall(collected.contains))
  }




}
