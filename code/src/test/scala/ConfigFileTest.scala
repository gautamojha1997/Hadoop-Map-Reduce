import org.scalatest.FunSuite
import com.typesafe.config.ConfigFactory

class ConfigFileTest extends FunSuite{

  val conf = ConfigFactory.load("Config")
  test("Check whether the config file is loaded correctly"){
    assert(conf!=null)
  }

  test("Whether CoAuthorCountOutputPath value is accessed"){
    val p = conf.getString("CoAuthorCountOutputPath")
    assert(p == "/co-authorcounts")
  }

  test("Whether SortedMostCoAuthorPath value is accessed"){
    val p = conf.getString("SortedMostCoAuthorPath")
    assert(p == "/sortedMostCoAuthor")
  }

  test("Whether SortedWithoutCoAuthorPath value is accessed"){
    val p = conf.getString("SortedWithoutCoAuthorPath")
    assert(p == "/sortedWithoutCoAuthor")
  }

  test("Whether PublicationCountPath value is accessed"){
    val p = conf.getString("PublicationCountPath")
    assert(p == "/publication-count-of-authors")
  }

}
