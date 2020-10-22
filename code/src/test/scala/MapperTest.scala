import MapReduceTasks.SortListOfAuthors.Mappers
import org.scalatest.FunSuite

class MapperTest extends FunSuite{


  test("Test whether retrieveAuthorFromXml returns proper result"){
    val input_xml = "<phdthesis mdate=\"2016-10-06\" key=\"phd/ethos/Bass92\">\n<author>Andrew P. Bass</author>\n<author>P. M. garg</author>\n<author>P. M. John</author>\n<author>M. Mavin</author>\n<title>The transformational implementation of JSD process specifications via finite automata representation.</title>\n<year>1992</year>\n<school>Aston University, Birmingham, UK</school>\n<ee>http://eprints.aston.ac.uk/10678/</ee>\n<ee>http://ethos.bl.uk/OrderDetails.do?uin=uk.bl.ethos.334713</ee>\n<note type=\"source\">British Library, EThOS</note>\n</phdthesis>"
    val dblpdtd = getClass.getClassLoader.getResource("dblp.dtd").toURI
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dblpdtd">
              <dblp>""" + input_xml + "</dblp>"

    val authors_list = new Mappers().retrieveAuthorFromXml(xmlString,"author")
    assert(authors_list.size == 4)

    val title = new Mappers().retrieveAuthorFromXml(xmlString,"title")
    assert(title.size==1)
  }
}
