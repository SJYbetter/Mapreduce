import java.util

import org.junit.Test

import scala.collection.mutable

class TestMap {

  @Test
  def test_aaa(): Unit = {
    val s0 = mutable.HashSet.empty[Int]
    val s1 = mutable.HashSet.empty[Int]
    s0.add(10)
    s0.add(20)
    s1.add(10)
    s1.add(30)


    val s = new util.ArrayList[Int]()

    //s.add(10)

    s0.foreach(y => {
      if (s1.contains(y)) {
        //println(y)
        s.add(y)
      }
    })

    assert(s.get(0) == 10)
  }
}
