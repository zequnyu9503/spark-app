package pers.yzq.spark

import java.io.{File, FileInputStream}
import java.util.Properties

/**
  *
  * @Author: YZQ
  * @date 2019/5/31
  */
private [spark] object PropertiesHelper extends Serializable {
  private var property : Properties = null

  {
    val twa_path = System.getProperty("user.dir") + File.separator + "twa.properties"
    val twa_file = new File(twa_path)
    if(twa_file.exists()) {
      val stream = new FileInputStream(twa_file)
      property = new Properties()
      property.load(stream)
    } else {
      //scalastyle:off println
      System.err.println("No twa.properties exists below the path of user.dir")
      //scalastyle:on println
      System.exit(-1)
    }
  }

  def getProperty(key:String) : String = property.getProperty(key)
}
