package cn.yuge.spark.utils

import java.util.regex.Pattern

/**
  * Created by chenxiaoyu
  * Author: chenxiaoyu
  * Date: 2018/5/31
  * Desc:
  *
  */
object StringsUtil {
  /**
    *
    * @param str
    * @param rex 正则表达式
    * @return 返回匹配到的第一组结果
    */
  def getRexString(str: String, rex: String): String = {
    var result = ""
    val p = Pattern.compile(rex)
    val m = p.matcher(str)
    while ( {
      m.find
    }) result = m.group(1)
    result
  }
}
