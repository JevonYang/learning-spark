package com.yang.spark

import scala.util.parsing.json.JSON

class JsonUtils {

  def parseLog(json: String): Unit = {
    val log = JSON.parseFull(json)
  }

}
