package cn.regionfs.jraft.snapshot

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils

class LogIndexFile(path: String) {
  private val defaultValue: Long = -1L
  private val encoding = "UTF-8"

  def getFile: File = {
    val fileDir = new File(path)
    if (!fileDir.exists()) {
      fileDir.mkdirs()
    }
    val file = new File(getPath + File.separator + "index")
    if (!file.exists()) {
      file.createNewFile()
      FileUtils.writeStringToFile(file, defaultValue.toString, encoding)
    }
    file
  }

  def getPath: String = this.path
  def save(index: Long): Boolean = {
    try {
      FileUtils.writeStringToFile(getFile, index.toString, encoding)
      true
    }
    catch {
      case _: Exception => false
    }
  }

  def load(): Long = {
    val s = FileUtils.readFileToString(getFile, encoding)
    if (!StringUtils.isBlank(s)) s.toLong
    else defaultValue
  }
}
