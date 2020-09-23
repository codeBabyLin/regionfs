package cn.regionfs.jraft.snapshot

import java.io.{BufferedInputStream, File, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipInputStream, ZipOutputStream}

import com.alipay.sofa.jraft.entity.PeerId

class PandaGraphSnapshotFile {
  val comUtil = new CompressDbFileUtil
  //val pandaConfig: PandaConfig = PandaRuntimeContext.contextGet[PandaConfig]()
  val per = new PeerId()
 // per.parse(pandaConfig.bolt)
  def save(dataPath: String, snapshotPath: String): Unit = {
    comUtil.compressToZip(Map("dataPath" -> dataPath), snapshotPath, "backup" + ".zip")
  }
  def load(fileNamewithPath: String, desPath: String): Unit = {
    comUtil.decompress(fileNamewithPath, desPath)
  }
}

class CompressDbFileUtil {
  def compressToZip(sourceFilePath: Map[String, String], zipFilePath: String, zipFilename: String): Unit = {
    val zipPath = new File(zipFilePath)
    if (!zipPath.exists()) {
      zipPath.mkdirs()
    }
    val zipFile = new File(zipPath + File.separator + zipFilename)
    val fos = new FileOutputStream(zipFile)
    val zos = new ZipOutputStream(fos)
    sourceFilePath.foreach(m => {
      val sourceFile = new File(m._2)
      writeZip(sourceFile, zos)
    })
    zos.close()
    fos.close()
  }

  def decompress(zipFilePath: String, toLocalPath: String): Unit = {
    val fis = new FileInputStream(new File(zipFilePath))
    val zis = new ZipInputStream(fis)

    Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach {
      file =>
        val dir = new File(toLocalPath + File.separator + file.getName)
        if (!dir.exists()) {
          new File(dir.getParent).mkdirs()
        }
        val fos = new FileOutputStream(toLocalPath + File.separator + file.getName)
        val buffer = new Array[Byte](1024)
        Stream.continually(zis.read(buffer)).takeWhile(_ != -1).foreach(fos.write(buffer, 0, _))
        fos.close()
    }
    zis.close()
    fis.close()
  }

  def writeZip(file: File, zos: ZipOutputStream, currentPath: String = ""): Unit = {
    if (file.isDirectory) {
      val parentPath = currentPath + file.getName + File.separator
      val files = file.listFiles()
      files.foreach(f => writeZip(f, zos, parentPath))
    }
    else {
      val bis = new BufferedInputStream(new FileInputStream(file))
      val zipEntry = new ZipEntry(currentPath + file.getName)
      zos.putNextEntry(zipEntry)
      val buffer: Array[Byte] = new Array[Byte](1024)
      Stream.continually(bis.read(buffer)).takeWhile(_ != -1).foreach(zos.write(buffer, 0, _))
      bis.close()
    }
  }
}