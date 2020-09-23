package regionfs

import org.grapheco.regionfs.tool.RegionFsCmd
import org.junit.Test

/**
  * Created by bluejoe on 2020/2/8.
  */
class ShellCmdTest extends FileTestBase {
  override val con = new StrongMultiNode

  @Test
  def testStat(): Unit = {
    RegionFsCmd.main("config -zk 10.0.82.217:2182".split(" "));
    RegionFsCmd.main("stat -zk 10.0.82.217:2182".split(" "));
    RegionFsCmd.main("put -zk 10.0.82.217:2182 ./pom.xml ./README.md".split(" "));
    RegionFsCmd.main("stat -zk 10.0.82.217:2182".split(" "));
  }
}
