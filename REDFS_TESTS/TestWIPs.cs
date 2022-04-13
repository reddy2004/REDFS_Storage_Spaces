using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;

namespace REDFS_TESTS
{
    
    [TestClass]
    public class TestWIPs
    {
        string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
        [TestMethod]
        public void VerifyWIDOffsets()
        {
            //Offsets of values of wip in the 256byte wip data, Any inadvertant change should be caught by the tests
            Assert.AreEqual(WIDOffsets.wip_dbndata, 0);
            Assert.AreEqual(WIDOffsets.wip_inoloc, 128);
            Assert.AreEqual(WIDOffsets.wip_parent, 132);
            Assert.AreEqual(WIDOffsets.wip_size, 136);

            Assert.AreEqual(WIDOffsets.wip_created_from_fsid, 144);
            Assert.AreEqual(WIDOffsets.wip_modified_in_fsid, 148);
            Assert.AreEqual(WIDOffsets.wip_flags, 152);
            Assert.AreEqual(WIDOffsets.wip_cookie, 156);
            Assert.AreEqual(WIDOffsets.wip_ibflag, 160);
        }
        /*
         * A WIP is actually an acronym from ontap, i.e wafl inode pointer. I'm sort of used
         * to using the keyword wip from my netapp days to refer to a pointer of an inode struct.
         */
        [TestMethod]
        public void CheckWipCreation()
        {
            Assert.AreEqual(OPS.WIP_SIZE, 256);
            Assert.AreEqual(OPS.WIP_USED_SIZE, 160);
            Assert.AreEqual(OPS.NUM_PTRS_IN_WIP, 16);
            Assert.AreEqual(OPS.NUM_WIPS_IN_BLOCK, 32);
            RedFS_Inode wip = new RedFS_Inode(WIP_TYPE.REGULAR_FILE, 100, 99);
            wip.setfilefsid_on_dirty(1);

            Assert.IsTrue(wip.get_ino() == 100);
            Assert.IsTrue(wip.get_parent_ino() == 99);
            Assert.IsTrue(wip.get_filefsid_created() == 1);

            long[] randDbns = new long[OPS.NUM_PTRS_IN_WIP];
            Random r = new Random();
            for (int i=0;i < OPS.NUM_PTRS_IN_WIP; i++)
            {
                randDbns[i] = r.Next(1, Int32.MaxValue) + UInt32.MaxValue;
                wip.set_child_dbn(i, randDbns[i]);

            }

            for (int i = 0; i < OPS.NUM_PTRS_IN_WIP; i++)
            {
                Assert.AreEqual(wip.get_child_dbn(i), randDbns[i]);
            }

            byte[] wipRawData = new byte[OPS.WIP_SIZE];
            wip.get_bytes(wipRawData);

            RedFS_Inode wip_t = new RedFS_Inode(WIP_TYPE.REGULAR_FILE, 0, 0);
            wip_t.parse_bytes(wipRawData);

            Assert.IsTrue(wip.Equals(wip_t));
        }

        [TestMethod]
        public void TestRedFS_Inode_1()
        {
            //Create a REDFS Inode of regular type file and populate test data. Write out to disk.
            //Read from disk to verify that the formatting and byte alignments etc are fine.
            RedFS_Inode inode = new RedFS_Inode(WIP_TYPE.REGULAR_FILE, 999, 998);

            //Now lets do sanity checkking
            Assert.IsTrue(inode.verify_inode_number());
            Assert.IsTrue(inode.get_filesize() == 0);
            Assert.IsTrue(inode.get_inode_level() == 0);
            Assert.IsTrue(inode.get_parent_ino() == 998);
            Assert.IsTrue(inode.get_ino() == 999);
            Assert.IsTrue(inode.get_wiptype() == WIP_TYPE.REGULAR_FILE);
            Assert.IsTrue(inode.get_incore_cnt() == 0);
            for (int i = 0; i < OPS.NUM_PTRS_IN_WIP; i++)
            {
                Assert.IsTrue(inode.get_child_dbn(i) == -1);
            }
        }

        [TestMethod]
        public void TestRedFS_Inode_2()
        {
            //Create a REDFS Inode of regular type file and populate test data. Write out to disk.
            //Read from disk to verify that the formatting and byte alignments etc are fine.
            RedFS_Inode inode = new RedFS_Inode(WIP_TYPE.REGULAR_FILE, 999, 998);

            for (int i = 0; i < OPS.NUM_PTRS_IN_WIP; i++)
            {
                inode.set_child_dbn(i, 10 + 10 * i);
            }
            inode.set_filesize(64 * 1024 * 2);

            //Now lets do sanity checkking
            Assert.IsTrue(inode.verify_inode_number());
            Assert.IsTrue(inode.get_filesize() == (64 * 1024 * 2));
            Assert.IsTrue(inode.get_inode_level() == 0);
            Assert.IsTrue(inode.get_parent_ino() == 998);
            Assert.IsTrue(inode.get_ino() == 999);
            Assert.IsTrue(inode.get_wiptype() == WIP_TYPE.REGULAR_FILE);
            Assert.IsTrue(inode.get_incore_cnt() == 0);
            for (int i = 0; i < OPS.NUM_PTRS_IN_WIP; i++)
            {
                Assert.IsTrue(inode.get_child_dbn(i) == (i * 10 + 10));
            }

            RedFS_Inode inode_t = new RedFS_Inode(WIP_TYPE.REGULAR_FILE, 0, 0);
            byte[] data = new byte[256];
            inode.get_bytes(data);
            inode_t.parse_bytes(data);

            Assert.IsTrue(inode.Equals(inode_t));

        }
    }
}
