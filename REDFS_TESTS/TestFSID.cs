using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;

namespace REDFS_TESTS
{
    [TestClass]
    public class TestFSID
    {
        [TestMethod]
        public void TestOffsetsOfFSID()
        {
            Assert.AreEqual(CFSvalueoffsets.fsid_ofs, 0);
            Assert.AreEqual(CFSvalueoffsets.fsid_bofs, 4);
            Assert.AreEqual(CFSvalueoffsets.fsid_flags, 8);

            Assert.AreEqual(CFSvalueoffsets.fsid_logical_data, 16);
            Assert.AreEqual(CFSvalueoffsets.fsid_unique_data, 24);
            Assert.AreEqual(CFSvalueoffsets.fsid_start_inodenum, 32);


            Assert.AreEqual(CFSvalueoffsets.fsid_created, 36);
            Assert.AreEqual(CFSvalueoffsets.fsid_lastrefresh, 44);

            Assert.AreEqual(CFSvalueoffsets.fsid_inofile_data, 512);
            Assert.AreEqual(CFSvalueoffsets.fsid_inomap_data, 768);

            //Finally
            Assert.AreEqual(OPS.FSID_BLOCK_SIZE, 1024);
        }

        public void TestGetSetOfFSIDAttributes()
        {
            byte[] buffer = new byte[OPS.FSID_BLOCK_SIZE];
            
            //Create volume id of 1 with backing/parent id of 0
            RedFS_FSID fsid = new RedFS_FSID(1, 0);

            //Create two inodes, public inode file and inodemap file
            RedFS_Inode inof = new RedFS_Inode(WIP_TYPE.PUBLIC_INODE_FILE, 0, 0);
            RedFS_Inode imap = new RedFS_Inode(WIP_TYPE.PUBLIC_INODE_MAP, 1, 0);

            //Lets set some dummy dbns
            inof.set_child_dbn(0, 1000);
            imap.set_child_dbn(0, 1111);
            inof.set_filesize(8192);
            imap.set_filesize(8192);

            //Lets populate the fsid.
            fsid.set_inodemap_wip(imap);
            fsid.set_inodefile_wip(inof);
            fsid.set_logical_data(100);
            fsid.set_start_inonumber(101);

            fsid.get_bytes(buffer);
            RedFS_FSID fsid_t = new RedFS_FSID(1, buffer);

            Assert.IsTrue(fsid.Equals(fsid_t));

        }
    }
}
