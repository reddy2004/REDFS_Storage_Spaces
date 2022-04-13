using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;

namespace REDFS_TESTS
{
    [TestClass]
    public class TestOPSClassFunctions
    {
        [TestMethod]
        public void TestDefinitionsAreAsExpected()
        {
            //If it fails, then we need to fix both test and the code. This will catch any inadvertent changes to values.
            Assert.AreEqual(OPS.FS_BLOCK_SIZE, 8192);
            Assert.AreEqual(OPS.REF_BLOCK_SIZE, 4096);
            Assert.AreEqual(OPS.FS_BLOCK_SIZE_IN_KB, 8);
            Assert.AreEqual(OPS.FS_PTR_SIZE, 8);
            Assert.AreEqual(OPS.FS_SPAN_OUT, 1024);
            Assert.AreEqual(OPS.NUM_PTRS_IN_WIP, 16);
            Assert.AreEqual(OPS.WIP_SIZE, 256);
            Assert.AreEqual(OPS.WIP_USED_SIZE, 160);
            Assert.AreEqual(OPS.NUM_WIPS_IN_BLOCK, 32);

            Assert.AreEqual(OPS.REF_INDEX_SIZE, 8);
            Assert.AreEqual(OPS.REF_INDEXES_IN_BLOCK, 512);
            Assert.AreEqual(OPS.NUM_DBNS_IN_1GB, 131072);

            Assert.AreEqual(OPS.SPAN_ENTRY_SIZE_BYTES, 32);
            Assert.AreEqual(OPS.NUM_SPAN_MAX_ALLOWED, 128 * 1024);

            Assert.AreEqual(OPS.SIZE_OF_MAPBUFS, 256 * 1024);
            Assert.AreEqual(OPS.NUM_MAPBUFS_FOR_CONTAINER, 8192);
            Assert.AreEqual(OPS.MAPFILE_SIZE_IN_BYTES, (long)2 * 1024 * 1024 * 1024);
        }

        [TestMethod]
        public void TestUsageOf6BytesForLong()
        {
            Random r = new Random();
            long dbn =  (long)r.Next(0, Int32.MaxValue) * r.Next(0, 1024);
            byte[] val = BitConverter.GetBytes(dbn);
            byte[] val_t = new byte[8];

            byte[] buffer = new byte[OPS.FS_BLOCK_SIZE];
            OPS.set_dbn(buffer, 0, dbn);
            Array.Copy(buffer, 0, val_t, 0, 8);

            Assert.AreEqual(dbn, OPS.get_dbn(buffer, 0));
            Assert.AreEqual(OPS.HashToString(val), OPS.HashToString(val_t));
        }

        [TestMethod]
        public void TestDBNEntryConversionsInsideIndirects()
        {
            byte[] buffer = new byte[OPS.FS_BLOCK_SIZE];

            long[] dummyDbns = new long[OPS.FS_SPAN_OUT];

            Random r = new Random();
            for (int i=0;i<dummyDbns.Length;i++)
            {
                dummyDbns[i] = (long)r.Next(0, Int32.MaxValue) * r.Next(0, 1024);
                OPS.set_dbn(buffer, i, dummyDbns[i]);
            }

            for (int i = 0; i < dummyDbns.Length; i++)
            {
                Assert.AreEqual(dummyDbns[i], OPS.get_dbn(buffer, i));
            }
            
        }

        [TestMethod]
        public void TestFileSizeToLevelTranslation()
        {
            //L0 capacity = 8KB
            //L1 capacity = 8 * 1024 = 8MB
            //L2 capacity = 1024 L1s = 8GB
            //Wip capacity = 16 L2s = 128GB
            Assert.AreEqual(OPS.FSIZETOILEVEL(0), 0);      //0 L0s
            Assert.AreEqual(OPS.FSIZETOILEVEL(1024 * 1024), 1); //1MB 1 L1
            Assert.AreEqual(OPS.FSIZETOILEVEL((long)1024 * 1024 * 1024), 2); //1GB 1 L2
            Assert.AreEqual(OPS.FSIZETOILEVEL((long)1024 * 1024 * 1024 * 120), 2); //121 GB  8 L2s 
        }

        [TestMethod]
        public void TestIndexComputation()
        {
            Assert.AreEqual(100, OPS.PIDXToStartFBN(0, 100));
            Assert.AreEqual(20 * 1024, OPS.PIDXToStartFBN(1, 20));
            Assert.AreEqual((long)4 * 1024 * 1024, OPS.PIDXToStartFBN(2, 4));

            Assert.AreEqual(1356, OPS.OffsetToFBN(11111111));
            Assert.AreEqual(2712, OPS.OffsetToFBN(22222222));

            Assert.AreEqual(1357, OPS.NUML0(11111111));
            Assert.AreEqual(2, OPS.NUML1(11111111));
            Assert.AreEqual(0, OPS.NUML2(11111111));

            Assert.AreEqual(8192, OPS.NEXT8KBOUNDARY(4096, 12121));
            Assert.AreEqual(16384, OPS.NEXT8KBOUNDARY(8193, 8194));

            Assert.AreEqual(19, OPS.myidx_in_myparent(0, 19));
            Assert.AreEqual(0, OPS.myidx_in_myparent(1, 19));
        }
    }
}
