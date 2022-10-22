using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Threading;

namespace REDFS_TESTS
{
    [TestClass]
    public class TestBlockAllocator_Set3
    {
        string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
        private void InitNewTestContainer(out string containerName)
        {
            REDFS.isTestMode = true;
            ContainerObject co1 = new ContainerObject();
            int id1 = (new Random()).Next();
            co1.containerName = "FPTest_RBA3_" + id1;
            co1.containerPath = docFolder + "\\REDFS\\FPTest_RBA3_" + id1;

            REDFS.AddNewContainer(co1);

            REDFS.MountContainer(true, co1.containerName);
            Assert.IsNotNull(REDFS.redfsContainer);

            Assert.AreEqual(co1.containerName, REDFS.redfsContainer.containerName);
            Assert.AreEqual(co1.containerPath, REDFS.redfsContainer.containerFolderPath);
            containerName = co1.containerName;
        }

        private void CleanupTestContainer(string containerName)
        {
            REDFS.UnmountContainer();
            REDFS.CleanupTestContainer(containerName);
        }


        /*
         * Here we will be doing more complicated tests. Firstly by creating a container with multiple Span types.
         * Allocating as needed and free blocks as needed. Then we unmount container, remount it and check that
         * all the numbers add up correctly
         */
        [TestMethod]
        public void TestWithMultipleSpansOFAllTypes()
        {
            string containerName;
            InitNewTestContainer(out containerName);

           
            //This is what is the chunk layout.
            //a. 1GB-2GB we have default spans
            //b. 2GB-4GB we have 2 spans of mirrored segments
            //c. 4-8 GB we have a RAID5 span spread over 5 1G segments and mapped to 4GB in dbn space
            //d. We allocate random dbns from each of these types and free them. Both times we
            //   check usage metrics is as expected!

            //Two segments default 1GB and 2GB in dbn space
            RAWSegment[] dataDefault1 = new RAWSegment[1];
            dataDefault1[0] = new RAWSegment(SEGMENT_TYPES.Default, 1, 1);
            RAWSegment[] dataDefault2 = new RAWSegment[1];
            dataDefault2[0] = new RAWSegment(SEGMENT_TYPES.Default, 1, 2);

            DBNSegmentSpan dss1 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, 0, dataDefault2, null);
            DBNSegmentSpan dss2 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, OPS.NUM_DBNS_IN_1GB, dataDefault1, null);

            //Two segments mirrored at 3GB and 4GB in dbn space
            RAWSegment[] dataMirror1 = new RAWSegment[2];
            dataMirror1[0] = new RAWSegment(SEGMENT_TYPES.Mirrored, 3, 2);
            dataMirror1[1] = new RAWSegment(SEGMENT_TYPES.Mirrored, 4, 2);
            DBNSegmentSpan dss3 = new DBNSegmentSpan(SPAN_TYPE.MIRRORED, 2 * OPS.NUM_DBNS_IN_1GB, dataMirror1, null);

            RAWSegment[] dataMirror2 = new RAWSegment[2];
            dataMirror2[0] = new RAWSegment(SEGMENT_TYPES.Mirrored, 3, 3);
            dataMirror2[1] = new RAWSegment(SEGMENT_TYPES.Mirrored, 4, 3);
            DBNSegmentSpan dss4 = new DBNSegmentSpan(SPAN_TYPE.MIRRORED, 3 * OPS.NUM_DBNS_IN_1GB, dataMirror2, null);

            //Four segments mirrored at 4GB to 8GB in dbn space
            RAWSegment[] dataRaid = new RAWSegment[4];
            dataRaid[0] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 1, 3);
            dataRaid[1] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 2, 1);
            dataRaid[2] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 3, 1);
            dataRaid[3] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 4, 1);

            RAWSegment[] dataParity = new RAWSegment[1];
            dataParity[0] = new RAWSegment(SEGMENT_TYPES.RAID5Parity, 5, 1);

            DBNSegmentSpan dss5678 = new DBNSegmentSpan(SPAN_TYPE.RAID5, 4 * OPS.NUM_DBNS_IN_1GB, dataRaid, dataParity);

            DBNSegmentSpanMap spanMap = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;
            spanMap.InsertDBNSegmentSpan(dss1);
            spanMap.InsertDBNSegmentSpan(dss2);
            spanMap.InsertDBNSegmentSpan(dss3);
            spanMap.InsertDBNSegmentSpan(dss4);
            spanMap.InsertDBNSegmentSpan(dss5678);

            REDFSBlockAllocator rba = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator;
            REDFS.UnmountContainer();

            REDFS.MountContainer(true, containerName);

            //Test that we loaded correctly.
            Assert.IsTrue(spanMap.Equals(rba.dbnSpanMap));
            REDFSBlockAllocator rba1 = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator;

            Random r = new Random();
            int dbnsToAllocate = 100;
            long[] dbnsDefault = rba1.allocateDBNS(0, SPAN_TYPE.DEFAULT, r.Next(OPS.NUM_DBNS_IN_1GB, 2 * OPS.NUM_DBNS_IN_1GB - 2 * dbnsToAllocate - 1), dbnsToAllocate * 2);
            long[] dbnsMirror = rba1.allocateDBNS(0, SPAN_TYPE.MIRRORED, r.Next(2 * OPS.NUM_DBNS_IN_1GB, 4 * OPS.NUM_DBNS_IN_1GB - 2 * dbnsToAllocate - 1), dbnsToAllocate * 2);

            //align somewhere to 4 bytes
            //i.e start at 4th segment, random offset somewhere inside but - 400blks so we dont straddle the 8th segement
            int straddle = r.Next(100000) * 4;
            long start_position_raid5 = 4 * OPS.NUM_DBNS_IN_1GB + straddle - (4 * dbnsToAllocate * 2);

            long[] dbnsRaid5 = rba1.allocateDBNS(0, SPAN_TYPE.RAID5, start_position_raid5, dbnsToAllocate * 4);

            Assert.AreEqual(dbnsDefault.Length, 200);
            Assert.AreEqual(dbnsMirror.Length, 200);
            Assert.AreEqual(dbnsRaid5.Length, 400);

            Assert.IsTrue(dbnsRaid5[0] % 4 == 0);
            Assert.IsTrue(dbnsRaid5[dbnsRaid5.Length - 1] % 4 == 3);

            for (int d = 0; d < dbnsDefault.Length; d++)
            {
                Assert.IsTrue(dbnsDefault[d] < 2 * OPS.NUM_DBNS_IN_1GB);
            }
            for (int m = 0; m < dbnsMirror.Length; m++)
            {
                Assert.IsTrue((dbnsMirror[m] < 4 * OPS.NUM_DBNS_IN_1GB) && (dbnsMirror[m] >= 2 * OPS.NUM_DBNS_IN_1GB));
            }
            for (int r1 = 0; r1 < dbnsRaid5.Length; r1++)
            {
                Assert.IsTrue((dbnsRaid5[r1] < 8 * OPS.NUM_DBNS_IN_1GB) && (dbnsRaid5[r1] >= 4 * OPS.NUM_DBNS_IN_1GB));
            }

            Thread.Sleep(10000);

            //1 for the fsid
            Assert.AreEqual(1 + dbnsDefault.Length + dbnsMirror.Length + dbnsRaid5.Length, rba1.allocBitMap32TBFile.USED_BLK_COUNT);
            Assert.AreEqual(2 * OPS.NUM_DBNS_IN_1GB - 200, rba1.GetAvailableBlocksWithType(SPAN_TYPE.DEFAULT));
            Assert.AreEqual(2 * OPS.NUM_DBNS_IN_1GB - 200, rba1.GetAvailableBlocksWithType(SPAN_TYPE.MIRRORED));
            Assert.AreEqual(4 * OPS.NUM_DBNS_IN_1GB - 400, rba1.GetAvailableBlocksWithType(SPAN_TYPE.RAID5));

            //end of half test.
            Thread.Sleep(1000);

            REDFS.UnmountContainer();
            Thread.Sleep(1000);
            REDFS.MountContainer(true, containerName);

            REDFSBlockAllocator rba_t = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator;

            //1 for fsid
            Assert.AreEqual(1+ dbnsDefault.Length + dbnsMirror.Length + dbnsRaid5.Length, rba_t.allocBitMap32TBFile.USED_BLK_COUNT);
            Assert.AreEqual(2 * OPS.NUM_DBNS_IN_1GB - 200, rba_t.GetAvailableBlocksWithType(SPAN_TYPE.DEFAULT));
            Assert.AreEqual(2 * OPS.NUM_DBNS_IN_1GB - 200, rba_t.GetAvailableBlocksWithType(SPAN_TYPE.MIRRORED));
            Assert.AreEqual(4 * OPS.NUM_DBNS_IN_1GB - 400, rba_t.GetAvailableBlocksWithType(SPAN_TYPE.RAID5));

            //Lets (optionally) verify that the bits are set correctly.
            for (int d = 0; d < dbnsDefault.Length; d++) {
                Assert.IsTrue(rba_t.allocBitMap32TBFile.fsid_checkbit(dbnsDefault[d]));
                rba_t.mod_refcount(0, dbnsDefault[d], REFCNT_OP.DECREMENT_REFCOUNT_ONDEALLOC, null, false);
            }
            Thread.Sleep(10000);
            for (int m = 0; m<dbnsMirror.Length; m++)
            {
                Assert.IsTrue(rba_t.allocBitMap32TBFile.fsid_checkbit(dbnsMirror[m]));
                rba_t.mod_refcount(0, dbnsMirror[m], REFCNT_OP.DECREMENT_REFCOUNT_ONDEALLOC, null, false);
            }
            Thread.Sleep(10000);
            for (int r1 = 0; r1<dbnsRaid5.Length; r1++)
            {
                Assert.IsTrue(rba_t.allocBitMap32TBFile.fsid_checkbit(dbnsRaid5[r1]));
            }
            //to avoid stradling 4 block boundaries, only for raid 5 dbns.
            lock (GLOBALQ.m_deletelog_spanmap)
            {
                for (int r1 = 0; r1 < dbnsRaid5.Length; r1++)
                {
                    rba_t.mod_refcount(0, dbnsRaid5[r1], REFCNT_OP.DECREMENT_REFCOUNT_ONDEALLOC, null, false);
                }
            }

            Thread.Sleep(60000);
            Assert.AreEqual(2 * OPS.NUM_DBNS_IN_1GB, rba_t.GetAvailableBlocksWithType(SPAN_TYPE.DEFAULT));
            Assert.AreEqual(2 * OPS.NUM_DBNS_IN_1GB, rba_t.GetAvailableBlocksWithType(SPAN_TYPE.MIRRORED));
            Assert.AreEqual(4 * OPS.NUM_DBNS_IN_1GB, rba_t.GetAvailableBlocksWithType(SPAN_TYPE.RAID5));

            //end of test.
            Thread.Sleep(2000);
            CleanupTestContainer(containerName);
        }
    }
}
