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
    public class TestBlockAllocator_Set2
    {
        string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

        [TestMethod]
        public void TestAllocateDBNsInDefaultSpan()
        {
            REDFS.isTestMode = true;
            ContainerObject co1 = new ContainerObject();
            int id1 = (new Random()).Next();
            co1.containerName = "Set1_1_Test" + id1;
            co1.containerPath = docFolder + "\\REDFS\\Set1_1_Test" + id1;

            REDFS.AddNewContainer(co1);

            REDFS.MountContainer(true, co1.containerName);
            Assert.IsNotNull(REDFS.redfsContainer);

            Assert.AreEqual(co1.containerName, REDFS.redfsContainer.containerName);
            Assert.AreEqual(co1.containerPath, REDFS.redfsContainer.containerFolderPath);

            ChunkInfo ci = new ChunkInfo();
            ci.allowedSegmentTypes = SEGMENT_TYPES.Default;
            //ci.id is automatically assigned
            ci.size = 2;
            ci.freeSpace = ci.size * 1024;
            ci.path = docFolder + "\\cifile1.dat";
            ci.speedClass = "default";
            ci.chunkIsAccessible = true;

            REDFS.redfsContainer.AddNewChunkToContainer(false, ci);

            //We have the default chunk and the new one we just added
            Assert.AreEqual(2, REDFS.redfsContainer.redfsChunks.Count);

            Assert.AreEqual(OPS.NUM_SPAN_MAX_ALLOWED, 128 * 1024); //128 TB
            Assert.AreEqual(OPS.NUM_DBNS_IN_1GB, 131072); //With 8k blocks

            //Now that we have a container, lets try to create two default segments. Our new chunk should have got the id=1,
            //and with that we create two default spans with two segments.
            //As a twist, the first datasegment will be in the 2nd GB and the second datasegment will be in the first GB of the chunkfile
            RAWSegment[] dataDefault1 = new RAWSegment[1];
            dataDefault1[0] = new RAWSegment(SEGMENT_TYPES.Default, 1, 1);
            RAWSegment[] dataDefault2 = new RAWSegment[1];
            dataDefault2[0] = new RAWSegment(SEGMENT_TYPES.Default, 1, 2);

            DBNSegmentSpan dss1 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, 0, dataDefault2, null);
            DBNSegmentSpan dss2 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, OPS.NUM_DBNS_IN_1GB, dataDefault1, null);

            //Note that we are creating these outside of 'REDFSBlockAllocator' but since we have loaded REDFSContainer, these will get properly
            //written out into the container files. We can simply load the REDFSBlockAllocator later and verify ondisk infomration is valid and as expected.
            DBNSegmentSpanMap spanMap = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;
            spanMap.InsertDBNSegmentSpan(dss1);
            spanMap.InsertDBNSegmentSpan(dss2);

            //keep a copy in memory before unmounting
            REDFSBlockAllocator rba = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator;

            REDFS.UnmountContainer();

            //Re-read everyting from disk again.

            REDFS.MountContainer(true, co1.containerName);
            DBNSegmentSpanMap spanMap1 = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;
            Assert.IsTrue(spanMap1.Equals(rba.dbnSpanMap));

            REDFSBlockAllocator rba1 = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator;
            Random r = new Random();
            int dbnsToAllocate = 100;
            long[] dbns = rba1.allocateDBNS(0, SPAN_TYPE.DEFAULT, r.Next(0, OPS.NUM_DBNS_IN_1GB - dbnsToAllocate - 1), dbnsToAllocate);

            //Lets check if the refcounts are set
            for (int i=0;i<dbns.Length; i++)
            {
                int refcount = 0;
                int childrefcount = 0;

                rba1.GetRefcounts(dbns[i], ref refcount, ref childrefcount);
                Assert.IsTrue(refcount == 1 && childrefcount == 0);
            }

            Thread.Sleep(5000);
            REDFS.UnmountContainer();
            REDFS.CleanupTestContainer(co1.containerName);
        }

        [TestMethod]
        public void TestAllocateDBNsInRAID5Span()
        {
            REDFS.isTestMode = true;
            ContainerObject co1 = new ContainerObject();
            int id1 = (new Random()).Next();
            co1.containerName = "Set1_3_Test" + id1;
            co1.containerPath = docFolder + "\\REDFS\\Set1_3_Test" + id1;
            Console.WriteLine(co1.containerPath);
            REDFS.AddNewContainer(co1);

            REDFS.MountContainer(true, co1.containerName);
            Assert.IsNotNull(REDFS.redfsContainer);

            Console.WriteLine(">> mounted " + REDFS.redfsContainer.containerName);
            Assert.AreEqual(co1.containerName, REDFS.redfsContainer.containerName);
            Assert.AreEqual(co1.containerPath, REDFS.redfsContainer.containerFolderPath);

            for (int i = 0; i < 4; i++)
            {
                ChunkInfo ci1 = new ChunkInfo();
                ci1.allowedSegmentTypes = SEGMENT_TYPES.RAID5Data | SEGMENT_TYPES.RAID5Parity;
                //ci1.id is automatically assigned
                ci1.size = 1;
                ci1.freeSpace = ci1.size * 1024;
                ci1.path = docFolder + "\\cifile1.dat" + i;
                ci1.speedClass = "default";
                ci1.chunkIsAccessible = true;

                REDFS.redfsContainer.AddNewChunkToContainer(false, ci1);
            }


            ChunkInfo ci2 = new ChunkInfo();
            ci2.allowedSegmentTypes = SEGMENT_TYPES.RAID5Parity | SEGMENT_TYPES.RAID5Data;
            //ci2.id is automatically assigned
            ci2.size = 2;
            ci2.freeSpace = ci2.size * 1024;
            ci2.path = docFolder + "\\cifile2.dat.parity";
            ci2.speedClass = "default";
            ci2.chunkIsAccessible = true;

            REDFS.redfsContainer.AddNewChunkToContainer(false, ci2);

            //We have the default chunk and the new one we just added
            Assert.AreEqual(6, REDFS.redfsContainer.redfsChunks.Count);

            //Lets print them out
            List<ChunkInfo> chks = REDFS.redfsContainer.getChunksInContainer();
            foreach (ChunkInfo ci in chks)
            {
                Console.WriteLine(ci.id + "," + ci.size + "," + ci.path);
            }


            //Now that we have a container, lets try to create two default segments. Our new chunk should have got the id=1,
            //and with that we create two default spans with two segments.
            //As a twist, the first datasegment will be in the 2nd GB and the second datasegment will be in the first GB of the chunkfile
            RAWSegment[] dataRaid = new RAWSegment[4];
            dataRaid[0] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 1, 1);
            dataRaid[1] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 2, 1);
            dataRaid[2] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 3, 1);
            dataRaid[3] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 4, 1);

            RAWSegment[] dataParity = new RAWSegment[1];
            dataParity[0] = new RAWSegment(SEGMENT_TYPES.RAID5Parity, 5, 1);

            //this span of 4 GB in dbn space is the first 4 GB starting at offset 0. So essentially
            //we have 4 usable 1GB blocks with one 1GB parity thrown in. Internally this is split into
            //4 spans and stored for the ease of alignment and processing
            DBNSegmentSpan dss1 = new DBNSegmentSpan(SPAN_TYPE.RAID5, 0, dataRaid, dataParity);

            Assert.IsTrue(dss1.isSegmentValid);

            //Note that we are creating these outside of 'REDFSBlockAllocator' but since we have loaded REDFSContainer, these will get properly
            //written out into the container files. We can simply load the REDFSBlockAllocator later and verify ondisk infomration is valid and as expected.
            DBNSegmentSpanMap spanMap = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;
            spanMap.InsertDBNSegmentSpan(dss1);

            REDFSBlockAllocator rba = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator;

            //Let print after reloading
            Console.WriteLine(" ----- source writtent to disk -------");
            spanMap.PrintSpanToSegmentMapping();
            Console.WriteLine("-------- from disk --------------");
            rba.dbnSpanMap.PrintSpanToSegmentMapping();
            Console.WriteLine("-------------------------------------");

            REDFS.UnmountContainer();

            REDFS.MountContainer(true, co1.containerName);
            DBNSegmentSpanMap spanMap1 = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;
            REDFSBlockAllocator rba1 = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator;

            Assert.IsTrue(spanMap.Equals(rba.dbnSpanMap));

            //Exact replica of code from the previous test case
            Random r = new Random();
            int dbnsToAllocate = 500;
            long[] dbns = rba1.allocateDBNS(0, SPAN_TYPE.DEFAULT, r.Next(0, OPS.NUM_DBNS_IN_1GB - dbnsToAllocate - 1), dbnsToAllocate);

            //We just search the wrong span
            Assert.AreEqual(dbns.Length, 0);

            //Lets try again
            dbns = rba1.allocateDBNS(0, SPAN_TYPE.RAID5, r.Next(0, OPS.NUM_DBNS_IN_1GB - dbnsToAllocate - 1), dbnsToAllocate);
            Assert.AreEqual(500, dbns.Length);

            //Lets check if the refcounts are set
            for (int i = 0; i < dbns.Length; i++)
            {
                int refcount = 0;
                int childrefcount = 0;

                rba1.GetRefcounts(dbns[i], ref refcount, ref childrefcount);
                Assert.IsTrue(refcount == 1 && childrefcount == 0);
            }
            //End of  dbns alloc test and verify

            Thread.Sleep(5000);
            REDFS.UnmountContainer();
            REDFS.CleanupTestContainer(co1.containerName);
        }

        [TestMethod]
        public void TestBatchAllocateDBNsInDefaultSpan()
        {
            REDFS.isTestMode = true;
            ContainerObject co1 = new ContainerObject();
            int id1 = (new Random()).Next();
            co1.containerName = "Set1_7_Test" + id1;
            co1.containerPath = docFolder + "\\REDFS\\Set1_7_Test" + id1;

            REDFS.AddNewContainer(co1);

            REDFS.MountContainer(true, co1.containerName);
            Assert.IsNotNull(REDFS.redfsContainer);

            Assert.AreEqual(co1.containerName, REDFS.redfsContainer.containerName);
            Assert.AreEqual(co1.containerPath, REDFS.redfsContainer.containerFolderPath);

            ChunkInfo ci = new ChunkInfo();
            ci.allowedSegmentTypes = SEGMENT_TYPES.Default;
            //ci.id is automatically assigned
            ci.size = 4;
            ci.freeSpace = ci.size * 1024;
            ci.path = docFolder + "\\cifile1.dat";
            ci.speedClass = "default";
            ci.chunkIsAccessible = true;

            REDFS.redfsContainer.AddNewChunkToContainer(false, ci);

            //We have the default chunk and the new one we just added
            Assert.AreEqual(2, REDFS.redfsContainer.redfsChunks.Count);

            Assert.AreEqual(OPS.NUM_SPAN_MAX_ALLOWED, 128 * 1024); //128 TB
            Assert.AreEqual(OPS.NUM_DBNS_IN_1GB, 131072); //With 8k blocks

            //Now that we have a container, lets try to create two default segments. Our new chunk should have got the id=1,
            //and with that we create two default spans with two segments.
            //As a twist, the first datasegment will be in the 2nd GB and the second datasegment will be in the first GB of the chunkfile
            RAWSegment[] dataDefault1 = new RAWSegment[1];
            dataDefault1[0] = new RAWSegment(SEGMENT_TYPES.Default, 1, 1);
            RAWSegment[] dataDefault2 = new RAWSegment[1];
            dataDefault2[0] = new RAWSegment(SEGMENT_TYPES.Default, 1, 2);
            RAWSegment[] dataDefault3 = new RAWSegment[1];
            dataDefault3[0] = new RAWSegment(SEGMENT_TYPES.Default, 1, 3);
            RAWSegment[] dataDefault4 = new RAWSegment[1];
            dataDefault4[0] = new RAWSegment(SEGMENT_TYPES.Default, 1, 4);

            DBNSegmentSpan dss1 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, 0, dataDefault2, null);
            DBNSegmentSpan dss2 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, OPS.NUM_DBNS_IN_1GB, dataDefault1, null);
            DBNSegmentSpan dss3 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, 2 * OPS.NUM_DBNS_IN_1GB, dataDefault1, null);
            DBNSegmentSpan dss4 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, 3 * OPS.NUM_DBNS_IN_1GB, dataDefault1, null);

            //Note that we are creating these outside of 'REDFSBlockAllocator' but since we have loaded REDFSContainer, these will get properly
            //written out into the container files. We can simply load the REDFSBlockAllocator later and verify ondisk infomration is valid and as expected.
            DBNSegmentSpanMap spanMap = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;
            spanMap.InsertDBNSegmentSpan(dss1);
            spanMap.InsertDBNSegmentSpan(dss2);
            spanMap.InsertDBNSegmentSpan(dss3);
            spanMap.InsertDBNSegmentSpan(dss4);

            //keep a copy in memory before unmounting
            REDFSBlockAllocator rba = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator;

            REDFS.UnmountContainer();

            //Re-read everyting from disk again.

            REDFS.MountContainer(true, co1.containerName);
            DBNSegmentSpanMap spanMap1 = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;
            Assert.IsTrue(spanMap1.Equals(rba.dbnSpanMap));

            REDFSBlockAllocator rba1 = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator;
            Random r = new Random();

            int dbnsToAllocate = 10000;

            //because we are allocating > 1024 dbns, which is > OPS.FS_SPAN_OUT, we should be doing batch alloc/update refcounts in wrloader
            long[] dbns = rba1.allocateDBNS(0, SPAN_TYPE.DEFAULT, r.Next(0, OPS.NUM_DBNS_IN_1GB - dbnsToAllocate - 1), dbnsToAllocate);

            //Lets check if the refcounts are set, lets check every 100th one as this is slow op
            for (int i = 0; i < dbns.Length; i+=100)
            {
                int refcount = 0;
                int childrefcount = 0;

                rba1.GetRefcounts(dbns[i], ref refcount, ref childrefcount);
                Assert.IsTrue(refcount == 1 && childrefcount == 0);
            }

            Thread.Sleep(5000);
            REDFS.UnmountContainer();
            REDFS.CleanupTestContainer(co1.containerName);
        }
    }
}
