using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Threading;

namespace REDFS_TESTS
{
    /*
     * Here we utilize both REDFSBlockAllocator and Persistant storage to write blocks and
     * retrive them after container unmount and mount
     */ 
    [TestClass]
    public class TestPersistantStorage_Set1
    {
        string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
        private void InitNewTestContainer(out string containerName)
        {
            ContainerObject co1 = new ContainerObject();
            int id1 = (new Random()).Next();
            co1.containerName = "Persistance_RBA1_" + id1;
            co1.containerPath = docFolder + "\\REDFS\\Persistance_RBA1_" + id1;

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

        private void TestThatDataIsPresentInChunkFilesFor_TestInitPersistantStorage_1(string path, byte[] buffer)
        {
            using (FileStream f = new FileStream(path, FileMode.Open))
            {
                byte[] ondisk = new byte[OPS.FS_BLOCK_SIZE];
                f.Seek(OPS.FS_BLOCK_SIZE, SeekOrigin.Begin);
                f.Read(ondisk);

                Boolean doesMatch = true;
                for (int i = 0; i < buffer.Length; i++)
                {
                    if (buffer[i] != ondisk[i]) doesMatch = false;
                }
                Assert.IsTrue(doesMatch);

                Array.Clear(ondisk, 0, ondisk.Length);

                f.Seek(OPS.FS_BLOCK_SIZE + OPS.FS_BLOCK_SIZE * OPS.NUM_DBNS_IN_1GB, SeekOrigin.Begin);
                f.Read(ondisk);

                doesMatch = true;
                for (int i = 0; i < buffer.Length; i++)
                {
                    if (buffer[i] != ondisk[i]) doesMatch = false;
                }

                Assert.IsTrue(doesMatch);
            }
        }
       
        private void CreateTwoChunksWithAlternatingSegments(string containerName, out string chunk1path, out string chunk2path)
        {
            ChunkInfo ci1 = new ChunkInfo();
            ci1.size = 2;
            ci1.freeSpace = ci1.size * 1024;
            ci1.path = docFolder + "\\REDFS\\" + containerName + "\\cifile1.dat";
            ci1.speedClass = "default";
            ci1.allowedSegmentTypes = SEGMENT_TYPES.Default;

            ChunkInfo ci2 = new ChunkInfo();
            ci2.size = 2;
            ci2.freeSpace = ci2.size * 1024;
            ci2.path = docFolder + "\\REDFS\\" + containerName + "\\cifile2.dat";
            ci2.speedClass = "default";
            ci2.allowedSegmentTypes = SEGMENT_TYPES.Default | SEGMENT_TYPES.Mirrored | SEGMENT_TYPES.RAID5Data;

            REDFS.redfsContainer.AddNewChunkToContainer(false, ci1);
            REDFS.redfsContainer.AddNewChunkToContainer(false, ci2);

            REDFS.redfsContainer.containerOperations.CreateNewOperation(ALLOWED_OPERATIONS.CHUNK_CREATE, "Create Chunking", "Writing", "1," + ci1.path + "," + ci1.size + "," + ci1.allowedSegmentTypes);
            REDFS.redfsContainer.containerOperations.CreateNewOperation(ALLOWED_OPERATIONS.CHUNK_CREATE, "Create Chunking", "Writing", "2," + ci2.path + "," + ci2.size + "," + ci2.allowedSegmentTypes);

            //Wait for actual file creation
            REDFS.redfsContainer.WaitForChunkCreationToComplete();
            REDFS.redfsContainer.containerOperations.StopAllRunningOperations();

            Assert.AreEqual(REDFS.redfsContainer.redfsChunks.Count, 3);

            //Lets create a default segment of 4 gb, with segments of chunk 1 at position 0 and 2 and segments of chunk 2 in position of 2 and 3.
            // i.e  Chunk1 1st GB => DBN (0,1GB)
            //      chunk2 1st GB => DBN (1GB, 2GB)
            //      chunk1 2nd GB => DBN (2GB, 3GB)
            //      chunk2 2nd GB => DBN (3GB, 4GB)
            RAWSegment[] dataDefault1 = new RAWSegment[1];
            dataDefault1[0] = new RAWSegment(SEGMENT_TYPES.Default, 1, 1);
            RAWSegment[] dataDefault2 = new RAWSegment[1];
            dataDefault2[0] = new RAWSegment(SEGMENT_TYPES.Default, 2, 1);
            RAWSegment[] dataDefault3 = new RAWSegment[1];
            dataDefault3[0] = new RAWSegment(SEGMENT_TYPES.Default, 1, 2);
            RAWSegment[] dataDefault4 = new RAWSegment[1];
            dataDefault4[0] = new RAWSegment(SEGMENT_TYPES.Default, 2, 2);

            //Now lets define the segment spans that are seen in dbn address apce.
            DBNSegmentSpan dss1 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, 0, dataDefault1, null);
            DBNSegmentSpan dss2 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, OPS.NUM_DBNS_IN_1GB, dataDefault2, null);
            DBNSegmentSpan dss3 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, 2 * OPS.NUM_DBNS_IN_1GB, dataDefault3, null);
            DBNSegmentSpan dss4 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, 3 * OPS.NUM_DBNS_IN_1GB, dataDefault4, null);

            //Lets create a spanmap and insert them
            DBNSegmentSpanMap spanMap = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;
            spanMap.InsertDBNSegmentSpan(dss1);
            spanMap.InsertDBNSegmentSpan(dss2);
            spanMap.InsertDBNSegmentSpan(dss3);
            spanMap.InsertDBNSegmentSpan(dss4);

            chunk1path = ci1.path;
            chunk2path = ci2.path;
        }

        [TestMethod]
        public void TestInitPersistantStorage_1()
        {
            REDFS.isTestMode = true;
            string containerName;
            InitNewTestContainer(out containerName);
            string chunk1path;
            string chunk2path;

            CreateTwoChunksWithAlternatingSegments(containerName, out chunk1path, out chunk2path);

            //With this information loaded, we can simply write to a particular dbn, For this test lets write the
            //2nd block of each of the 4 1Gig defaults spans we have.
            Random r = new Random();
            byte[] buffer = new byte[OPS.FS_BLOCK_SIZE];
            r.NextBytes(buffer);
           
            RedFSPersistantStorage rps = REDFS.redfsContainer.ifsd_mux.redfsCore.redFSPersistantStorage;

            Red_Buffer rb1 = new RedBufL0(1);
            Red_Buffer rb2 = new RedBufL0(1 + OPS.NUM_DBNS_IN_1GB);
            Red_Buffer rb3 = new RedBufL0(1 + 2 * OPS.NUM_DBNS_IN_1GB);
            Red_Buffer rb4 = new RedBufL0(1 + 3 * OPS.NUM_DBNS_IN_1GB);

            rb1.data_to_buf(buffer);
            rb2.data_to_buf(buffer);
            rb3.data_to_buf(buffer);
            rb4.data_to_buf(buffer);

            WritePlanElement wpe = new WritePlanElement();
            List<Red_Buffer> rblist = new List<Red_Buffer>();

            //First block.
            wpe.dataChunkIds = new int[1];
            wpe.dataChunkIds[0] = 1;
            wpe.dbns = new long[1];
            wpe.dbns[0] = rb1.get_ondisk_dbn();
            wpe.writeOffsets = new long[1];
            wpe.writeOffsets[0] = OPS.FS_BLOCK_SIZE;
            rblist.Add(rb1);
            rps.ExecuteWritePlan(wpe, null, rblist);

            //Second block
            rblist.Clear();
            wpe.dataChunkIds[0] = 2;
            wpe.dbns[0] = rb2.get_ondisk_dbn();
            wpe.writeOffsets[0] = OPS.FS_BLOCK_SIZE;
            rblist.Add(rb2);
            rps.ExecuteWritePlan(wpe, null, rblist);

            //Third block
            rblist.Clear();
            wpe.dataChunkIds[0] = 1;
            wpe.dbns[0] = rb3.get_ondisk_dbn();
            wpe.writeOffsets[0] = OPS.FS_BLOCK_SIZE + OPS.FS_BLOCK_SIZE * OPS.NUM_DBNS_IN_1GB;
            rblist.Add(rb3);
            rps.ExecuteWritePlan(wpe, null, rblist);

            //Fourth block
            rblist.Clear();
            wpe.dataChunkIds[0] = 2;
            wpe.dbns[0] = rb4.get_ondisk_dbn();
            rblist.Add(rb4);
            wpe.writeOffsets[0] = OPS.FS_BLOCK_SIZE + OPS.FS_BLOCK_SIZE * OPS.NUM_DBNS_IN_1GB;
            rps.ExecuteWritePlan(wpe, null, rblist);

            rps.shut_down();

            //Lets read the files to confirm
            TestThatDataIsPresentInChunkFilesFor_TestInitPersistantStorage_1(chunk1path, buffer);
            TestThatDataIsPresentInChunkFilesFor_TestInitPersistantStorage_1(chunk2path, buffer);
            CleanupTestContainer(containerName);
        }

        [TestMethod]
        public void TestInitPersistantStorage_2()
        {
            REDFS.isTestMode = true;
            string containerName;
            InitNewTestContainer(out containerName);
            string chunk1path;
            string chunk2path;

            CreateTwoChunksWithAlternatingSegments(containerName, out chunk1path, out chunk2path);

            RedFSPersistantStorage rps = REDFS.redfsContainer.ifsd_mux.redfsCore.redFSPersistantStorage;
            REDFSBlockAllocator rba = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator;

            long[] skipthese = rba.allocateDBNS(0, SPAN_TYPE.DEFAULT, 0, 10000); //Should give from start of usable dbns
            long[] usethese = rba.allocateDBNS(0, SPAN_TYPE.DEFAULT, 0, 100); //Should give from 10000 + reserved dbns

            Assert.IsTrue(skipthese[0] == OPS.MIN_ALLOCATABLE_DBN);
            Assert.IsTrue(usethese[0] == OPS.MIN_ALLOCATABLE_DBN + skipthese.Length);
            Assert.IsTrue(skipthese.Length == 10000);
            Assert.IsTrue(usethese.Length == 100);
            Console.WriteLine(skipthese.Length + " and " + usethese.Length);
            Console.WriteLine(skipthese[0] + " and " + usethese[0]);

            //Now lets punch some holes in the dbns of skipthese[], basically we are freeing them
            for (int i=0;i<100;i++)
            {
                rba.mod_refcount(0, skipthese[i + 900], REFCNT_OP.DECREMENT_REFCOUNT_ONDEALLOC, null, false);
            }
            Thread.Sleep(5000);//wait for bitmaps to be updated

            long[] prevFreedDbns = rba.allocateDBNS(0, SPAN_TYPE.DEFAULT, 0, 100);
            //Now since are have freed skipthese[900] whichs is basically OPS.MIN_ALLOCATABLE_DBN + 900th dbn.

            Assert.IsTrue(prevFreedDbns.Length == 100);
            Assert.IsTrue(prevFreedDbns[0] == 900 + OPS.MIN_ALLOCATABLE_DBN);
            Assert.IsTrue(prevFreedDbns[99] == 999 + OPS.MIN_ALLOCATABLE_DBN);

            //Lets read the files to confirm
            CleanupTestContainer(containerName);
        }
    }
}
