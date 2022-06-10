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
    public class TestWRLoader_Set1
    {
        string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

        [TestMethod]
        public void TestWRBufBasic()
        {
            Assert.IsTrue(OPS.FS_BLOCK_SIZE == 8192);
            Assert.IsTrue(OPS.FS_SPAN_OUT == 1024);

            //Test all flags settting, comparator, reading and writing dbn refcounts and child_refcounts
            int assume_rbn = 1000;
            long start_dbn = assume_rbn * OPS.REF_INDEXES_IN_BLOCK;
            WRBuf wrbuf1 = new WRBuf(assume_rbn);

            Assert.AreEqual(start_dbn, wrbuf1.start_dbn);

            long[] refs = new long[OPS.REF_INDEXES_IN_BLOCK];
            long[] childrefs = new long[OPS.REF_INDEXES_IN_BLOCK];

            int i = 99, j = 88, idx = 0;
            for (long d = start_dbn; d < (start_dbn + OPS.REF_INDEXES_IN_BLOCK); d++)
            {
                wrbuf1.set_refcount(d, i++);
                wrbuf1.set_childcount(d, j++);
                refs[idx] = i - 1;
                childrefs[idx++] = j - 1;
            }

            idx = 0;
            for (long d = start_dbn; d < (start_dbn + OPS.REF_INDEXES_IN_BLOCK); d++)
            {
                Assert.AreEqual(refs[idx], wrbuf1.get_refcount(d));
                Assert.AreEqual(childrefs[idx++], wrbuf1.get_childcount(d));
            }
        }

        [TestMethod]

        public void TestWRLoaderBasic()
        {
            ContainerObject co1 = new ContainerObject();
            int id1 = (new Random()).Next();
            co1.containerName = "WRset1_1_Test" + id1;
            co1.containerPath = docFolder + "\\REDFS\\WRset1_1_Test" + id1;

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

            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.refCountMap.m_initialized);

            REDFS.UnmountContainer();
            REDFS.CleanupTestContainer(co1.containerName);
        }

        [TestMethod]
        public void TestWRLoaderQueueItem()
        {
            ContainerObject co1 = new ContainerObject();
            int id1 = (new Random()).Next();
            co1.containerName = "WRset1_2_Test" + id1;
            co1.containerPath = docFolder + "\\REDFS\\WRset1_2_Test" + id1;

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

            WRLoader wrloader = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.refCountMap;
            Thread.Sleep(1000);

            wrloader.mod_refcount(0, -1, 100, REFCNT_OP.INCREMENT_REFCOUNT_ALLOC, null, false);
            
            int refcnt=0, childcnt=0;
            wrloader.get_refcount(100, ref refcnt, ref childcnt);
            
            Assert.AreEqual(1, refcnt);
            Assert.AreEqual(0, childcnt);
            
            wrloader.mod_refcount(0, -1, 100, REFCNT_OP.INCREMENT_REFCOUNT_ALLOC, null, false);
            wrloader.get_refcount(100, ref refcnt, ref childcnt);

            Console.WriteLine("(refcount, childcnt)" + refcnt + "," + childcnt);

            Assert.AreEqual(2, refcnt);
            Assert.AreEqual(0, childcnt);
           
            REDFS.UnmountContainer();
            REDFS.CleanupTestContainer(co1.containerName);
        }


    }
}
