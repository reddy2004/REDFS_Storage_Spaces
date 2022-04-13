using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace REDFS_ClusterMode
{
    /*
     * This class presents a flat view of the disk to the REDFS middlelayer of the file system.
     * The dbns are numbered from 0 to <n>, with each block being 8KB in size with a total address space of 128TB.
     * 
     * The diskspace or DBN space is partitioned into DBNSegments where it could be segments from 1GB to 5GB.
     * Each of this DBNSegments has its own protection level. Ex Default, 2x mirrored or 4D+1P RAID 5 parity. (its not raid 5, but for first cut of implimentaiton)
     * 
     * The allocator decides where to place the data for a given file/folder (depending on its protectioin settings),
     * allocates dbn's apprpriately and passes it to the upper layer. The filesystem then writes out the file on this dbns
     * 
     * This class also manages the refcount and allocmap of the dbns.
     * 
     * The total max address space of 128TB is spread out over multiple files which should be accessible to the host computer.
     * It is possible to specify the types of these files and the hardware that they reside on, so that the allocator can
     * optimize for speedier versus cheaper storage.
     * 
     * Also maintains a free pool information and the refcount information in a seperate file stored in the container folder.
     */ 
    public class REDFSBlockAllocator
    {
        String absoluteContainerPath;

        public IDictionary redfsChunks = new Dictionary<int, REDFSChunk>();

        public DBNSegmentSpanMap dbnSpanMap;

        public Map128TB allocBitMap32TBFile;

        public WRLoader refCountMap;

        //for each type of dbn
        private long[] quickSearchStartFbn = new long[3];

        /*
         * The block allocator maintains the list of all the chunks, segments and spans. This class is crutial to the working of redfs.
         * The following activities take place during init.
         * a. First load the chunk information from the json file and init the list of chunks. Verify all of them are present and accessible.
         * b. Load the 1 MB segmentMap file and use that data to update the chunk class objects, specifically
         *    - which segments in the chunk are used. and if they actually correspond to the segment types allowed in the chunk.
         *    - the drive letter of the chunk file. we need this so that we dont create RAID5 on chunk files on the same disk right?
         *    - The total free space available in each span is added and summarized.
         *    - The total free space in DBN space is computed and updated internally.
         * c. Load the Map128TB class which is the refcount file.
         * d. Load the AllocMap file which has a bit to indicate used/unused block in dbn space. This file is about 1GB, verify that the 
         *    free blocks are less than the amount indicated by the spanMap information.
         * e. Init might take a few seconds extra, but it worth it.
         */
        public REDFSBlockAllocator()
        {
            absoluteContainerPath = REDFS.getAbsoluteContainerPath();
            OPS.GenerateXORBuf(absoluteContainerPath);
        }

        public Boolean ComputeSegmentUsageBitMap()
        {
            //Lets find out which segments of each chunk are in use?
            foreach (REDFSChunk chunk in redfsChunks.Values)
            {
                chunk.isSegmentInUse = dbnSpanMap.getSegmentUsageBitmapForChunk(chunk);
            }
            return true;
        }

        public Boolean InitBlockAllocator()
        {
            Console.WriteLine("Entering InitBlockAllocator");
            try
            {
                string chunksFile = absoluteContainerPath + "\\chunk.files";
                Console.WriteLine("Reading chunks for container " + absoluteContainerPath + " @ location " + chunksFile);
                using (StreamReader sr = new StreamReader(chunksFile))
                {
                    string line;
                    while ((line = sr.ReadLine()) != null)
                    {
                        ChunkInfo b2 = JsonConvert.DeserializeObject<ChunkInfo>(line);
                        REDFSChunk rck = new REDFSChunk(b2.id, b2.allowedSegmentTypes, b2.path, b2.size);
                        redfsChunks.Add(b2.id, rck);
                    }
                }
                Console.WriteLine("Loaded " + redfsChunks.Count + " chunks' information from disk");
            } 
            catch (Exception e)
            {
                Console.WriteLine("Failed to read chunk information from disk");
                Console.WriteLine(e.Message);
                return false;
            }

            try
            {
                //does not throw any exception.
                dbnSpanMap = new DBNSegmentSpanMap();
                dbnSpanMap.init();
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to load & process the spanMap file for the container in " + absoluteContainerPath);
                Console.WriteLine(e.Message);
                throw new SystemException("Failed to init spanmap in redfsblockallocator!");
                return false;
            }

            try
            {
                allocBitMap32TBFile = new Map128TB("refcounts.redfs");
                allocBitMap32TBFile.init();

                Console.WriteLine("Used block count tracked by the refcount file is : " + allocBitMap32TBFile.USED_BLK_COUNT);

                //XXX todo, load the allocMap file which has bit for each dbn. This map helps us
                //allocate dbns easily. In theory, we can infer this file from refcounts.redfs, but that would be cumbersome
                //and boot up will take time.
                
                refCountMap = new WRLoader();
                refCountMap.init();

            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to load the refcount Map file. i.e map128TB class");
                Console.WriteLine(e.Message);
                return false;
            }
            Console.WriteLine("Leaving InitBlockAllocator");
            return true;
        }

        public void Sync()
        {
            dbnSpanMap.Sync();
            allocBitMap32TBFile.Sync();
            refCountMap.Sync();
        }

        public long allocateDBN(int fsid, SPAN_TYPE spanType)
        {
            DEFS.ASSERT(spanType == SPAN_TYPE.DEFAULT || spanType == SPAN_TYPE.MIRRORED, "We cannot ask for a single dbn for non defautl and non mirror type protection");
            long[] dbn = allocateDBNS(fsid, spanType, quickSearchStartFbn[(int)spanType], 1);
            return dbn[0];
        }

        public long[] allocateDBNSMultipleOfFour(int fsid, SPAN_TYPE spanType, int numdbns)
        {
            DEFS.ASSERT(numdbns % 4 == 0, "Cannot call allocateDBNSMultipleOfFour which is not a multiple of 4block stripes");
            return allocateDBNS(fsid, spanType, quickSearchStartFbn[(int)spanType], numdbns);
        }

        /*
         * First its tricky here, based on the span type, we must lookup our spanMap and try to
         * locate which span i.e (dbn_start, dbn_end) is suitable here. Then we check if the span
         * has enough free blocks to serve this request. if yes, we then go back to our allocMap
         * and look at the bitmap to find a set of free blocks to allocate. Notice that num_dbns
         * must be >=1 for default and mirror and >=4*x where x>=1 so that requested dbns are multiple
         * of 4. An used dbn is 'surrendered' back to REDFS. 
         * In case os 'surrendered' block, redfs writes a zero filled block to round off to 4 and
         * tracks such block seperately.
         * A 'surrendered' block exists only in raid span type and it is always because the file
         * did not have 4 blocks to completely utilize the 4 blk stripe that was returned for used
         * by this allocator
         * 
         * Allocation is done and these dbns can be used for any purpose. However, while free'ing these
         * dbns, or while cloning, snapshotting etc we must know what we are doing and hence we will
         * use the mod_refcount() routine for free'ing the blocks. These dbns are used by the Filesystem
         * to place blocks and then we must update our tables to indicate what type of blocks are placed here.
         * 
         * allocMap has 1 bit/dbn to indicate if that slot is being used.
         * RFI2.dat has the refcount of those blocks.
         * REDFS knows what these blocks are.
         */ 
        public long[] allocateDBNS(int fsid, SPAN_TYPE spanType, long start_dbn, int num_dbns)
        {
            if (start_dbn < OPS.MIN_ALLOCATABLE_DBN)
            {
                start_dbn = OPS.MIN_ALLOCATABLE_DBN;
            }

            if (start_dbn > (long)OPS.NUM_DBNS_IN_1GB * 1024 * 128) //128TB
            {
                throw new SystemException("Could not find free space! for " + spanType.ToString() + " num:" + num_dbns + " sdbn = " + start_dbn);
            }

            if (spanType == SPAN_TYPE.RAID5 && num_dbns % 4 != 0)
            {
                throw new SystemException("Requested dbns for RAID5 must be multiple of 4");
            }

            long start_dbn_here = dbnSpanMap.GetSpanWithSpecificTypesAndRequiredFreeBlocks(spanType, start_dbn, num_dbns);

            if (start_dbn_here == -1)
            {
                return Array.Empty<long>();
            }

            long end_dbn_here = ((spanType == SPAN_TYPE.RAID5) ?
                            (start_dbn_here + 4 * OPS.NUM_DBNS_IN_1GB) : (start_dbn_here + OPS.NUM_DBNS_IN_1GB));

            long[] dbns = allocBitMap32TBFile.allocate_Dbns(start_dbn_here, end_dbn_here , num_dbns);

            if (dbns.Length == 0)
            {
                //Lets try again, out first attempt failed
                //return allocateDBNS(fsid, spanType, end_dbn_here, num_dbns);
                throw new SystemException("Cannot allocate dbns");
            }

            int dbnsDone = 0;
            if (dbns.Length >= OPS.FS_SPAN_OUT)
            {
                int numSpans = dbns.Length / OPS.FS_SPAN_OUT;
                for (int j = 0;j < numSpans; j++)
                {
                    RedBufL1 wbL1 = (RedBufL1)new RedBufL1(0);
                    for (int k= 0;k< OPS.FS_SPAN_OUT;k++)
                    {
                        wbL1.set_child_dbn(k, dbns[j * OPS.FS_SPAN_OUT + k]);
                    }
                    batch_increment_refcount_on_alloc(fsid, wbL1);
                }
                dbnsDone += numSpans * OPS.FS_SPAN_OUT;
            }
            
            for (int i= dbnsDone; i<dbns.Length;i++)
            {
                //Now we have the list of dbns, lets set the refcount information.
                refCountMap.mod_refcount(fsid, dbns[i], REFCNT_OP.INCREMENT_REFCOUNT_ALLOC, null, false);
            }

            quickSearchStartFbn[(int)spanType] = dbns[dbns.Length - 1];
            return dbns;
        }

        /*
         * Propogate the child refs to the corresponding child dbns
         * to be fixed
         */ 
        public void touch_refcount(int fsid, Red_Buffer wb, bool isinodefilel0)
        {
            refCountMap.touch_refcount(fsid, wb, isinodefilel0);
        }

        /*
         * This is inc/dec the refcount of a block. This is always called on a block which is already used and the bit
         * in allocBitMap32TBFile is already set.
         * On decrement, refcount is decremented and if it is zero, then its free'd by queuing the delete operation
         * Increment increments the refcount but does not touch the allocation bit in allocBitMap32TBFile
         */
        public void mod_refcount(int fsid, long dbn, REFCNT_OP optype, Red_Buffer wb, bool isinodefilel0)
        {
            refCountMap.mod_refcount(fsid, dbn, optype, wb, isinodefilel0);
        }

        public void decrement_refcount_ondealloc(int fsid, long dbn)
        {
            /*
             * Must correspond to an l0 buffer, otherwise we need the indirect to propogate
             * the refcount downwards. For indirects, use the other one. L0 also can use the
             * other one - no probs.
             * We need this, we dont want to load the actual L0 wb's when deleting!!
             */
            refCountMap.mod_refcount(fsid, dbn, REFCNT_OP.DECREMENT_REFCOUNT_ONDEALLOC, null, false);
        }

        /*
         * Used for batch updates on alloc, total of 1024 dbns are alloced here.
         */ 
        public void batch_increment_refcount_on_alloc(int fsid, RedBufL1 wbL1)
        {
            refCountMap.mod_refcount(fsid, 0, REFCNT_OP.BATCH_INCREMENT_REFCOUNT_ALLOC, wbL1, false);
        }

        public void increment_refcount_onalloc(int fsid, long dbn)
        {
            refCountMap.mod_refcount(fsid, dbn, REFCNT_OP.INCREMENT_REFCOUNT_ALLOC, null, false);
        }

        /*
         * Used while duping fsid
         */ 
        public void increment_refcount(int fsid, Red_Buffer wb, bool isinodefilel0)
        {
            refCountMap.mod_refcount(fsid, wb.get_ondisk_dbn(), REFCNT_OP.INCREMENT_REFCOUNT, wb, isinodefilel0);
        }

        public void GetRefcounts(long dbn, ref int refcount, ref int childrefcount)
        {
            refCountMap.get_refcount(dbn, ref refcount, ref childrefcount);
        }

        /*
         * Given a span type, find out how many blocks are available for use
         */ 
        public long GetAvailableBlocksWithType(SPAN_TYPE type)
        {
            return dbnSpanMap.GetAvailableBlocksWithType(type);
        }

        public long GetAvailableBlockInformationFromAllocationMap()
        {
            return allocBitMap32TBFile.USED_BLK_COUNT;
        }
        public List<ReadPlanElement> PrepareReadPlan(long[] dbns)
        {
            return dbnSpanMap.PrepareReadPlan(dbns);
        }

        public ReadPlanElement PrepareReadPlanSingle(long dbn)
        {
            return dbnSpanMap.PrepareReadPlanSingle(dbn);
        }

        public WritePlanElement PrepareWritePlanSingle(long dbn)
        {
            return dbnSpanMap.PrepareWritePlanSingle(dbn);
        }

        public List<WritePlanElement> PrepareWritePlan(long[] dbns)
        {
            //We have to actually check that all dbns are part of the same type 
            //XXX todo
            SPAN_TYPE type = SPAN_TYPE.DEFAULT;
            throw new SystemException("Not yet implimented!");
            //return dbnSpanMap.PrepareWritePlan(dbns, type);
        }

        public void SyncAndTerminate()
        {
            if (dbnSpanMap == null)
            {
                throw new SystemException("spanmap is null!");
            }
            dbnSpanMap.SyncAndTerminate();
            refCountMap.shut_down();
            allocBitMap32TBFile.shut_down();
        }
    }
}
