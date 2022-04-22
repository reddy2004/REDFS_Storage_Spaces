﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace REDFS_ClusterMode
{
    public class WRLoader
    {
        public bool m_initialized = false;
        private long m_creation_time;
        private long counter = 0, cachehits, total_ops;

        private FileStream mfile1 = null; /* Main file which has refcounts, with each entry of 16 bytes */
        private FileStream tfile0 = null; /* temp file used to store /rather queue ops */
        private int tfilefbn = 0;

        Random ridGen = new Random();

        /*
         * Stack and cache management.
         */
        private WRBuf[] iStack = new WRBuf[1024 * 16];
        private int iStackTop = 0;
        private int cachesize = 0;
        private WRBuf[] refcache = new WRBuf[1024 * 16];

        private byte[] tmpiodatatfileR = new byte[OPS.FS_BLOCK_SIZE];
        private byte[] tmpiodatatfileW = new byte[OPS.FS_BLOCK_SIZE];
        private byte[] tmpsnapshotcache = new byte[OPS.FS_BLOCK_SIZE];

        List<string> msgs = new List<string>();

        public void init()
        {
            //DEFS.DEBUG("WRLdr", "Starting WRLoader");
            Thread tc = new Thread(new ThreadStart(tServiceThread));
            tc.Start();
            m_initialized = true;
        }

        public void sync_blocking()
        {
            UpdateReqI r = new UpdateReqI();
            r.optype = REFCNT_OP.DO_SYNC;
            GLOBALQ.m_reqi_queue.Add(r);
            while (r.processed == false) Thread.Sleep(100);
        }

        public void Sync()
        {
            UpdateReqI r = new UpdateReqI();
            r.optype = REFCNT_OP.DO_SYNC;
            GLOBALQ.m_reqi_queue.Add(r);
        }

        public void snapshot_scanner_dowork(int rbn, bool takesnap)
        {
            UpdateReqI r = new UpdateReqI();
            r.optype = (takesnap) ? REFCNT_OP.TAKE_DISK_SNAPSHOT : REFCNT_OP.UNDO_DISK_SNAPSHOT;
            r.tfbn = rbn;
            GLOBALQ.m_reqi_queue.Add(r);
        }

        public void shut_down()
        {
            UpdateReqI r = new UpdateReqI();
            r.optype = REFCNT_OP.SHUT_DOWN;
            GLOBALQ.m_reqi_queue.Add(r);
            while (r.processed == false) Thread.Sleep(100);
        }

        private void printspeed()
        {
            if (counter % 16384 == 0)
            {
                long currtime = DateTime.Now.ToUniversalTime().Ticks;
                int seconds = (int)((currtime - m_creation_time) / 10000000);

                if (seconds != 0)
                {
                    Console.WriteLine("Speed (" + ((counter) / 256) + "/" + seconds +
                                    ")= " + ((counter) / 256) / seconds +
                                    " MBps Avg cache_hits % = " + (cachehits * 100) / total_ops +
                                    ",q=" + GLOBALQ.m_reqi_queue.Count + ", csize = " + cachesize);
                }
            }
        }

        public WRLoader()
        {
            m_creation_time = DateTime.Now.ToUniversalTime().Ticks;

            for (int i = 0; i < iStack.Length; i++)
            {
                iStack[i] = new WRBuf(0);
            }
            iStackTop = iStack.Length - 1;

            //Bad idea to preallocate?
            for (int i = 0; i < GLOBALQ.WRObj.Length; i++)
            {
                GLOBALQ.WRObj[i] = new WRContainer();
            }

            Directory.CreateDirectory(REDFS.getAbsoluteContainerPath() + "\\temp\\");

            //Main refcount file which as 8 bytes for each block. 4 bytes for its refcount & 4 bytes which has a refcount
            //that needs to be propogated downwards
            mfile1 = new FileStream(REDFS.getAbsoluteContainerPath() + "\\RFI2.dat", FileMode.OpenOrCreate, FileAccess.ReadWrite);

            //A temporary file where we write out the some of the commands to disk for sake of persistant storage. These commands
            //to update and change refcounts can be replayed if the system goes down and comes up again!
            tfile0 = new FileStream(REDFS.getAbsoluteContainerPath() + "\\temp\\tfile", FileMode.OpenOrCreate, FileAccess.ReadWrite);
        }

        private WRBuf allocate(int r)
        {
            lock (iStack)
            {
                WRBuf wb = iStack[iStackTop];
                iStackTop--;
                wb.reinit(r, ridGen.Next());
                return wb;
            }
        }
        private void deallocate(WRBuf wb)
        {
            lock (iStack)
            {
                iStackTop++;
                iStack[iStackTop] = wb;
            }
        }
        private bool free_incore_buf(int rbn)
        {
            deallocate(GLOBALQ.WRObj[rbn].incoretbuf);
            GLOBALQ.WRObj[rbn].incoretbuf = null;
            return true;
        }
        private void sync_buf(int rbn)
        {
            long offset = OPS.REF_BLOCK_SIZE * (long)(rbn);

            mfile1.Seek(offset, SeekOrigin.Begin);
            mfile1.Write(GLOBALQ.WRObj[rbn].incoretbuf.data, 0, OPS.REF_BLOCK_SIZE);

            GLOBALQ.WRObj[rbn].incoretbuf.is_dirty = false;
        }

        /*
         * Called when 'memory' pressure is detected, or periodically
         * to sync the modified values the disk & free up slots.
         */
        private void internal_sync_and_flush_cache_advanced()
        {
            int curr = 0;
            bool mempressureflag = false;
            int mempressurecounter = 0;

            Array.Sort(refcache, 0, cachesize, new wrcomparator());

            if (cachesize > 8192)
            {
                mempressureflag = true;
                mempressurecounter = cachesize - 8192; // how many to remove.
            }

            for (int i = 0; i < cachesize; i++)
            {
                if (refcache[i].is_dirty)
                {
                    sync_buf(refcache[i].m_rbn);
                }

                if ((mempressureflag && (mempressurecounter-- >= 0)) ||
                        refcache[i].get_buf_age() > 10000)
                {
                    if (free_incore_buf(refcache[i].m_rbn))
                    {
                        refcache[i] = null;
                    }
                    else
                    {
                        refcache[curr++] = refcache[i];
                    }
                }
                else
                {
                    refcache[curr++] = refcache[i];
                }
            }

            DEFS.ASSERT(cachesize >= (curr), "memory leak detected");
            cachesize = curr;


            /* 
             * Flush the ref count file 
             */
            mfile1.Flush();
        }

        /*
         * For a given rbn, it will load the 4k page into memory and return.
         * Also updates the queue to indicate the same.
         */
        private void load_wrbufx(int rbn)
        {
            if (GLOBALQ.WRObj[rbn].incoretbuf != null)
            {
                cachehits++;
            }
            else
            {

                WRBuf tbuf = allocate(rbn);

                mfile1.Seek((long)rbn * OPS.REF_BLOCK_SIZE, SeekOrigin.Begin);
                mfile1.Read(tbuf.data, 0, OPS.REF_BLOCK_SIZE);

                GLOBALQ.WRObj[rbn].incoretbuf = tbuf;
                refcache[cachesize++] = tbuf;
            }

            DoSnapshotWork(rbn);

            /* After the load, see if we have to clean up */
            if (cachesize > 15 * 1024)
            {
                internal_sync_and_flush_cache_advanced();
            }
        }

        private void DoSnapshotWork(int rbn)
        {
            if (GLOBALQ.disk_snapshot_mode_enabled && GLOBALQ.disk_snapshot_map[rbn] == false)
            {
                DEFS.ASSERT(GLOBALQ.disk_snapshot_optype == REFCNT_OP.TAKE_DISK_SNAPSHOT ||
                                GLOBALQ.disk_snapshot_optype == REFCNT_OP.UNDO_DISK_SNAPSHOT,
                                    "Failure in having correct optype set");
                DEFS.ASSERT(GLOBALQ.snapshotfile != null, "Snapshot file cannot be null");

                GLOBALQ.disk_snapshot_map[rbn] = true;
                int startdbn = rbn * OPS.REF_INDEXES_IN_BLOCK;

                if (GLOBALQ.disk_snapshot_optype == REFCNT_OP.TAKE_DISK_SNAPSHOT)
                {
                    for (int idx = 0; idx < OPS.REF_INDEXES_IN_BLOCK; idx++)
                    {
                        int curr = GLOBALQ.WRObj[rbn].incoretbuf.get_refcount(startdbn + idx);
                        if (curr != 0)
                        {
                            GLOBALQ.WRObj[rbn].incoretbuf.set_refcount(startdbn + idx, curr + 1);
                            GLOBALQ.WRObj[rbn].incoretbuf.set_dedupe_overwritten_flag(startdbn + idx, false);
                            OPS.snapshot_setbit(idx, tmpsnapshotcache, true);
                        }
                        else
                        {
                            OPS.snapshot_setbit(idx, tmpsnapshotcache, false);
                        }
                    }
                    int fileoffset = rbn * 64;
                    GLOBALQ.snapshotfile.Seek(fileoffset, SeekOrigin.Begin);
                    GLOBALQ.snapshotfile.Write(tmpsnapshotcache, 0, 64);
                }
                else
                {
                    int fileoffset = rbn * 64;
                    GLOBALQ.snapshotfile.Seek(fileoffset, SeekOrigin.Begin);
                    GLOBALQ.snapshotfile.Read(tmpsnapshotcache, 0, 64);

                    for (int idx = 0; idx < OPS.REF_INDEXES_IN_BLOCK; idx++)
                    {
                        if (OPS.snapshot_getbit(idx, tmpsnapshotcache) == true)
                        {
                            int curr = GLOBALQ.WRObj[rbn].incoretbuf.get_refcount(startdbn + idx);
                            GLOBALQ.WRObj[rbn].incoretbuf.set_refcount(startdbn + idx, curr - 1);
                        }
                    }
                }
                GLOBALQ.disk_snapshot_map[rbn] = true;
                GLOBALQ.WRObj[rbn].incoretbuf.is_dirty = true;
            } //end of if-case.        
        }

        private Red_Buffer allocate_wb(BLK_TYPE type)
        {
            Red_Buffer wb = null;
            switch (type)
            {
                case BLK_TYPE.REGULAR_FILE_L1:
                    wb = new RedBufL1(0);
                    break;
                case BLK_TYPE.REGULAR_FILE_L2:
                    wb = new RedBufL2(0);
                    break;
            }
            DEFS.ASSERT(wb != null, "Wrong request for allocate_wb(), type = " + type);
            return wb;
        }


        //Update a batch of dbns that just got allocated. We dont know their use yet, just set the dbn refcount to 1
        private void apply_update_internal_batch(UpdateReqI cu)
        {
            RedBufL1 wbl1 = (RedBufL1)allocate_wb(cu.blktype);

            lock (tfile0)
            {
                tfile0.Seek((long)cu.tfbn * OPS.FS_BLOCK_SIZE, SeekOrigin.Begin);
                tfile0.Read(tmpiodatatfileR, 0, OPS.FS_BLOCK_SIZE);
                OPS.Decrypt_Read_WRBuf(tmpiodatatfileR, wbl1.data);
            }

            for (int idx = 0; idx < OPS.FS_SPAN_OUT; idx++)
            {
                long dbn = wbl1.get_child_dbn(idx);
                int rbn = REFDEF.dbn_to_rbn(dbn);
                load_wrbufx(rbn);
                GLOBALQ.WRObj[rbn].incoretbuf.set_refcount(dbn, 1);
                GLOBALQ.WRObj[rbn].incoretbuf.set_dedupe_overwritten_flag(dbn, true);
            }
        }

        /*
         * Given a dbn, load the appropriate block and apply the update.
         */
        private void apply_update_internal(long dbn, BLK_TYPE type, int value, REFCNT_OP optype, bool updatechild)
        {
            int rbn = REFDEF.dbn_to_rbn(dbn);
            load_wrbufx(rbn);
            counter++;

            int curr = GLOBALQ.WRObj[rbn].incoretbuf.get_refcount(dbn);
            GLOBALQ.WRObj[rbn].incoretbuf.set_refcount(dbn, curr + value);


            if (optype == REFCNT_OP.INCREMENT_REFCOUNT_ALLOC ||
                optype == REFCNT_OP.DECREMENT_REFCOUNT_ONDEALLOC)
            {
                //DEFS.DEBUG("DSAF", "Apply update internal, " + optype + " : " + dbn + "," + value + "," + updatechild);
                GLOBALQ.WRObj[rbn].incoretbuf.set_dedupe_overwritten_flag(dbn, true);
            }

            if (updatechild)
            {
                if (type != BLK_TYPE.REGULAR_FILE_L0 && type != BLK_TYPE.IGNORE)
                {
                    int currchd = GLOBALQ.WRObj[rbn].incoretbuf.get_childcount(dbn);
                    GLOBALQ.WRObj[rbn].incoretbuf.set_childcount(dbn, currchd + value);
                    //DEFS.DEBUGCLR("/-0-0-/", "dbn,  " + dbn + "(" + curr + "->" + (curr + value) + ") (" + currchd + "->" + (currchd + value) + ")");

                    int currchd_verify = GLOBALQ.WRObj[rbn].incoretbuf.get_childcount(dbn);
                    DEFS.ASSERT((currchd + value) == currchd_verify, "Child refcount not writting out correctly!");
                }
            }
        }

        private void do_inode_refupdate_work(UpdateReqI cu, int childcnt)
        {
            byte[] buffer = new byte[OPS.FS_BLOCK_SIZE];

            lock (tfile0)
            {
                tfile0.Seek((long)cu.tfbn * OPS.FS_BLOCK_SIZE, SeekOrigin.Begin);
                tfile0.Read(tmpiodatatfileR, 0, OPS.FS_BLOCK_SIZE);
                OPS.Decrypt_Read_WRBuf(tmpiodatatfileR, buffer);
            }

           
            //Parent of inowip is always -1.
            RedFS_Inode wip = new RedFS_Inode(WIP_TYPE.REGULAR_FILE, 0, -1);
            
            byte[] buf = new byte[128];

            //32 entries in inode block
            for (int i = 0; i < 32; i++)
            {
                for (int t = 0; t < 128; t++) buf[t] = buffer[i * 128 + t];
                wip.parse_bytes(buf);

                BLK_TYPE type = BLK_TYPE.IGNORE;
                int numidx = 0;

                int inolevel = wip.get_inode_level();
                int numl2 = wip.get_inode_level();
                long size = wip.get_filesize();
                int inon = wip.get_ino();
                String wipstr = wip.ToString();

                switch (wip.get_inode_level())
                {
                    case 0:
                        type = BLK_TYPE.REGULAR_FILE_L0;
                        numidx = OPS.NUML0(wip.get_filesize());
                        break;
                    case 1:
                        type = BLK_TYPE.REGULAR_FILE_L1;
                        numidx = OPS.NUML1(wip.get_filesize());
                        break;
                    case 2:
                        type = BLK_TYPE.REGULAR_FILE_L2;
                        numidx = OPS.NUML2(wip.get_filesize());
                        break;
                }
                for (int x = 0; x < numidx; x++)
                {
                    long dbn = wip.get_child_dbn(x);
                    if (dbn <= 0) continue;
                    apply_update_internal(dbn, type, childcnt, cu.optype, true);
                }
            }
        }

        private void do_regular_dirORfile_work(UpdateReqI cu, int childcnt)
        {
            Red_Buffer wb = allocate_wb(cu.blktype);
            byte[] buffer = new byte[OPS.FS_BLOCK_SIZE];

            lock (tfile0)
            {
                tfile0.Seek((long)cu.tfbn * OPS.FS_BLOCK_SIZE, SeekOrigin.Begin);
                tfile0.Read(tmpiodatatfileR, 0, OPS.FS_BLOCK_SIZE);
                OPS.Decrypt_Read_WRBuf(tmpiodatatfileR, buffer);
            }

            wb.data_to_buf(buffer);

            REDFS_BUFFER_ENCAPSULATED wbe = new REDFS_BUFFER_ENCAPSULATED(wb);

            BLK_TYPE belowtype = BLK_TYPE.IGNORE;

            switch (cu.blktype)
            {
                case BLK_TYPE.REGULAR_FILE_L1:
                    belowtype = BLK_TYPE.REGULAR_FILE_L0;
                    break;
                case BLK_TYPE.REGULAR_FILE_L2:
                    belowtype = BLK_TYPE.REGULAR_FILE_L1;
                    break;
            }

            for (int i = 0; i < 1024; i++)
            {
                long dbnt = wbe.get_child_dbn(i);
                if (dbnt <= 0) continue;
                apply_update_internal(dbnt, belowtype, childcnt, cu.optype, true);
            }
        }

        /*
         * Also write this file to the delete log,
         */
        private void checkset_if_blockfree(long dbn, int c)
        {
            int rbn = REFDEF.dbn_to_rbn(dbn);
            if (GLOBALQ.WRObj[rbn].incoretbuf.get_refcount(dbn) == 0)
            {
                DEFS.ASSERT(GLOBALQ.WRObj[rbn].incoretbuf.get_childcount(dbn) == 0,
                    "WTF happened? chdcnt = " + c + " -> " + GLOBALQ.WRObj[rbn].incoretbuf.get_childcount(dbn));
                lock (GLOBALQ.m_deletelog2)
                {
                    GLOBALQ.m_deletelog2.Add(dbn);
                }
                lock (GLOBALQ.m_deletelog_spanmap)
                {
                    //XXX todo, if bit is free, then we must also update the spanmap so it know that the bit became free and it can
                    //then update the used block counter.
                    GLOBALQ.m_deletelog_spanmap.Add(dbn);
                }
                GLOBALQ.WRObj[rbn].incoretbuf.set_childcount(dbn, 0); //must clear this.
            }
        }

        /*
         * Will block on GLOBALQ.m_reqi_queue and take it to
         * its logical conclusion.
         */
        public void tServiceThread()
        {
            //long protected_blkdiff_counter = 0;
            long[] protected_blkdiff_counter = new long[1024];

            while (true)
            {
                UpdateReqI cu = (UpdateReqI)GLOBALQ.m_reqi_queue.Take();

                if (cu.optype == REFCNT_OP.SHUT_DOWN)
                {
                    internal_sync_and_flush_cache_advanced();
                    DEFS.ASSERT(GLOBALQ.m_reqi_queue.Count == 0, "There cannot be any pending updates when shutting down");
                    //DEFS.DEBUGYELLOW("REF", "Bailing out now!!");
                    //dont take a lock here.

                    for (int i = 0; i < 1024; i++)
                    {
                        /* comment as not yet implimented
                        if (REDDY.FSIDList[i] == null || protected_blkdiff_counter[i] == 0)
                            continue;

                        REDDY.FSIDList[i].diff_upadate_logical_data(protected_blkdiff_counter[i]);
                        REDDY.FSIDList[i].set_dirty(true);
                        protected_blkdiff_counter[i] = 0;
                        ..end comment */
                    }

                    cu.processed = true;
                    m_initialized = false;
                    break;
                }

                if (cu.optype == REFCNT_OP.DO_SYNC)
                {
                    internal_sync_and_flush_cache_advanced();

                    //dont take a lock here.
                    for (int i = 0; i < 1024; i++)
                    {
                        /* comment as not yet implimented
                        if (REDDY.FSIDList[i] == null || protected_blkdiff_counter[i] == 0)
                            continue;

                        REDDY.FSIDList[i].diff_upadate_logical_data(protected_blkdiff_counter[i]);
                        REDDY.FSIDList[i].set_dirty(true);
                        protected_blkdiff_counter[i] = 0;
                         end comment..*/
                    }
                    cu.processed = true;
                    tfile0.Flush();
                    mfile1.Flush();
                    continue;
                }

                if (cu.optype == REFCNT_OP.TAKE_DISK_SNAPSHOT ||
                        cu.optype == REFCNT_OP.UNDO_DISK_SNAPSHOT)
                {
                    int rbn_update = cu.tfbn; //overloaded since its just file offset.
                    load_wrbufx(rbn_update); //will dowork
                    DEFS.ASSERT(cu.dbn == 0, "This should not be set");
                    DEFS.ASSERT(cu.optype == GLOBALQ.disk_snapshot_optype, "this must also match");
                    //DoSnapshotWork(rbn_update);
                    counter++;
                    total_ops++;
                    printspeed();
                    continue;
                }

                if (cu.dbn != 0)
                {
                    if (cu.optype == REFCNT_OP.DECREMENT_REFCOUNT_ONDEALLOC) protected_blkdiff_counter[cu.fsid] -= OPS.FS_BLOCK_SIZE;
                    else if (cu.optype == REFCNT_OP.INCREMENT_REFCOUNT_ALLOC) protected_blkdiff_counter[cu.fsid] += OPS.FS_BLOCK_SIZE;
                    //all other ops you can ignore.
                }
                else if (cu.dbn == 0 && cu.optype == REFCNT_OP.BATCH_INCREMENT_REFCOUNT_ALLOC)
                {
                    protected_blkdiff_counter[cu.fsid] += OPS.FS_SPAN_OUT * OPS.FS_BLOCK_SIZE;
                }

                int rbn = REFDEF.dbn_to_rbn(cu.dbn);
                total_ops++;
                counter++;

                /* 
                 * Now if this has a child update pending, then we must clean it up.
                 * For each entry, i.e dbn, load the upto 1024, into memory and update
                 * the refcount. Essentially when we access this buffer - it must not
                 * have any pending update to itself or its children.
                 * 
                 * How the children are updated depends on the blk_type, thats why so many
                 * cases.
                 */
                load_wrbufx(rbn);

                if (cu.optype == REFCNT_OP.GET_REFANDCHD_INFO)
                {
                    cu.processed = true;
                    continue;
                }

                int childcnt = GLOBALQ.WRObj[rbn].incoretbuf.get_childcount(cu.dbn);

                if (childcnt > 0)
                {
                    //DEFS.DEBUG("CNTr", "Encountered child update for " + cu.dbn + " = " +
                    //    GLOBALQ.WRObj[rbn].incoretbuf.get_refcount(cu.dbn) + "," + childcnt);

                    if (cu.blktype == BLK_TYPE.REGULAR_FILE_L0)// || cu.blktype == BLK_TYPE.DIRFILE_L0)
                    {
                        /* Normal handling*/
                        //DEFS.ASSERT(cu.blktype == GLOBALQ.WRObj[rbn].incoretbuf.get_blk_type(cu.dbn), "Block mismatch");
                        DEFS.ASSERT(cu.tfbn == -1, "tfbn cannot be set for a level 0 block generally");
                        DEFS.ASSERT(false, "How can there be a childcnt update for a level zero block?");
                    }
                    else if (cu.blktype == BLK_TYPE.REGULAR_FILE_L1 || cu.blktype == BLK_TYPE.REGULAR_FILE_L2)
                    {
                        //DEFS.ASSERT(false, "Not yet implimented chdcnt in wrloader : " + REFDEF.get_string_rep(cu));
                        DEFS.ASSERT(cu.tfbn != -1, "Tfbn should've been set here.");
                        do_regular_dirORfile_work(cu, childcnt);
                        GLOBALQ.WRObj[rbn].incoretbuf.set_childcount(cu.dbn, 0);
                    }
                    else if (cu.blktype == BLK_TYPE.PUBLIC_INODE_FILE_L0)
                    {
                        do_inode_refupdate_work(cu, childcnt);
                        GLOBALQ.WRObj[rbn].incoretbuf.set_childcount(cu.dbn, 0);
                    }
                    else if (cu.blktype == BLK_TYPE.BATCH_DBNS)
                    {
                        throw new SystemException("not yet implimented!");
                        //do_inode_refupdate_work(cu, childcnt);
                        //GLOBALQ.WRObj[rbn].incoretbuf.set_childcount(cu.dbn, 0);
                    }
                    else
                    {
                        DEFS.ASSERT(false, "passed type = " + cu.blktype + "dbn = " + cu.dbn + " chdcnt = " + childcnt);
                    }
                }

                if (cu.optype != REFCNT_OP.TOUCH_REFCOUNT)
                {
                    /* 
                     * Now that pending updates are propogated ,apply the queued update to this refcount.
                     * If it becomes free, notify that.
                     */
                    load_wrbufx(rbn);

                    if (cu.optype == REFCNT_OP.BATCH_INCREMENT_REFCOUNT_ALLOC)
                    {
                        apply_update_internal_batch(cu);
                    }
                    else
                    {
                        apply_update_internal(cu.dbn, cu.blktype, cu.value, cu.optype, (cu.optype == REFCNT_OP.INCREMENT_REFCOUNT));
                        checkset_if_blockfree(cu.dbn, childcnt);
                    }
                    
                }

                /* After the load, see if we have to clean up */
                if (cachesize > 15 * 1024)
                {
                    internal_sync_and_flush_cache_advanced();
                }
                printspeed();
            }

            tfile0.Flush();
            tfile0.Close();
            mfile1.Flush();
            mfile1.Close();
        }

        /*
         * Using the input as start_dbn and end_dbn so that caller is aware that its a contigious list
         */
        public void get_refcount_batch(long start_dbn, long end_dbn, int[] refcnt, int[] childcnt)
        {
            DEFS.ASSERT(end_dbn - start_dbn <= OPS.FS_SPAN_OUT, "Get batch refcount cannot be more than fs span out");

            UpdateReqI rStart = new UpdateReqI();
            rStart.optype = REFCNT_OP.GET_REFANDCHD_INFO;
            rStart.dbn = start_dbn;
            GLOBALQ.m_reqi_queue.Add(rStart);

            UpdateReqI rEnd = new UpdateReqI();
            rEnd.optype = REFCNT_OP.GET_REFANDCHD_INFO;
            rEnd.dbn = end_dbn;
            GLOBALQ.m_reqi_queue.Add(rEnd);

            while (rStart.processed == false || rEnd.processed == false)
            {
                Thread.Sleep(20);
            }

            int i = 0;
            for (long dbn = start_dbn; dbn < end_dbn; dbn++, i++)
            {
                int rbn = REFDEF.dbn_to_rbn(dbn);
                refcnt[i] = GLOBALQ.WRObj[rbn].incoretbuf.get_refcount(dbn);
                childcnt[i] = GLOBALQ.WRObj[rbn].incoretbuf.get_childcount(dbn);
            }
        }

        public void get_refcount(long dbn, ref int refcnt, ref int childcnt)
        {
            UpdateReqI r = new UpdateReqI();
            r.optype = REFCNT_OP.GET_REFANDCHD_INFO;
            r.dbn = dbn;
            GLOBALQ.m_reqi_queue.Add(r);

            int rbn = REFDEF.dbn_to_rbn(dbn);

            while (r.processed == false)
            {
                Thread.Sleep(10);
            }
            refcnt = GLOBALQ.WRObj[rbn].incoretbuf.get_refcount(dbn);
            childcnt = GLOBALQ.WRObj[rbn].incoretbuf.get_childcount(dbn);
        }

        public void touch_refcount(int fsid, Red_Buffer wb, bool isinodefilel0)
        {
            DEFS.ASSERT(wb != null, "touch refcount needs the wb");

            if (wb.get_touchrefcnt_needed() == false)// || wb.get_ondisk_dbn() == 0)
            {
                return;
            }
            else
            {
                wb.set_touchrefcnt_needed(false);
            }

            if (wb.get_level() == 0 && isinodefilel0)
            {
                mod_refcount(fsid, wb.get_ondisk_dbn(), REFCNT_OP.TOUCH_REFCOUNT, wb, true);
            }
            else
            {
                DEFS.ASSERT(wb.get_level() > 0, "touch_refcount is only for indirects only, except for ino-L0!");
                mod_refcount(fsid, wb.get_ondisk_dbn(), REFCNT_OP.TOUCH_REFCOUNT, wb, false);
            }
        }

        public void mod_refcount(int fsid, long dbn, REFCNT_OP optype, Red_Buffer wb, bool isinodefilel0)
        {
            DEFS.ASSERT(optype == REFCNT_OP.INCREMENT_REFCOUNT || /*optype == REFCNT_OP.DECREMENT_REFCOUNT ||*/
                    optype == REFCNT_OP.TOUCH_REFCOUNT || /*optype == REFCNT_OP.DO_LOAD || */
                    optype == REFCNT_OP.INCREMENT_REFCOUNT_ALLOC ||
                    optype == REFCNT_OP.DECREMENT_REFCOUNT_ONDEALLOC || 
                    optype == REFCNT_OP.BATCH_INCREMENT_REFCOUNT_ALLOC, "Wrong param in mod_refcount");

            DEFS.ASSERT(isinodefilel0 || (wb == null || wb.get_level() > 0), "wrong type to mod_refcount " + isinodefilel0 + (wb == null));

            UpdateReqI r = new UpdateReqI();
            r.optype = optype;
            r.dbn = dbn;
            r.fsid = fsid;

            switch (optype)
            {
                case REFCNT_OP.INCREMENT_REFCOUNT:
                case REFCNT_OP.INCREMENT_REFCOUNT_ALLOC:
                    r.value = 1;
                    break;
                //case REFCNT_OP.DECREMENT_REFCOUNT:
                case REFCNT_OP.DECREMENT_REFCOUNT_ONDEALLOC:
                    r.value = -1;
                    break;
                case REFCNT_OP.TOUCH_REFCOUNT:
                    //case REFCNT_OP.DO_LOAD:
                    r.value = 0;
                    break;
            }

            r.blktype = (wb != null) ? ((isinodefilel0) ? BLK_TYPE.PUBLIC_INODE_FILE_L0 : wb.get_blk_type()) :
                ((optype == REFCNT_OP.INCREMENT_REFCOUNT_ALLOC || optype == REFCNT_OP.DECREMENT_REFCOUNT_ONDEALLOC) ?
                BLK_TYPE.IGNORE : (optype == REFCNT_OP.BATCH_INCREMENT_REFCOUNT_ALLOC ? BLK_TYPE.BATCH_DBNS : BLK_TYPE.REGULAR_FILE_L0));

            if (wb != null && (wb.get_level() > 0 || BLK_TYPE.PUBLIC_INODE_FILE_L0 == r.blktype))
            {
                lock (tfile0)
                {
                    OPS.Encrypt_Data_ForWrite(tmpiodatatfileW, wb.buf_to_data());
                    tfile0.Seek((long)tfilefbn * OPS.FS_BLOCK_SIZE, SeekOrigin.Begin);
                    tfile0.Write(tmpiodatatfileW, 0, OPS.FS_BLOCK_SIZE);
                    r.tfbn = tfilefbn;
                    tfilefbn++;
                }
            }
            else
            {
                r.tfbn = -1;
            }

            if (optype != REFCNT_OP.INCREMENT_REFCOUNT_ALLOC && optype != REFCNT_OP.DECREMENT_REFCOUNT &&
                    optype != REFCNT_OP.DECREMENT_REFCOUNT_ONDEALLOC && optype != REFCNT_OP.TOUCH_REFCOUNT)
            {
                Console.WriteLine("REFCNT", "Queued update for " + r.blktype + ", dbn = " +
                        r.dbn + ", and operation = " + r.optype + ", transaction offset : " + r.tfbn);
            }

            GLOBALQ.m_reqi_queue.Add(r);
        }
    }
}