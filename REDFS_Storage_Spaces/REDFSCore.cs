using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace REDFS_ClusterMode
{
    public enum REDFS_OP
    {
        REDFS_READ,
        REDFS_WRITE
    }

    /*
     * Manages the following objects.
     * RedFSPersistantStorage : to read/write data including metadata and userdata.
     * REDFSBlockAllocator    : to manage block usage, updating refcounts etc.
     * 
     * The above two objects can also be used by Dedupe, redfscontainer for planning operations outside the redfscore object.
     * However, all operations related to the filesystem must go throught the REDFSCore object.
     */
    public class REDFSCore
    {
        public REDFSBlockAllocator redfsBlockAllocator;

        public RedFSPersistantStorage redFSPersistantStorage;

        private Boolean mTerminateThread = false;
        private Boolean mThreadTerminated = true;

        public ZBufferCache mFreeBufCache;

        private MD5 md5 = System.Security.Cryptography.MD5.Create();

        /*
         * Used in redfs_checkin_wip() and redfs_checkout_wip().
         * If wip is deleted, then remove it or else keep in memory till the program is completed.
         * To verify that what we read is the same as what we wrote out during the sesssion.
         * All updates to wip are written out during sync and fingerprints are updateed in the below dictionary
         * 
         * XXX this works only when creating new wips in a new (test) session and does not capture corruptions when opening
         * an existing container and reading existing files.
         */ 
        public IDictionary[] wip_checkin_fps = new Dictionary<int, string>[1024];

        public REDFSCore(string containerName)
        {
            redFSPersistantStorage = new RedFSPersistantStorage(containerName);

            redfsBlockAllocator = new REDFSBlockAllocator();

            if (!redfsBlockAllocator.InitBlockAllocator())
            {
                DEFS.ASSERT(false, "Exiting since block allocator has failed to init");
                System.Environment.Exit(-11);
            }

            mFreeBufCache = new ZBufferCache();
            mFreeBufCache.init();

            Thread tc = new Thread(new ThreadStart(tServiceThread));
            tc.Start();
        }

        //for testing helper functions. Do not create threads or the blockAllocator or persistant storage.
        public REDFSCore()
        {

        }

        /*
         * This guy must take care of WRBufs and flush them if dirty. This cleaning should
         * happen bottom up.
         */
        public void tServiceThread()
        {

            while (!mTerminateThread)
            {
                RedFS_Inode wip = (RedFS_Inode)GLOBALQ.m_wipdelete_queue.Take();
                if (wip.get_ino() == -1)
                {
                    mTerminateThread = true;
                }
                else
                {
                    int deallocs = delete_wip_internal2(wip);
                    Thread.Sleep(10); //wait to drain

                    for (int i = 0; i < OPS.NUM_PTRS_IN_WIP; i++) { wip.set_child_dbn(i, DBN.INVALID); }
                    wip.is_dirty = false;
                    wip.set_filesize(0);
                    wip.set_ino(0, -1);
                }

                //not critical if we shutdown without processing this queue, but will lead to leakage

                //XXX regular frees may bring out random dbns which cause the dbn to be reset and then which slows
                //down say an ongoing copying process.
                /*
                do
                {
                    
                    lock (GLOBALQ.m_deletelog3)
                    {
                        int count = GLOBALQ.m_deletelog3.Count;
                        try
                        {
                            for (int i = 0; i < count; i++)
                            {
                                long freedbn = GLOBALQ.m_deletelog3[0];
                                GLOBALQ.m_deletelog3.RemoveAt(0);


                                DBNSegmentSpan segment = redfsBlockAllocator.dbnSpanMap.startDBNToDBNSegmentSpan[DBNSegmentSpan.GetDBNSpaceSegmentOffset(freedbn)];
                                DEFS.ASSERT(segment.type == SPAN_TYPE.DEFAULT, "should be default as of now!");

                                if (redfsBlockAllocator.quickSearchStartDbn[0] > freedbn)
                                {
                                    redfsBlockAllocator.quickSearchStartDbn[0] = freedbn;
                                }
                                segment.totalFreeBlocks++;
                                totalFreedBlocks++;
                            }
                        }
                        catch (Exception e)
                        {
                            throw new SystemException("EXEPTION : Caught in m_deletelog3 : cnt = " + count + " and size = " +
                                GLOBALQ.m_deletelog3.Count + " e.msg = " + e.Message);
                        }
                    }
                } while (GLOBALQ.m_deletelog3.Count > 0);
                */
                if (mTerminateThread)
                {
                    break;
                }
            }
            mThreadTerminated = true;
        }

        /*
         * Should be called only if the container is being initialized for the first time
         */
        public RedFS_FSID CreateZeroRootVolume()
        {
            /*
             * Now lets add the base chunk as the first default type segment in dbn space,
             * We should add the first chunk by default in normal mode. For tests we add
             * this into our span manually
             */
            if (!REDFS.isTestMode)
            {
                RAWSegment[] dataDefault1 = new RAWSegment[1];
                dataDefault1[0] = new RAWSegment(SEGMENT_TYPES.Default, 0, 0);

                DBNSegmentSpan dss1 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, 0, dataDefault1, null);

                RAWSegment[] dataDefault2 = new RAWSegment[1];
                dataDefault2[0] = new RAWSegment(SEGMENT_TYPES.Default, 0, 1);
                DBNSegmentSpan dss2 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, OPS.NUM_DBNS_IN_1GB, dataDefault2, null);

                DBNSegmentSpanMap spanMap = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;
                spanMap.InsertDBNSegmentSpan(dss1);
                spanMap.InsertDBNSegmentSpan(dss2);

                //Now mark those chunk segments as used.
                //((REDFSChunk)redfsBlockAllocator.redfsChunks_inner[0]).isSegmentInUse[  -= 2048;
                //REDFS.redfsContainer.SaveChunkListToDisk("from newcontainer create");
            }
            RedFS_FSID wbfsid  = CreateEmptyFSID(0);
            redFSPersistantStorage.write_fsid(wbfsid);
            return wbfsid;
        }

        public RedFS_FSID CreateEmptyFSID(int newFsidId)
        {
            RedFS_FSID newFsid = new RedFS_FSID(newFsidId, 0);

            RedFS_Inode iMapWip = new RedFS_Inode(WIP_TYPE.PUBLIC_INODE_MAP, 1, -1);
            RedFS_Inode inodeFile = new RedFS_Inode(WIP_TYPE.PUBLIC_INODE_FILE, 0, -1);

            iMapWip.setfilefsid_on_dirty(newFsidId);
            inodeFile.setfilefsid_on_dirty(newFsidId);

            lock (inodeFile)
            {
                redfs_resize_wip(newFsidId, inodeFile, (long)128 * 1024 * 1024 * 1024, (newFsidId != 0));

                redfs_resize_wip(newFsidId, iMapWip, ((long)inodeFile.get_filesize() / OPS.WIP_SIZE) / 8, (newFsidId != 0));

                DEFS.ASSERT(inodeFile.get_filesize() == (long)128 * 1024 * 1024 * 1024, "inowip size mismatch " + inodeFile.get_filesize());
                DEFS.ASSERT(iMapWip.get_filesize() == ((long)inodeFile.get_filesize() / OPS.WIP_SIZE) / 8, "iMapWip size mismatch " + iMapWip.get_filesize());

                inodeFile.isWipValid = true;
                iMapWip.isWipValid = true;

                newFsid.set_inodefile_wip(inodeFile);
                newFsid.set_inodemap_wip(iMapWip);
                newFsid.set_logical_data(0);
                newFsid.set_start_inonumber(64);

                inodeFile.isWipValid = true;
                iMapWip.isWipValid = true;

                PrintableWIP pwip2bx = redfs_list_tree(inodeFile, new long[] { 0 }, new int[] { 0 });

                sync(inodeFile);
                sync(iMapWip);
                flush_cache(inodeFile, true);
                flush_cache(iMapWip, true);

                DEFS.ASSERT(inodeFile.get_incore_cnt() == 0, "Dont cache blocks during init");
                DEFS.ASSERT(iMapWip.get_incore_cnt() == 0, "Dont cache blocks during init");

                wip_checkin_fps[newFsidId] = new Dictionary<int, string>();
            }
            redfsBlockAllocator.allocBitMap32TBFile.fsid_setbit(newFsid.get_fsid());

            return newFsid;
        }

        public RedFS_FSID redfs_dup_fsid(RedFS_FSID bfs)
        {
            if (bfs.isDirty())
            {
                Console.WriteLine("Cannot dup an fsid which is dirty!");
                return null;
            }

            int target = -1;
            for (int i = 0; i < 1024; i++)
            {
                if (redfsBlockAllocator.allocBitMap32TBFile.fsid_checkbit(i) == false)
                {
                    target = i;
                    break;
                }
            }

            redfsBlockAllocator.allocBitMap32TBFile.fsid_setbit(target);

            RedFS_FSID newone = new RedFS_FSID(target, bfs.get_fsid(), bfs.data);
            redfs_commit_fsid(newone);

            // Update refcounts here 
            RedFS_Inode inowip = bfs.get_inode_file_wip("FSID_DUP");
            RedFS_Inode imapwip = bfs.get_inodemap_wip();

            RedFS_Inode newinowip = newone.get_inode_file_wip("fsid dup");
            RedFS_Inode newimapwip = newone.get_inodemap_wip();

            DEFS.ASSERT(inowip.Equals(newinowip), "ino wips must match after fsid duping!");
            DEFS.ASSERT(imapwip.Equals(newimapwip), "imap wips must match after fsid duping!");

            lock(imapwip)
            {
                if (imapwip.get_inode_level() == 1)
                {
                    for (int i = 0; i < OPS.NUML1(imapwip.get_filesize()); i++)
                    {
                        RedBufL1 wbl1 = (RedBufL1)redfs_load_buf(imapwip, 1, OPS.PIDXToStartFBN(1, i), false);
                        redfsBlockAllocator.increment_refcount(newone.get_fsid(), 1, wbl1, false);
                    }
                }
                else
                {
                    throw new SystemException("imap file should be level 1 file");
                }
            }

            lock (inowip)
            {
                if (inowip.get_inode_level() == 2)
                {
                    for (int i = 0; i < OPS.NUML2(inowip.get_filesize()); i++)
                    {
                        RedBufL2 wbl2 = (RedBufL2)redfs_load_buf(inowip, 2, OPS.PIDXToStartFBN(2, i), false);
                        redfsBlockAllocator.increment_refcount(newone.get_fsid(), inowip.get_ino(), wbl2, false);
                    }
                } 
                else
                {
                    throw new SystemException("Inode file must be L2 (level 2 file)");
                }
            }

            inowip.isWipValid = true;
            imapwip.isWipValid = true;
            wip_checkin_fps[target] = new Dictionary<int, string>();

            return newone;
        }

        public void InsertChunk(ChunkInfo b2)
        {
            redFSPersistantStorage.InsertChunk(b2);
        }

        public void ShutDown()
        {
            mTerminateThread = true;
            GLOBALQ.m_wipdelete_queue.Add(new RedFS_Inode(WIP_TYPE.REGULAR_FILE, -1, 0));
            while (!mThreadTerminated)
            {
                Thread.Sleep(100);
            }

            redFSPersistantStorage.shut_down();
            redfsBlockAllocator.SyncAndTerminate();
            mFreeBufCache.shutdown();
        }

        //----------------------------------------------------------------------------------------------------------------------
        //                                          Private methods
        //----------------------------------------------------------------------------------------------------------------------

        // Must not do sync, but just remove non-dirty buffers.
        // It might be the case that an L0 which is dirty is kept and and L1 which is non-dirty is cleaned.
        // Eventually when that L0 becomes clean, we dont have L1 to write out the dbn and we assert in get_buf3()
        // So if L0's are present (i.e still dirty), track the start fbns, and keep the l1's in memory and even corresponding L2 if needed

        public void flush_cache(RedFS_Inode wip, bool inshutdown)
        {

            lock (wip)
            {
                List<Red_Buffer> newListL0 = new List<Red_Buffer>();

                long[] start_fbn_tracker = new long[wip.L0list.Count]; //may repeat no issues
                int start_fbn_tracker_ctr = 0;

                for (int i = 0; i < wip.L0list.Count; i++)
                {
                    RedBufL0 wbl0 = (RedBufL0)wip.L0list[i];
                    //XXX should we have some kind of TTL??
                    if (wbl0.is_dirty == false)
                    {
                        mFreeBufCache.deallocate4(wbl0, "FlushCache:" + wip.get_ino() + " cnt:" + wip.L0list.Count);
                    }
                    else
                    {
                        newListL0.Add(wbl0);
                        int idx_in_parent = wbl0.myidx_in_myparent();
                        if (wip.get_inode_level() > 0)
                        {
                            RedBufL1 wbl1 = (RedBufL1)get_buf3("gc", wip, 1, wbl0.m_start_fbn, true);
                            DEFS.ASSERT(wbl1.get_child_dbn(idx_in_parent) == wbl0.m_dbn, "Issue during garbage collection, " + wbl1.get_child_dbn(idx_in_parent) + " vs " + wbl0.m_dbn);
                        }
                        start_fbn_tracker[start_fbn_tracker_ctr++] = OPS.SomeFBNToStartFBN(wip.get_inode_level(), wbl0.m_start_fbn);
                    }
                }
                wip.L0list.Clear();
                wip.L0list = newListL0;

                List<Red_Buffer> newListL1 = new List<Red_Buffer>();

                for (int i = 0; i < wip.L1list.Count; i++)
                {
                    RedBufL1 wbl1 = (RedBufL1)wip.L1list[i];

                    bool need_to_keep_L1 = false;
                    for (int ctr = 0; ctr < start_fbn_tracker_ctr; ctr++)
                    {
                        if (start_fbn_tracker[ctr] == wbl1.m_start_fbn)
                        {
                            need_to_keep_L1 = true;
                            break;
                        }
                    }

                    if (wbl1.is_dirty == true || need_to_keep_L1 == true)
                    {
                        newListL1.Add(wbl1);
                    }
                    else
                    {
                        if (wip._lasthitbuf != null && wip._lasthitbuf.get_start_fbn() == wbl1.m_start_fbn)
                        {
                            wip._lasthitbuf = null; //we are removing so mark this null
                        }
                    }
                }
                wip.L1list.Clear();
                wip.L1list = newListL1;

                List<Red_Buffer> newListL2 = new List<Red_Buffer>();

                for (int i = 0; i < wip.L2list.Count; i++)
                {
                    RedBufL2 wbl2 = (RedBufL2)wip.L2list[i];

                    bool need_to_keep_L2 = false;
                    for (int ctr = 0; ctr < start_fbn_tracker_ctr; ctr++)
                    {
                        if (start_fbn_tracker[ctr] == wbl2.m_start_fbn)
                        {
                            need_to_keep_L2 = true;
                            break;
                        }
                    }

                    if (wbl2.is_dirty|| need_to_keep_L2)
                    {
                        newListL2.Add(wbl2);
                    }
                }
                wip.L2list.Clear();
                wip.L2list = newListL2;

                if (!(!inshutdown || wip.get_incore_cnt() == 0))
                {
                    //redfs_show_vvbns2(wip, true);
                    //OPS.dumplistcontents(wip.L0list);
                }

                DEFS.ASSERT(!inshutdown || wip.get_incore_cnt() == 0, "Cannot have dirty buffers incore " +
                    " when doing a flush cache during shutdown (" + wip.L0list.Count + "," + wip.L1list.Count +
                    "," + wip.L2list.Count + ") : " + wip.get_string_rep2());
            }
        }

        public int NEXT_INODE_NUMBER(RedFS_FSID fsid)
        {
            lock (this)
            {
                int ino = find_free_ino_bit(fsid);
                return ino;
            }
        }

        /*
        * Scans the given 8k buffer, and returns the offset of a free bit.
        * offset can vary between 0 and 8192*8
        */
        public int get_free_bitoffset(int startsearchoffset, byte[] data)
        {
            DEFS.ASSERT(data.Length == OPS.FS_BLOCK_SIZE && startsearchoffset < OPS.FS_BLOCK_SIZE, "get_free_bitoffset input must be a " +
                            " buffer of size 8192, but passed size = " + data.Length);

            for (int offset = startsearchoffset; offset < data.Length; offset++)
            {
                if (data[offset] != (byte)0xFF)
                {
                    int x = OPS.get_first_free_bit(data[offset]);
                    data[offset] = OPS.set_free_bit(data[offset], x);
                    return (offset * 8 + x);
                }
            }
            return -1;
        }

        /*
         * Give a fsid, it looks into the iMapWip and gets a free bit. The fsid block has the
         * largest inode number that is currently used, and the iMapWip itself. I'm not using anylocks
         * for this wip since this operation will never be concurrent. All FS modification code that
         * may use this path already would have a lock on the rootdir. Ex duping, deleting, inserting etc.
         * 
         * XXX: Note that we are never freeing the inode bit once set!. So basically this is a dummy function.
         * We still work because we can afford to wait for 500M inodes to allocated before we do a wrap around!!.
         */
        private int find_free_ino_bit(RedFS_FSID fsid)
        {
            int max_fbns = 8192; //num blocks in inodeMapFile

            int curr_max_inode = fsid.get_start_inonumber();

            byte[] buffer = new byte[OPS.FS_BLOCK_SIZE];

            RedFS_Inode iMapWip = fsid.get_inodemap_wip();
            long fbn = OPS.OffsetToFBN(curr_max_inode / 8);

            for (long cfbn = fbn; cfbn < max_fbns; cfbn++)
            {
                Array.Clear(buffer, 0, OPS.FS_BLOCK_SIZE);
                
                redfs_read(iMapWip, (cfbn * OPS.FS_BLOCK_SIZE), buffer, 0, OPS.FS_BLOCK_SIZE);

                int startsearchoffset = ((cfbn == fbn) ? (curr_max_inode / 8) : 0) % OPS.FS_BLOCK_SIZE;

                int free_bit = get_free_bitoffset(startsearchoffset, buffer);
                if (free_bit != -1)
                {
                    int free_inode = (int)(cfbn * (OPS.FS_BLOCK_SIZE * 8) + free_bit);
                    redfs_write(iMapWip, (cfbn * OPS.FS_BLOCK_SIZE), buffer, 0, OPS.FS_BLOCK_SIZE, WRITE_TYPE.OVERWRITE_IN_PLACE);

                    sync(iMapWip);
                    fsid.set_inodemap_wip(iMapWip);
                    flush_cache(iMapWip, true);

                    fsid.set_start_inonumber(free_inode + 1);
                    redfs_commit_fsid(fsid);

                    DEFS.ASSERT(iMapWip.get_incore_cnt() == 0, "Dont cache imap blocks");
                    return free_inode;
                }
            }

            fsid.set_start_inonumber(64);
            redfs_commit_fsid(fsid); //do we need this regularly?
            DEFS.ASSERT(false, "XXXXX VERY RARE EVENT XXXX INODE WRAP AROUND XXXX");
            return find_free_ino_bit(fsid);
        }

        public bool redfs_checkout_wip(RedFS_Inode inowip, RedFS_Inode mywip, int m_ino)
        {
            WIP_TYPE oldtype = mywip.get_wiptype();

            //not required in Release.
            for (int i = 0; i < 16; i++)
            {
                DEFS.ASSERT(mywip.get_child_dbn(i) == DBN.INVALID, "Wip cannot be valid during checkout, " +
                        i + " value = " + mywip.get_child_dbn(i));
            }
            long fileoffset = m_ino * OPS.WIP_SIZE;

            byte[] buffer = new byte[OPS.WIP_SIZE];
            lock (inowip)
            {
                lock (mywip)
                {
                    redfs_read(inowip, fileoffset, buffer, 0, OPS.WIP_SIZE);
                    mywip.parse_bytes(buffer);

                    DEFS.ASSERT(!Array.TrueForAll(buffer, b => (b == 0)), "We have read 0'd data for wip, this cannot happen for a valid inode number");
                    DEFS.ASSERT(mywip.isWipValid, "wip should be marked valid after checkout!");

                    if (oldtype != WIP_TYPE.UNDEFINED)
                        mywip.set_wiptype(oldtype);
                }
            }

            //Now lets mark it clean, since we havent actually modified any info
            mywip.is_dirty = false;
            mywip.update_fingerprint();

            int fsid = inowip.get_filefsid();
            if (wip_checkin_fps[fsid][m_ino] != null) {
                string incore = (string)wip_checkin_fps[inowip.get_filefsid()][m_ino];
                DEFS.ASSERT(mywip.fingerprint == incore, "wip has been corrupted while reading and it does not match the exisitng incore fingerprint");
            }
            DEFS.ASSERT(mywip.get_ino() == m_ino, "Ino should match on checkout!");
            return mywip.verify_inode_number();
        }

        public bool redfs_punch_wip_hole(RedFS_Inode inowip, RedFS_Inode mywip, int m_ino)
        {
            long fileoffset = m_ino * OPS.WIP_SIZE;
            lock (inowip)
            {
                lock (mywip)
                {
                    byte[] zeroBuffer = new byte[OPS.WIP_SIZE];
                    Array.Clear(zeroBuffer, 0, OPS.WIP_SIZE);

                    redfs_write(inowip, fileoffset, zeroBuffer, 0, OPS.WIP_SIZE, WRITE_TYPE.OVERWRITE_IN_PLACE);
                    inowip.is_dirty = true;
                    wip_checkin_fps[inowip.get_filefsid()][m_ino] = "0";
                }
            }
            return true;
        }

        public bool redfs_checkin_wip(RedFS_Inode inowip, RedFS_Inode mywip, int m_ino)
        {
            long fileoffset = m_ino * OPS.WIP_SIZE;
            lock (inowip)
            {
                lock (mywip)
                {
                    DEFS.ASSERT(mywip.isWipValid, "wip must be valid during checkin");
                    mywip.update_fingerprint();
                    redfs_write(inowip, fileoffset, mywip.data, 0, OPS.WIP_SIZE, WRITE_TYPE.OVERWRITE_IN_PLACE);
                    mywip.is_dirty = false;
                    inowip.is_dirty = true;
                    DEFS.ASSERT(!Array.TrueForAll(mywip.data, b => (b == 255)), "We have read FF'd data for wip, this cannot happen for a valid inode number 322");
                    DEFS.ASSERT(!Array.TrueForAll(mywip.data, b => (b == 0)), "We have read 0'd data for wip, this cannot happen for a valid inode number 322");
                    wip_checkin_fps[inowip.get_filefsid()][m_ino] = mywip.fingerprint;
                }
            }
            return mywip.verify_inode_number();
        }

        private Boolean shouldSave(long[] fbns, int[] levels, long curr_fbn, int curr_level)
        {
            if (fbns == null || levels == null || fbns.Length == 0)
            {
                return false;
            }
            DEFS.ASSERT(fbns.Length == levels.Length, "should be dqual");

            for (int i=0;i<fbns.Length;i++)
            {
                if (fbns[i] == curr_fbn && levels[i] == curr_level)
                {
                    fbns[i] = -1;
                    levels[i] = 0;
                    return true;
                }
            }
            return false;
        }

        public void redfs_get_refcounts(long[] dbns, out int[] refcnts, out int[] childcnts)
        {
            if (dbns == null || dbns.Length == 0)
            {
                refcnts = null;
                childcnts = null;
                return;
            }
            refcnts = new int[dbns.Length];
            childcnts = new int[dbns.Length];

            for (int i = 0; i < dbns.Length; i++)
            {
                int refcnt2a = 0, childrefcnt2a = 0;
                if (dbns[i] != 0) {
                    redfsBlockAllocator.GetRefcounts(dbns[i], ref refcnt2a, ref childrefcnt2a);
                }
                refcnts[i] = refcnt2a;
                childcnts[i] = childrefcnt2a;
            }
        }

        /*
         * return json string, so its easy to work with
         */ 
        public PrintableWIP redfs_list_tree(RedFS_Inode wip, long[] fbns, int[] levels)
        {
            PrintableWIP pwip = new PrintableWIP();

            long numDbns = OPS.NUML0(wip.get_filesize()) ;
            long numL1s = OPS.NUML1(wip.get_filesize());
            long numL2s = OPS.NUML2(wip.get_filesize());

            pwip.ino = wip.get_ino();
            pwip.pino = wip.get_parent_ino();
            pwip.spanType = wip.spanType.ToString();
            pwip.wipType = wip.get_wiptype().ToString();
            pwip.length = wip.get_filesize();

            if (wip.get_inode_level() == 0)
            {
                pwip.level = 0;
                pwip.wipIdx = new long[numDbns];
                for (int i=0;i<numDbns;i++)
                {
                    pwip.wipIdx[i] = wip.get_child_dbn(i);
                    if (wip.get_child_dbn(i) != 0)
                    {
                        pwip.ondiskL0Blocks++;
                    }

                    if (shouldSave(fbns, levels, i, 0))
                    {
                        Red_Buffer rf = redfs_load_buf(wip, 0, i, false);
                        DEFS.ASSERT(rf.get_ondisk_dbn() == wip.get_child_dbn(i), "dbns should match in get_tree");
                        pwip.requestedBlocks.Add(rf);
                    }
                }
            } 
            else if (wip.get_inode_level() == 1)
            {
                pwip.level = 1;
                pwip.wipIdx = new long[numL1s];
                pwip.L0_DBNS = new long[numDbns];

                int L0ctr = (int)numDbns;
                for (int i = 0; i < numL1s; i++)
                {
                    long dbnL1 = wip.get_child_dbn(i);
                    if (dbnL1 == 0)
                    {
                        L0ctr -= OPS.FS_BLOCK_SIZE;
                        continue;
                    }

                    pwip.ondiskL1Blocks++;
                    RedBufL1 wbl1 =  (RedBufL1)redfs_load_buf(wip, 1, (long)i * OPS.FS_SPAN_OUT, false);
                    pwip.wipIdx[i] = wip.get_child_dbn(i);

                    int count = (L0ctr > OPS.FS_SPAN_OUT) ? OPS.FS_SPAN_OUT : L0ctr;

                    for (int j=0;j<count;j++)
                    {
                        pwip.L0_DBNS[i * OPS.FS_SPAN_OUT + j] = wbl1.get_child_dbn(j);
                        if (wbl1.get_child_dbn(j) != 0)
                        {
                            pwip.ondiskL0Blocks++;
                        }

                        if (shouldSave(fbns, levels, i * OPS.FS_SPAN_OUT + j, 0))
                        {
                            Red_Buffer rf = redfs_load_buf(wip, 0, i, false);
                            pwip.requestedBlocks.Add(rf);
                            DEFS.ASSERT(rf.get_ondisk_dbn() == wbl1.get_child_dbn(j), "dbns should match in get_tree");
                        }
                    }

                    if (shouldSave(fbns, levels, i * OPS.FS_SPAN_OUT, 1))
                    {
                        Red_Buffer rf = redfs_load_buf(wip, 1, i * OPS.FS_SPAN_OUT, false);
                        pwip.requestedBlocks.Add(rf);
                        DEFS.ASSERT(rf.get_ondisk_dbn() == wip.get_child_dbn(i), "dbns should match in get_tree");
                    }
                    L0ctr -= count;
                }
            }
            else if (wip.get_inode_level() == 2)
            {
                pwip.level = 2;
                pwip.wipIdx = new long[numL2s];
                pwip.L1_DBNS = new long[numL1s];
                pwip.L0_DBNS = new long[numDbns];

                int L1ctr = (int)numL1s;
                for (int k=0;k<numL2s;k++)
                {
                    long dbnL2 = wip.get_child_dbn(k);
                    if (dbnL2 == 0)
                    {
                        numDbns -= OPS.FS_BLOCK_SIZE * OPS.FS_BLOCK_SIZE;
                        continue;
                    }
                    pwip.wipIdx[k] = wip.get_child_dbn(k);
                    pwip.ondiskL2Blocks++;

                    RedBufL2 wbl2 = (RedBufL2)redfs_load_buf(wip, 2, (long)k * OPS.FS_SPAN_OUT * OPS.FS_SPAN_OUT, false);

                    int countL1 = (L1ctr > OPS.FS_SPAN_OUT) ? OPS.FS_SPAN_OUT : L1ctr;

                    for (int j = 0; j < countL1; j++)
                    {
                        long dbnL1 = wbl2.get_child_dbn(j);

                        if (dbnL1 == 0)
                        {
                            numDbns -= OPS.FS_BLOCK_SIZE;
                            continue;
                        }

                        pwip.ondiskL1Blocks++;
                        pwip.L1_DBNS[k * OPS.FS_SPAN_OUT + j] = wbl2.get_child_dbn(j);

                        RedBufL1 wbl1 = (RedBufL1)redfs_load_buf(wip, 1, (long)k * OPS.FS_SPAN_OUT + j, false);

                        int count = (numDbns > OPS.FS_SPAN_OUT) ? OPS.FS_SPAN_OUT :  (int)(numDbns % OPS.FS_SPAN_OUT);

                        for (int m = 0; m < count; m++)
                        {
                            pwip.L0_DBNS[k * OPS.FS_SPAN_OUT * OPS.FS_SPAN_OUT +  j * OPS.FS_SPAN_OUT + m] = wbl1.get_child_dbn(m);

                            if (wbl1.get_child_dbn(m) != 0)
                            {
                                pwip.ondiskL0Blocks++;
                            }

                            if (shouldSave(fbns, levels, k * OPS.FS_SPAN_OUT * OPS.FS_SPAN_OUT + j * OPS.FS_SPAN_OUT + m, 0))
                            {
                                Red_Buffer rf = redfs_load_buf(wip, 0, k * OPS.FS_SPAN_OUT * OPS.FS_SPAN_OUT + j * OPS.FS_SPAN_OUT + m, false);
                                pwip.requestedBlocks.Add(rf);
                                DEFS.ASSERT(rf.get_ondisk_dbn() == wbl1.get_child_dbn(m), "dbns should match in get_tree");
                            }
                        }

                        if (shouldSave(fbns, levels, k * OPS.FS_SPAN_OUT * OPS.FS_SPAN_OUT + j * OPS.FS_SPAN_OUT, 1))
                        {
                            Red_Buffer rf = redfs_load_buf(wip, 1, k * OPS.FS_SPAN_OUT * OPS.FS_SPAN_OUT + j * OPS.FS_SPAN_OUT, false);
                            pwip.requestedBlocks.Add(rf);
                            DEFS.ASSERT(rf.get_ondisk_dbn() == wbl2.get_child_dbn(j), "dbns should match in get_tree");
                        }

                        numDbns -= count;
                    }

                    if (shouldSave(fbns, levels, k * OPS.FS_SPAN_OUT * OPS.FS_SPAN_OUT, 2))
                    {
                        Red_Buffer rf = redfs_load_buf(wip, 2, k * OPS.FS_SPAN_OUT * OPS.FS_SPAN_OUT, false);
                        pwip.requestedBlocks.Add(rf);
                        DEFS.ASSERT(rf.get_ondisk_dbn() == wip.get_child_dbn(k), "dbns should match in get_tree");
                    }

                    L1ctr -= countL1;
                }
            }

            if (wip.get_filesize() < OPS.FS_BLOCK_SIZE && wip.get_wiptype() == WIP_TYPE.DIRECTORY_FILE && 
                wip.get_inode_level() == 0)
            {
                byte[] buffer = new byte[wip.get_filesize()];
                redfs_read(wip, 0, buffer, 0, buffer.Length);
                pwip.json = System.Text.Encoding.UTF8.GetString(buffer);
            }
            return pwip;
        }

        public void redfs_commit_fsid(RedFS_FSID fsidblk)
        {
            fsidblk.sync_internal();
            redFSPersistantStorage.write_fsid(fsidblk);
            fsidblk.set_dirty(false);
        }

        // If i want to delete a L0, then first i must propogate all the refcounts from the indirect first.
        // Then i will have to pass the indirect + its dbn first for the refcounts to be propogated downward.
        // After that i can reduce the refcount of the L1

        private int delete_wip_internal2(RedFS_Inode wip)
        {
            DEFS.ASSERT(wip.L0list.Count == 0, "delete_wip_internal2 shouldve flushed cached, L0");
            DEFS.ASSERT(wip.L1list.Count == 0, "delete_wip_internal2 shouldve flushed cached, L1");
            DEFS.ASSERT(wip.L2list.Count == 0, "delete_wip_internal2 shouldve flushed cached, L2");

            int numDeallocsCalled = 0;

            wip._lasthitbuf = null; //we are removing so mark this null

            if (wip.get_inode_level() == 0)
            {
                for (int i = 0; i < OPS.NUML0(wip.get_filesize()); i++)
                {
                    long dbnl0 = wip.get_child_dbn(i);
                    DEFS.ASSERT(dbnl0 != DBN.INVALID, "Invalid dbn found in valid dbn range!");
                    if (dbnl0 > 0)
                    {
                        redfsBlockAllocator.decrement_refcount_ondealloc(wip.get_filefsid(), dbnl0);
                        numDeallocsCalled++;
                    }
                }
            }
            else if (wip.get_inode_level() == 1)
            {
                int counter = OPS.NUML0(wip.get_filesize());
                for (int i = 0; i < OPS.NUML1(wip.get_filesize()); i++)
                {
                    RedBufL1 wbl1 = (RedBufL1)redfs_load_buf(wip, 1, OPS.PIDXToStartFBN(1, i), false);
                    int idx = 0;
                    redfsBlockAllocator.touch_refcount(wip.get_filefsid(), wip.get_ino(), wbl1, false);
                    while (counter > 0 && idx < OPS.FS_SPAN_OUT)
                    {
                        long dbnl0 = wbl1.get_child_dbn(idx);
                        DEFS.ASSERT(dbnl0 != DBN.INVALID, "Invalid dbn found in valid dbn range!");
                        if (dbnl0 > 0)
                        {
                            redfsBlockAllocator.decrement_refcount_ondealloc(wip.get_filefsid(), dbnl0);
                            numDeallocsCalled++;
                        }
                        counter--;
                        idx++;
                    }
                    DEFS.ASSERT(wbl1.m_dbn != DBN.INVALID && wbl1.m_dbn > 0, "Invalid dbn found in valid dbn range!");
                    redfsBlockAllocator.decrement_refcount_ondealloc(wip.get_filefsid(), wbl1.m_dbn);
                    numDeallocsCalled++;
                }
            }
            else if (wip.get_inode_level() == 2)
            {
                int counter = OPS.NUML0(wip.get_filesize());
                int numl1s_remaining = OPS.NUML1(wip.get_filesize());

                for (int i2 = 0; i2 < OPS.NUML2(wip.get_filesize()); i2++)
                {
                    RedBufL2 wbl2 = (RedBufL2)redfs_load_buf(wip, 2, OPS.PIDXToStartFBN(2, i2), false);
                    redfsBlockAllocator.touch_refcount(wip.get_filefsid(), wip.get_ino(), wbl2, false);
                    int curr_l1cnt = (numl1s_remaining > OPS.FS_SPAN_OUT) ? OPS.FS_SPAN_OUT : numl1s_remaining;

                    for (int i1 = 0; i1 < curr_l1cnt; i1++)
                    {
                        RedBufL1 wbl1 = (RedBufL1)redfs_load_buf(wip, 1, OPS.PIDXToStartFBN(1, i1), false);
                        int idx = 0;

                        redfsBlockAllocator.touch_refcount(wip.get_filefsid(), wip.get_ino(), wbl1, false);
                        while (counter > 0 && idx < OPS.FS_SPAN_OUT)
                        {
                            long dbnl0 = wbl1.get_child_dbn(idx);
                            DEFS.ASSERT(dbnl0 != DBN.INVALID, "Invalid dbn found in valid dbn range!");
                            if (dbnl0 > 0)
                            {
                                redfsBlockAllocator.decrement_refcount_ondealloc(wip.get_filefsid(), dbnl0);
                                numDeallocsCalled++;
                            }
                            counter--;
                            idx++;
                        }
                        DEFS.ASSERT(wbl1.m_dbn != DBN.INVALID && wbl1.m_dbn > 0, "Invalid dbn found in valid dbn range!");
                        redfsBlockAllocator.decrement_refcount_ondealloc(wip.get_filefsid(), wbl1.m_dbn);
                        numDeallocsCalled++;
                    }

                    DEFS.ASSERT(wbl2.m_dbn != DBN.INVALID && wbl2.m_dbn > 0, "Invalid dbn found in valid dbn range!");
                    redfsBlockAllocator.decrement_refcount_ondealloc(wip.get_filefsid(), wbl2.m_dbn);
                    numDeallocsCalled++;
                }
            }
            return numDeallocsCalled;
        }

        /*
         * Allocates new ondisk blocks and inserts them into the wiptree marking them dirty.
         * If we use isDummy=true, then only the indirects are populated with '0' addr dbns
         * which technically works because we return a zero'd 8K block when trying to read DBN 0
         */
        public void redfs_resize_wip(int fsid, RedFS_Inode wip, long newsize, bool preAlloc)
        {
            lock (wip)
            {
                if (wip.get_filesize() <= newsize)
                {
                    bool dummy6 = ((newsize - wip.get_filesize()) > 256 * 1024) ? true : false;
                    bool quickgrow = (wip.get_filesize() == 0) ? true : false;
                    if (dummy6 && quickgrow && !preAlloc)
                    {
                        redfs_grow_wip_internal_superfast2(wip, newsize, fsid == 0);
                    }
                    else
                    {
                        if (wip.m_ino == 0 || wip.m_ino == 1)
                        {
                            redfs_grow_wip_internal_superfast2(wip, newsize, fsid == 0);
                        }
                        else
                        {
                            redfs_grow_wip_internal(fsid, wip, newsize, dummy6); //usually make dummy .xxx tofix 
                        }
                    }

                    wip._lasthitbuf = null; //we are growing wip so mark this null

                }
                else
                {
                    throw new SystemException("Not yet implimented!");
                    redfs_shrink_wip_internal(wip.get_filefsid(), wip, newsize);
                }

                //XXX todo : Dont have to write out just allocated L0 since they will be overwritten anyway.
                //Try to write out L1 & L2s instead
                if (newsize > 0)
                {
                    //sync(wip);
                    //flush_cache(wip, true);
                }
                DEFS.ASSERT(wip.get_filesize() == newsize, "File size should match after resize");
            }
        }

        //
        // Setting the reserve flag will increment refcount. otherwise it must be for
        // resize type calls where the new inode is generally discarded.
        // 
        private void redfs_create_wip_from_dbnlist(RedFS_Inode wip, long[] dbnlist, bool doreserve)
        {
            DEFS.ASSERT(doreserve == false, "Not implimented yet");
            wip._lasthitbuf = null;

            if (wip.get_inode_level() == 0)
            {
                for (int i = 0; i < dbnlist.Length; i++)
                {
                    wip.set_child_dbn(i, dbnlist[i]);
                }
            }
            else
            {
                DEFS.ASSERT(dbnlist.Length < (OPS.FS_SPAN_OUT * OPS.FS_SPAN_OUT * 16), "size > 128 Gig is not yet implimented!");
                int numl1 = OPS.NUML1((long)dbnlist.Length * OPS.FS_BLOCK_SIZE);
                int counter = 0;

                for (int i = 0; i < numl1; i++)
                {
                    RedBufL1 wbl1 = (RedBufL1)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L1, i * OPS.FS_SPAN_OUT, true, Array.Empty<long>(), 0);
                    wip.set_child_dbn(i, wbl1.m_dbn);
                    int itr = 0, numL0EntriesInL1 = 0, startPoint = counter;

                    while (counter < dbnlist.Length && itr < OPS.FS_SPAN_OUT)
                    {
                        wbl1.set_child_dbn(itr, dbnlist[counter]);
                        redfsBlockAllocator.mod_refcount(wip.get_filefsid(), dbnlist[counter], REFCNT_OP.INCREMENT_REFCOUNT_ALLOC, null, false);
                        counter++;
                        itr++;
                        numL0EntriesInL1++;
                    }

                    for (int inner =0; inner < numL0EntriesInL1; inner++)
                    {
                       DEFS.ASSERT( wbl1.get_child_dbn(inner) == dbnlist[startPoint + inner], "FAiled to create a new wip from dbn list correctly");
                    }
                }
            }
        }

        /*
         * Sometimes, ex. in test cases, we take a wip directly and so we have to discard it so
         * that its buffers are free and accounted for during shutdown
         */ 
        public void redfs_discard_wip(RedFS_Inode wip)
        {
            DEFS.ASSERT(wip.is_dirty == false, "Wip cannot be dirty in discard routine!");
            if (wip.L0list.Count > 0)
            {
                foreach (RedBufL0 rbl0 in wip.L0list)
                {
                    DEFS.ASSERT(rbl0.is_dirty == false, "wip cannot have dirty buffers in redfs_discard_wip");
                }
                mFreeBufCache.deallocateList(wip.L0list, "discardWip(L0):" + wip.get_ino());
            }
            wip._lasthitbuf = null;
        }

        // Generally external interface to delete a wip. the caller must ensure
        // that wip->size is set to zero with no pointers and this must be written
        // into the inofile.

        public void redfs_delete_wip(int fsid, RedFS_Inode inowip, RedFS_Inode wip, bool clearincorebufs)
        {
            RedFS_Inode wip4delq = new RedFS_Inode(wip.get_wiptype(), wip.get_ino(), 0);

            wip._lasthitbuf = null;
            for (int i = 0; i < OPS.WIP_SIZE; i++)
            {
                wip4delq.data[i] = wip.data[i];
            }
            if (clearincorebufs == false)
            {
                DEFS.ASSERT(wip.L0list.Count == 0, "ext del shouldve flushed cached, L0");
                DEFS.ASSERT(wip.L1list.Count == 0, "ext del shouldve flushed cached, L1");
                DEFS.ASSERT(wip.L2list.Count == 0, "ext del shouldve flushed cached, L2");
            }
            else
            {
                wip.sort_buflists();
                mFreeBufCache.deallocateList(wip.L0list, "delWip:" +wip.get_ino());

                for (int i = 0; i < wip.L1list.Count; i++)
                {
                    RedBufL1 wbl1 = (RedBufL1)wip.L1list[i];
                    if (wbl1.is_dirty)
                    {
                        //xx why write when we are in process ofdeleting
                        //mvdisk.write(wip, wbl1);
                    }
                }

                for (int i = 0; i < wip.L2list.Count; i++)
                {
                    RedBufL2 wbl2 = (RedBufL2)wip.L2list[i];
                    if (wbl2.is_dirty)
                    {
                        //xx why write when we are in process ofdeleting
                        //mvdisk.write(wip, wbl2);
                    }
                }
                wip.L1list.Clear();
                wip.L2list.Clear();
            }

            redfsBlockAllocator.Sync();
            wip4delq.setfilefsid_on_dirty(fsid); //this must be set or else the counters go wrong.
            
            GLOBALQ.m_wipdelete_queue.Add(wip4delq);

            if (inowip != null)
            {
                redfs_punch_wip_hole(inowip, wip, wip.get_ino());
            }

            /*
            for (int i = 0; i < OPS.NUM_PTRS_IN_WIP; i++) { wip.set_child_dbn(i, DBN.INVALID); }
            wip.is_dirty = false;
            wip.set_filesize(0);
            wip.set_ino(0, -1);
            */
        }

        private int redfs_get_file_dbns(RedFS_Inode wip, long[] dbns, int startfbn, int count)
        {
            DEFS.ASSERT(dbns.Length == count, "Please pass correct sizes");
            if (wip.get_inode_level() == 0)
            {
                DEFS.ASSERT(startfbn < 16 && (startfbn + count) < 16, "Out of range request in redfs_get_file_dbns");
                int c = 0;
                for (int i = startfbn; i < count; i++)
                {
                    dbns[c++] = wip.get_child_dbn(i);
                }
                DEFS.ASSERT(c == count, "Mismatch has occurred 1 :" + c + "," + count);
                return c;
            }
            else
            {

                // The idea is to bring everything incore.

                int l0cnt = OPS.NUML0(wip.get_filesize());
                DEFS.ASSERT((startfbn + count) <= l0cnt, "Out of range request in redfs_get_file_dbns 2");

                int numl1 = OPS.NUML1(wip.get_filesize());
                int c = 0;
                for (int i = 0; i < numl1; i++)
                {
                    long startfbn_l1 = OPS.PIDXToStartFBN(1, i);
                    RedBufL1 wbl1 = (RedBufL1)redfs_load_buf(wip, 1, startfbn_l1, false);
                    for (int i0 = 0; i0 < OPS.FS_SPAN_OUT; i0++)
                    {
                        long cfbn = startfbn_l1 + i0;
                        if (cfbn >= startfbn && cfbn < (startfbn + count))
                        {
                            dbns[c++] = wbl1.get_child_dbn(i0);
                        }
                    }
                }
                DEFS.ASSERT(c == count, "Mismatch has occurred 2 :" + c + "," + count);
                return c;
            }
        }

        public RedFS_Inode redfs_clone_wip(RedFS_Inode wip)
        {
            lock (wip)
            {
                return redfs_clone_file_wip(wip);
            }
        }

        /**************************************************  Private methods *************************************/

        //

        //Returns a dirty wip, which has basic pointers to L0/L1/L2 depending on the inode level and size information filled in
        //The caller is responsible for adding inode number and checking in the wip in the required inodeFile of some FSID.
        //Essentially this routine can be used for any wip for cross volume cloning.
        private RedFS_Inode redfs_clone_file_wip(RedFS_Inode wip)
        {
            RedFS_Inode dupwip = new RedFS_Inode(wip.get_wiptype(), -1, -1);

            //shouldnt have any data incore, caller must have cleaned the old file and sync'd the inode file.
            DEFS.ASSERT(wip.get_incore_cnt() == 0, "Must not have incore buffers");
            DEFS.ASSERT(wip.get_wiptype() == WIP_TYPE.REGULAR_FILE, "Cannot dup a non regular file");
            DEFS.ASSERT(wip.is_dirty == false, "Cannot be dirty, this must have been flushed to disk!");
            DEFS.ASSERT(dupwip.get_filesize() == 0, "Dup wip cannot be inited already");

            //do increment refcount for >= L1 files, and for L0 files, just direct increment.
            if (wip.get_inode_level() == 0)
            {
                int numL0 = OPS.NUML0(wip.get_filesize());
                for (int i = 0; i < numL0; i++)
                {
                    long dbn0 = wip.get_child_dbn(i);
                    //refcntmgr.increment_refcount_onalloc(wip.get_filefsid(), dbn0);
                    redfsBlockAllocator.increment_refcount_onalloc(wip.get_filefsid(), dbn0);
                    dupwip.set_child_dbn(i, dbn0);
                }
            }
            else if (wip.get_inode_level() == 1)
            {
                for (int i = 0; i < OPS.NUML1(wip.get_filesize()); i++)
                {
                    RedBufL1 wbl1 = (RedBufL1)redfs_load_buf(wip, 1, OPS.PIDXToStartFBN(1, i), false);

                    redfsBlockAllocator.touch_refcount(wip.get_filefsid(), wip.get_ino(), wbl1, false);
                    redfsBlockAllocator.increment_refcount(wip.get_filefsid(), wip.get_ino(), wbl1, false);

                    long dbn1 = wip.get_child_dbn(i);
                    dupwip.set_child_dbn(i, dbn1);
                }
            }
            else
            {
                int numl1s_remaining = OPS.NUML1(wip.get_filesize());

                for (int i2 = 0; i2 < OPS.NUML2(wip.get_filesize()); i2++)
                {
                    RedBufL2 wbl2 = (RedBufL2)redfs_load_buf(wip, 2, OPS.PIDXToStartFBN(2, i2), false);

                    redfsBlockAllocator.touch_refcount(wip.get_filefsid(), wip.get_ino(), wbl2, false);
                    redfsBlockAllocator.increment_refcount(wip.get_filefsid(), wip.get_ino(), wbl2, false);

                    long dbn2 = wip.get_child_dbn(i2);
                    dupwip.set_child_dbn(i2, dbn2);
                }
            }
            dupwip.is_dirty = true;
            dupwip.isWipValid = true;
            dupwip.set_filesize(wip.get_filesize());

            return dupwip;
        }

        //
        // Shrink wip internal logic, will involve freeing bufs, and resizing wip.
        // Can be used to delete wip (truncate to zero size). Windows API will callback
        // setfilesize once write is done.
        // 
        private void redfs_shrink_wip_internal(int callingfsid, RedFS_Inode wip, long newsize)
        {
            if (newsize == 0)
            {
                redfs_delete_wip(callingfsid, null, wip, true);
                wip.set_filesize(0);
                return;
            }

            int numl0new = OPS.NUML0(newsize);
            long[] dbns = new long[numl0new];

            RedFS_Inode newwip = new RedFS_Inode(wip.get_wiptype(), wip.get_ino(), wip.get_parent_ino());
            newwip.setfilefsid_on_dirty(wip.get_filefsid());
            newwip.set_filesize(newsize);

            redfs_get_file_dbns(wip, dbns, 0, numl0new);
            redfs_create_wip_from_dbnlist(newwip, dbns, false);
            newwip.set_filesize(newsize);
            sync(newwip);

            redfs_delete_wip(callingfsid, null, wip, true);
            
            //now update what the ccaller passed
            for (int i = 0; i < OPS.NUM_WIPS_IN_BLOCK; i++)
            {
                wip.data[i] = newwip.data[i];
            }
            
            wip.is_dirty = true;
            //wip.fbn_wise_fps.Clear();
            //wip.fingerprint = "";
            //wip.logs.Clear();
            
            DEFS.ASSERT(wip.get_filesize() == newsize, "could not shrink wip 2");
        }

        private bool HasChildIncoreOld(RedFS_Inode wip, int level, long sfbn)
        {
            DEFS.ASSERT(level > 0, "Incorrect level to HasChildIncore()");
            if (level == 1)
            {
                int count0 = wip.L0list.Count;
                int span1 = OPS.FS_SPAN_OUT;

                for (int i = 0; i < count0; i++)
                {
                    RedBufL0 wbl0 = (RedBufL0)wip.L0list[i];
                    if (wbl0.m_start_fbn >= sfbn && wbl0.m_start_fbn < (sfbn + span1))
                    {
                        return true;
                    }
                }
                return false;
            }
            else
            {
                int count1 = wip.L1list.Count;
                int span2 = OPS.FS_SPAN_OUT * OPS.FS_SPAN_OUT;

                for (int i = 0; i < count1; i++)
                {
                    RedBufL1 wbl1 = (RedBufL1)wip.L1list[i];
                    if (wbl1.m_start_fbn >= sfbn && wbl1.m_start_fbn < (sfbn + span2))
                    {
                        return true;
                    }
                }
                return false;
            }
        }

        private void do_fast_read(RedFS_Inode wip, long fbn, byte[] buffer, int offset)
        {
            RedBufL1 wbl1 = (RedBufL1)redfs_load_buf(wip, 1, fbn, false);
            int idx = OPS.myidx_in_myparent(0, fbn);
            long dbn0 = wbl1.get_child_dbn(idx);

            DEFS.ASSERT(dbn0 != DBN.INVALID, "cannot have invalid dbn during fast read");

            ReadPlanElement rpe = redfsBlockAllocator.PrepareReadPlanSingle(dbn0);
            redFSPersistantStorage.ExecuteReadPlanSingle(rpe, buffer, offset, OPS.FS_BLOCK_SIZE);

            wip.log("Fast Read " + dbn0 + "(buffer size,offset =" + buffer.Length + "," + offset + ")");
        }

        //does not work correctly yet, but very fast > 60 Mbps
        private void do_fast_write(RedFS_Inode wip, long fbn, byte[] buffer, int offset)
        {
            REDFSCoreSideMetrics.m.StartMetric(METRIC_NAME.FASTWRITE_LATENCY_MS, offset);

            DEFS.ASSERT(wip.spanType == SPAN_TYPE.MIRRORED || wip.spanType == SPAN_TYPE.DEFAULT, "Fast write does not work for raid5 stripes");

            REDFSCoreSideMetrics.m.StartMetric(METRIC_NAME.LOADBUF_LATENCY_MS, offset);
            RedBufL1 wbl1 = (RedBufL1)redfs_load_buf(wip, 1, fbn, true);
            REDFSCoreSideMetrics.m.StopMetric(METRIC_NAME.LOADBUF_LATENCY_MS, offset);

            //if (!wbl1.is_dirty &&  wbl1.needdbnreassignment && wbl1.m_dbn != 0 && wbl1.m_dbn != DBN.INVALID)
            //{
            //    redfsBlockAllocator.decrement_refcount_ondealloc(wip.get_filefsid(), wbl1.m_dbn);
            //}

            if (wbl1.m_dbn == 0)
            {
                wbl1.m_dbn = redfsBlockAllocator.allocateDBN(wip, wip.spanType);
                wbl1.is_dirty = true;
            }

            if (wip.get_inode_level() == 1)
            {
                int pidx = wbl1.myidx_in_myparent();
                wip.set_child_dbn(pidx, wbl1.m_dbn);
            }
            else if (wip.get_inode_level() == 2)
            {
                REDFSCoreSideMetrics.m.StartMetric(METRIC_NAME.LOADBUF_LATENCY_MS, offset);
                RedBufL2 wbl2 = (RedBufL2)redfs_load_buf(wip, 2, fbn, true);
                REDFSCoreSideMetrics.m.StopMetric(METRIC_NAME.LOADBUF_LATENCY_MS, offset);

                wbl2.set_child_dbn(OPS.myidx_in_myparent(1, fbn), wbl1.m_dbn);
            }

            int idx = OPS.myidx_in_myparent(0, fbn);
            long dbn0 = wbl1.get_child_dbn(idx);

            if (dbn0 != 0 && dbn0 != DBN.INVALID)
            {
                redfsBlockAllocator.decrement_refcount_ondealloc(wip.get_filefsid(), dbn0);
            }

            dbn0 = redfsBlockAllocator.allocateDBN(wip, wip.spanType);

            wbl1.set_child_dbn(idx, dbn0);

            WritePlanElement wpe = redfsBlockAllocator.PrepareWritePlanSingle(dbn0);
            redFSPersistantStorage.ExecuteWritePlanSingle(wpe, buffer, offset);

            wip.log("Fast Write " + dbn0 + "(buffer size,offset =" + buffer.Length + "," + offset + ")");

            REDFSCoreSideMetrics.m.StopMetric(METRIC_NAME.FASTWRITE_LATENCY_MS, offset);
        }

        private void redfs_reassign_new_dbn(RedFS_Inode wip, Red_Buffer wb)
        {
            bool isinofileL0 = (wb.get_level() == 0) && (wip.get_wiptype() == WIP_TYPE.PUBLIC_INODE_FILE);

            DEFS.ASSERT(wb.get_dbn_reassignment_flag() == true, "Wrong call");

            if (wb.get_ondisk_dbn() != 0 && wb.get_ondisk_dbn() != DBN.INVALID)
            {
                redfsBlockAllocator.decrement_refcount_ondealloc(wip.get_filefsid(), wb.get_ondisk_dbn());
            }
            long newdbn = redfsBlockAllocator.allocateDBN(wip, wip.spanType);

            DEFS.ASSERT(newdbn > 0, "new dbn must be greater than 0");
            DEFS.ASSERT(newdbn != wb.get_ondisk_dbn(), "We really have to alloc a new dbn");

            REDFS_BUFFER_ENCAPSULATED wbe = new REDFS_BUFFER_ENCAPSULATED(wb);
            wbe.set_dbn(newdbn);
            wb.set_dbn_reassignment_flag(false);

        }

        /*
         * wbl0 can be null in case of fast read/write case.
         */
        private void redfs_allocate_new_dbntree(RedFS_Inode wip, RedBufL0 wbl0, long givenfbn)
        {
            bool reassigndone = false;
            long start_fbn = (wbl0 == null) ? givenfbn : (int)wbl0.m_start_fbn;

            if (wip.get_inode_level() == 0)
            {
                if (wbl0 != null && wbl0.needdbnreassignment)
                {
                    redfs_reassign_new_dbn(wip, wbl0);
                    int pidx = wbl0.myidx_in_myparent();
                    wip.set_child_dbn(pidx, wbl0.m_dbn);
                    wip.is_dirty = true;
                    reassigndone = true;
                    wbl0.needdbnreassignment = false;
                }
            }
            else if (wip.get_inode_level() == 1)
            {
                RedBufL1 wbl1 = (RedBufL1)get_buf3("RE-DBN", wip, 1, start_fbn, false);
                if (wbl1.needdbnreassignment)
                {
                    redfs_reassign_new_dbn(wip, wbl1);
                    int pidx = wbl1.myidx_in_myparent();
                    wip.set_child_dbn(pidx, wbl1.m_dbn);
                    wip.is_dirty = true;
                    reassigndone = true;
                    wbl1.needdbnreassignment = false;
                }
                if (wbl0 != null && wbl0.needdbnreassignment)
                {
                    redfs_reassign_new_dbn(wip, wbl0);
                    int pidx = wbl0.myidx_in_myparent();
                    wbl1.set_child_dbn(pidx, wbl0.m_dbn);
                    wbl1.is_dirty = true;
                    reassigndone = true;
                    wbl0.needdbnreassignment = false;
                }
            }
            else if (wip.get_inode_level() == 2)
            {
                RedBufL1 wbl1 = (RedBufL1)get_buf3("RE-DBN", wip, 1, (int)start_fbn, false);
                RedBufL2 wbl2 = (RedBufL2)get_buf3("RE-DBN", wip, 2, (int)start_fbn, false);

                if (wbl2.needdbnreassignment)
                {
                    redfs_reassign_new_dbn(wip, wbl2);
                    int pidx = wbl2.myidx_in_myparent();
                    wip.set_child_dbn(pidx, wbl2.m_dbn);
                    wip.is_dirty = true;
                    reassigndone = true;
                    wbl2.needdbnreassignment = false;
                }
                if (wbl1.needdbnreassignment)
                {
                    redfs_reassign_new_dbn(wip, wbl1);
                    int pidx = wbl1.myidx_in_myparent();
                    wbl2.set_child_dbn(pidx, wbl1.m_dbn);
                    wbl2.is_dirty = true;
                    reassigndone = true;
                    wbl1.needdbnreassignment = false;
                }
                if (wbl0 != null && wbl0.needdbnreassignment)
                {
                    long old_dbn = wbl0.m_dbn;
                    redfs_reassign_new_dbn(wip, wbl0);
                    int pidx = wbl0.myidx_in_myparent();
                    wbl1.set_child_dbn(pidx, wbl0.m_dbn);
                    wbl1.is_dirty = true;
                    reassigndone = true;
                    wbl0.needdbnreassignment = false;
                    long new_dbn = wbl0.m_dbn;

                    if (wip.get_wiptype() == WIP_TYPE.PUBLIC_INODE_FILE)
                    {
                        wip.log("L0 " + wbl0.m_start_fbn + "  dbn: " + old_dbn + " ->" + new_dbn);
                    }
                }
            }
            else
            {
                DEFS.ASSERT(false, "wrong level");
            }
            if (reassigndone == true)
            {
                //DEFS.DEBUG("DBN-R", "redfs_allocate_new_dbntree, wip = " + wip.m_ino + " sfbn = " + wbl0.m_start_fbn);
                //redfs_show_vvbns(wip, false);
                wip.dbnTreeReassignmentCount++;
            }
        }

        /*
         * Called from redfs_read or redfs_write and with lock on wip
         */ 
        private int do_io_internal(REDFS_OP type, RedFS_Inode wip, long fileoffset, byte[] buffer, int boffset, int blength)
        {
            DEFS.ASSERT(blength <= buffer.Length && ((buffer.Length - boffset) >= blength),
                                    "Overflow detected in do_io_internal type = " + type);

            if ((wip.get_filesize() < (fileoffset + blength)))
            {
                if (type == REDFS_OP.REDFS_WRITE)
                {
                    //DEFS.ASSERT(wip.get_ino() == 0 || type == REDFS_OP.REDFS_WRITE, "Cannot grow wip for " +
                    //        "read operations, except for inofile. wip.ino = " + wip.get_ino() + " and type = " + type + " and size " +
                    //        wip.get_filesize() + " to " + (fileoffset + blength));
                    redfs_resize_wip(wip.get_filefsid(), wip, (fileoffset + blength), false);
                }
                else
                {
                    long eof_read_boundary = fileoffset + blength;
                    if ((eof_read_boundary - wip.get_filesize()) > 0) //not sure why 4k page is assumed
                    {
                        blength = (int)(wip.get_filesize() - fileoffset);
                    }
                }
                wip.iohistory = 0;
            }

            int buffer_start = boffset;
            int buffer_end = boffset + blength;

            while (buffer_start < buffer_end)
            {
                int wboffset = (int)(fileoffset % OPS.FS_BLOCK_SIZE);
                int copylength = ((OPS.FS_BLOCK_SIZE - wboffset) < (buffer_end - buffer_start)) ?
                                        (OPS.FS_BLOCK_SIZE - wboffset) : (buffer_end - buffer_start);

                // Do dummy read if the write is a full write 
                long fbn = OPS.OffsetToFBN(fileoffset);

                bool fullwrite = (copylength == OPS.FS_BLOCK_SIZE && type == REDFS_OP.REDFS_WRITE &&
                        (wip.get_wiptype() != WIP_TYPE.PUBLIC_INODE_FILE )) ? true : false;
                bool fullread = (copylength == OPS.FS_BLOCK_SIZE && type == REDFS_OP.REDFS_READ &&
                        wip.get_wiptype() != WIP_TYPE.PUBLIC_INODE_FILE) ? true : false;

                if (fullwrite && wip.get_inode_level() > 0 &&
                            (wip.iohistory == 0xFFFFFFFFFFFFFFFF || (wip.get_wiptype() == WIP_TYPE.DIRECTORY_FILE)) && //stop caching writes and go fast after 64 blocks.
                            (get_buf3("TEST", wip, 0, fbn, true) == null))
                {
                    do_fast_write(wip, fbn, buffer, buffer_start);

                    RedBufL1 wbl1_t = (RedBufL1)get_buf3("TEST", wip, 1, fbn, true);
                    DEFS.ASSERT(wbl1_t != null, "We just did fast write, so shouldnt be null");
                    DEFS.ASSERT(wbl1_t.get_child_dbn(OPS.myidx_in_myparent(0, fbn)) > 0, "dbn should have been updated in l1 in fast write");

                    buffer_start += copylength;
                    wboffset += copylength;
                    fileoffset += copylength;

                    //wip.fbn_wise_fps[fbn] = OPS.compute_hash_string(buffer, 0, OPS.FS_BLOCK_SIZE) + " @ " + wbl1_t.get_child_dbn(OPS.myidx_in_myparent(0, fbn));
                    if (wip.fbn_wise_fps.Contains(fbn))
                    {
                        wip.fbn_wise_fps.Remove(fbn);
                    }
                    continue;
                }
                else if (fullread && wip.get_inode_level() > 0 &&
                            (wip.iohistory == 0) &&
                            (wip.get_wiptype() != WIP_TYPE.PUBLIC_INODE_FILE) &&
                            (get_buf3("TEST", wip, 0, fbn, true) == null))
                {
                    do_fast_read(wip, fbn, buffer, buffer_start);
                    buffer_start += copylength;
                    wboffset += copylength;
                    fileoffset += copylength;
                    continue;
                }

                DEFS.ASSERT(OPS.NUML0(wip.get_filesize()) > fbn, "we cannot load buf beyond eof!");

                if (wip.get_ino() == 65 && fbn == 0 && type == REDFS_OP.REDFS_READ && copylength == 512 && wboffset == 0 &&
                        wip.fbn_wise_fps.Contains(fbn) && (get_buf3("TEST", wip, 0, fbn, true) == null))
                {
                    Console.WriteLine("Break here!");
                }

                bool loadBufFlag = (wip.get_wiptype() == WIP_TYPE.PUBLIC_INODE_FILE) ? false : (type == REDFS_OP.REDFS_WRITE && copylength == OPS.FS_BLOCK_SIZE);
                RedBufL0 wbl0 = (RedBufL0)redfs_load_buf(wip, 0, fbn, loadBufFlag);

                /*
                 * After load buf we should have all the wip->L2 (optional)->L1 (optional)->L0 in memory.
                 * XXX add the check here. TODO
                 */

                if (type == REDFS_OP.REDFS_READ)
                {
                    //Lets verify if the block hash matchs if we had writtin out inthis sessions.
                    if (wip.fbn_wise_fps.Contains(fbn))
                    {
                        wbl0.update_fingerprint();
                        //DEFS.ASSERT((string)wip.fbn_wise_fps[fbn] == (wbl0.fingerprint + " @ " + wbl0.m_dbn), "Fingerprints for wip fbns should match after write and then read!");
                    }
                }

                if (type == REDFS_OP.REDFS_WRITE)
                {
                    byte[] original = new byte[OPS.FS_BLOCK_SIZE];
                    if (wip.get_wiptype() == WIP_TYPE.PUBLIC_INODE_FILE)
                    {
                        DEFS.ASSERT(copylength == OPS.WIP_SIZE, "write should be 256 bytes for inode file");
                        Array.Copy(wbl0.data, original, OPS.FS_BLOCK_SIZE);
                    }

                    Array.Copy(buffer, buffer_start, wbl0.data, wboffset, copylength); 
                    buffer_start += copylength;
                    wboffset += copylength;
                    wbl0.is_dirty = true;
                    wbl0.update_fingerprint();

                    //XXX fast overwrites with low ttl for inode file is creating problems when writing out large amount of data
                    //so keep the L0s more in memory for a longer time.
                    wbl0.mTimeToLive = (wip.get_wiptype() == WIP_TYPE.PUBLIC_INODE_FILE)? 20 : 2;

                    redfs_allocate_new_dbntree(wip, wbl0, -1);

                    if (wip.get_inode_level() > 0)
                    {
                        RedBufL1 wbl1_t = (RedBufL1)get_buf3("testing", wip, 1, wbl0.m_start_fbn, true);
                        int idx_t = wbl0.myidx_in_myparent();
                        DEFS.ASSERT(wbl1_t.get_child_dbn(idx_t) == wbl0.m_dbn, "Dbn in L1 and L0 should correspond after allocate_new_dbn_tree, " + wbl1_t.get_child_dbn(idx_t) + " vs " + wbl0.m_dbn);

                        //Additional check, if its directory, verify that all dbns in the L1 till now are valid and fine
                        if (wip.get_wiptype() == WIP_TYPE.DIRECTORY_FILE)
                        {
                            for (int id_t = 0; id_t < idx_t; id_t++)
                            {
                                DEFS.ASSERT(wbl1_t.get_child_dbn(id_t) > 0, "For dir file, all buffs till now should be valid and marked in wb_L1");
                            }
                        }
                    }
                    wip.iohistory = (wip.iohistory << 1) | 0x0000000000000001;

                    if (wip.get_wiptype() == WIP_TYPE.PUBLIC_INODE_FILE)
                    {
                        byte[] final = new byte[OPS.FS_BLOCK_SIZE];
                        Array.Copy(wbl0.data, final, OPS.FS_BLOCK_SIZE);

                        int diffbytes = 0; 
                        for (int i=0; i<OPS.FS_BLOCK_SIZE;i++)
                        {
                            if (original[i] != final[i]) diffbytes++;
                        }
                        DEFS.ASSERT(diffbytes <= OPS.WIP_SIZE, "the write should've been only 256 bytes");
                    }
                }
                else
                {
                    Array.Copy(wbl0.data, wboffset, buffer, buffer_start, copylength);
                    buffer_start += copylength;
                    wboffset += copylength;
                    //wbl0.mTimeToLive += (wbl0.mTimeToLive < 4) ? 1 : 0;
                    wbl0.mTimeToLive = (wip.get_wiptype() == WIP_TYPE.PUBLIC_INODE_FILE) ? 10 : 2;
                    wip.iohistory = (wip.iohistory << 1);
                }

                DEFS.ASSERT(wboffset <= OPS.FS_BLOCK_SIZE && buffer_start <= (boffset + blength), "Incorrect computation in redfs_io " + type);
                fileoffset += copylength;
            }


            if (type == REDFS_OP.REDFS_WRITE && wip.get_incore_cnt() > 1024 && wip.get_wiptype() != WIP_TYPE.PUBLIC_INODE_FILE)
            {
                sync(wip);
                flush_cache(wip, false);
            }
            if (wip.get_wiptype() == WIP_TYPE.PUBLIC_INODE_FILE)
            {
                // DEFS.DEBUG("WOPINOFILE", type + "(ino, fileoffset, length) = (" + wip.m_ino + "," + fileoffset + "," + blength + ")");
                //This sync also gave me lots of fucking problems. so be careful to commit inowip regularly
                //enabled on nov19, just to test.
                //sync(wip);
            }

            return blength;
        }


        public int redfs_write(RedFS_Inode wip, long fileoffset, byte[] buffer, int boffset, int blength, WRITE_TYPE type)
        {
            lock (wip)
            {
                if (type == WRITE_TYPE.OVERWRITE_IN_PLACE)
                {
                    return do_io_internal(REDFS_OP.REDFS_WRITE, wip, fileoffset, buffer, boffset, blength);
                }
                else if (type == WRITE_TYPE.TRUNCATE_AND_OVERWRITE)
                {
                    DEFS.ASSERT(wip.get_wiptype() != WIP_TYPE.PUBLIC_INODE_FILE &&
                        wip.get_wiptype() != WIP_TYPE.PUBLIC_INODE_MAP, "We cannot truncate and write inode or imap file");
                    redfs_shrink_wip_internal(wip.get_filefsid(), wip, 0);
                    DEFS.ASSERT(wip.get_filesize() == 0, "File should be truncated by now!");
                    int returnvalue = do_io_internal(REDFS_OP.REDFS_WRITE, wip, fileoffset, buffer, boffset, blength);

                    //Verify
                    return returnvalue;
                }
                else
                {
                    DEFS.ASSERT(false, "Unknown type passed, cannot handle it.");
                    return -1;
                }
            }
        }

        public int redfs_read(RedFS_Inode wip, long fileoffset, byte[] buffer, int boffset, int blength)
        {
            lock (wip)
            {
                return do_io_internal(REDFS_OP.REDFS_READ, wip, fileoffset, buffer, boffset, blength);
            }
        }

        public void redfs_do_raw_read_block(long dbn, byte[] buffer, int offset)
        {
            byte[] blockbuff = new byte[OPS.FS_BLOCK_SIZE];

            ReadPlanElement rpe = redfsBlockAllocator.PrepareReadPlanSingle(dbn);
            redFSPersistantStorage.ExecuteReadPlanSingle(rpe, blockbuff, 0, OPS.FS_BLOCK_SIZE);

            for (int i=0;i<OPS.FS_BLOCK_SIZE;i++)
            {
                buffer[offset + i] = blockbuff[i];
            }
        }

        public void redfs_do_raw_write_block(long dbn, byte[] buffer, int offset)
        {
            WritePlanElement wpe = redfsBlockAllocator.PrepareWritePlanSingle(dbn);
            redFSPersistantStorage.ExecuteWritePlanSingle(wpe, buffer, offset);
        }

        public void sync(RedFS_Inode wip)
        {
            //
            // First clean all the L0's, when cleaning, note that the entries in the L1 are matching!
            // Repeat the same for all the levels starting from down onwards.
            //
            lock (wip)
            {
                wip.sort_buflists();

                for (int i = 0; i < wip.L0list.Count; i++)
                {
                    RedBufL0 wbl0 = (RedBufL0)wip.L0list[i]; //too freaky bug, shouldnt be 0 but i.

                    int myidxinpt = wbl0.myidx_in_myparent();
                    if (wip.get_inode_level() == 0)
                    {
                        DEFS.ASSERT(wip.get_child_dbn(myidxinpt) == wbl0.m_dbn, "mismatch during sync, " +
                            " wbl1(" + myidxinpt + ")=" + wip.get_child_dbn(myidxinpt) + " and wbl0.m_dbn = " +
                            wbl0.m_dbn + " in wip = " + wip.get_ino());
                    }
                    else
                    {
                        RedBufL1 wbl1 = (RedBufL1)get_buf3("SYNC", wip, 1, (int)wbl0.m_start_fbn, false);
                        DEFS.ASSERT(wbl1.get_child_dbn(myidxinpt) == wbl0.m_dbn, "mismatch during sync, " +
                            " wbl1(" + myidxinpt + ")=" + wbl1.get_child_dbn(myidxinpt) + " and wbl0.m_dbn = " +
                            wbl0.m_dbn + " in wip = " + wip.get_ino());
                    }

                    if (wbl0.is_dirty)
                    {
                        WritePlanElement wpe = redfsBlockAllocator.PrepareWritePlanSingle(wbl0.m_dbn);
                        redFSPersistantStorage.ExecuteWritePlanSingle(wpe, wip, wbl0);
                        if (wip.get_ino() == 65 && wbl0.m_start_fbn == 0)
                        {
                            Console.WriteLine("Break here!");
                        }
                        wbl0.update_fingerprint();
                        wip.fbn_wise_fps[wbl0.m_start_fbn] = wbl0.fingerprint + " @ " + wbl0.m_dbn;
                    }

                    if (wip.get_wiptype() == WIP_TYPE.DIRECTORY_FILE || wbl0.isTimetoClear())
                    {
                        wip.L0list.RemoveAt(i);
                        mFreeBufCache.deallocate4(wbl0, "dirL0/TTL Over :" + wip.get_ino());
                        i--;
                    }
                }

                for (int i = 0; i < wip.L1list.Count; i++)
                {
                    RedBufL1 wbl1 = (RedBufL1)wip.L1list[i];


                    int myidxinpt = wbl1.myidx_in_myparent();
                    if (wip.get_inode_level() == 1)
                    {
                        DEFS.ASSERT(wip.get_child_dbn(myidxinpt) == wbl1.m_dbn, "mismatch during sync, " +
                            " wbl1(" + myidxinpt + ")=" + wip.get_child_dbn(myidxinpt) + " and wbl1.m_dbn = " +
                            wbl1.m_dbn + " in wip = " + wip.get_ino());
                    }
                    else if (wip.get_inode_level() == 2)
                    {
                        RedBufL2 wbl2 = (RedBufL2)get_buf3("SYNC", wip, 2, wbl1.m_start_fbn, false);
                        DEFS.ASSERT(wbl2.get_child_dbn(myidxinpt) == wbl1.m_dbn, "mismatch during sync, " +
                            " wbl2(" + myidxinpt + ")=" + wbl2.get_child_dbn(myidxinpt) + " and wbl1.m_dbn = " +
                            wbl1.m_dbn + " in wip = " + wip.get_ino());
                    }
                    else
                    {
                        DEFS.ASSERT(false, "how the hell do we have l1 bufs in L0 wip?");
                    }

                    if (wbl1.is_dirty)
                    {
                        //mvdisk.write(wip, wbl1);
                        WritePlanElement wpe = redfsBlockAllocator.PrepareWritePlanSingle(wbl1.m_dbn);
                        redFSPersistantStorage.ExecuteWritePlanSingle(wpe, wip, wbl1);
                    }
                    if (/*wip.get_wiptype() == WIP_TYPE.DIRECTORY_FILE ||*/
                                (wbl1.isTimetoClear() && !HasChildIncoreOld(wip, 1, wbl1.m_start_fbn)))
                    {

                        wip.L1list.RemoveAt(i);
                        i--;
                    }
                }

                for (int i = 0; i < wip.L2list.Count; i++)
                {
                    RedBufL2 wbl2 = (RedBufL2)wip.L2list[i];

                    int myidxinpt = wbl2.myidx_in_myparent();
                    if (wip.get_inode_level() == 2)
                    {
                        DEFS.ASSERT(wip.get_child_dbn(myidxinpt) == wbl2.m_dbn, "mismatch during sync, " +
                            " wbl1(" + myidxinpt + ")=" + wip.get_child_dbn(myidxinpt) + " and wbl1.m_dbn = " +
                            wbl2.m_dbn + " in wip = " + wip.get_ino());
                    }
                    else
                    {
                        DEFS.ASSERT(false, "how the hell do we have l1 bufs in L0 wip?");
                    }

                    if (wbl2.is_dirty)
                    {
                        //mvdisk.write(wip, wbl2);
                        WritePlanElement wpe = redfsBlockAllocator.PrepareWritePlanSingle(wbl2.m_dbn);
                        redFSPersistantStorage.ExecuteWritePlanSingle(wpe, wip, wbl2);
                    }
                    if (/*wip.get_wiptype() == WIP_TYPE.DIRECTORY_FILE ||*/
                        (wbl2.isTimetoClear() && !HasChildIncoreOld(wip, 2, wbl2.m_start_fbn)))
                    {
                        wip.L2list.RemoveAt(i);
                        i--;
                    }
                }
            }
            /*
             * Better sync for every inode sync, so that when shutting down, inowip is also
             * accounted for.
             */
            redfsBlockAllocator.Sync();
            wip.is_dirty = false;
            wip.iohistory = 0;
        }

        private void redfs_grow_wip_internal_superfast2(RedFS_Inode wip, long newsize, bool dontEvenAllocIndirects)
        {
            DEFS.ASSERT(newsize <= ((long)1024 * 1024 * 1024 * 128), "Max size for inode exceeded");
            DEFS.ASSERT(OPS.FSIZETOILEVEL(newsize) >= 1, "Cannot use fast grow for small files");

            int count = (OPS.FSIZETOILEVEL(newsize) == 1) ? OPS.NUML1(newsize) : OPS.NUML2(newsize);
            for (int i = 0; i < count; i++)
            {
                if (OPS.FSIZETOILEVEL(newsize) == 1)
                {
                    if (dontEvenAllocIndirects)
                    {
                        wip.set_child_dbn(i, 0);
                    }
                    else
                    {
                        int numL0sRemaining = (OPS.NUML0(newsize) - (OPS.FS_SPAN_OUT * i));
                        int numL0 = (numL0sRemaining > OPS.FS_SPAN_OUT) ? OPS.FS_SPAN_OUT : numL0sRemaining;

                        RedBufL1 wbl1 = (RedBufL1)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L1, i * OPS.FS_SPAN_OUT, false, Array.Empty<long>(), 0);
                        for (int k=0;k<numL0;k++)
                        {
                            wbl1.set_child_dbn(k, 0);
                        }
                        wip.set_child_dbn(i, wbl1.m_dbn);
                        wip.insert_buffer(mFreeBufCache, 1, wbl1);
                        wbl1.is_dirty = true;
                    }
                } 
                else
                {
                    if (dontEvenAllocIndirects)
                    {
                        wip.set_child_dbn(i, 0);
                    }
                    else
                    {
                        int numL1sRemaining = (OPS.NUML1(newsize) - (OPS.FS_SPAN_OUT * i));
                        int numL1 = (numL1sRemaining > OPS.FS_SPAN_OUT) ? OPS.FS_SPAN_OUT : numL1sRemaining;

                        RedBufL2 wbl2 = (RedBufL2)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L2, i * OPS.FS_SPAN_OUT * OPS.FS_SPAN_OUT, false, Array.Empty<long>(), 0);
                        
                        for (int k=0;k<numL1;k++)
                        {
                            wbl2.set_child_dbn(k, 0);
                        }
                        wip.set_child_dbn(i, wbl2.m_dbn);
                        wip.L2list.Add(wbl2);
                        wbl2.is_dirty = true;
                    }
                }
            }
            wip.set_filesize(newsize);
            wip.is_dirty = true;
        }

        /*
         * This can never return null normally, the caller *must* know that this buffer
         * is incore before calling. Must be called with a lock held on wip. But in case
         * is query is set true, then the caller is not sure if the buf is incore, in that
         * case we can return null safely.
         */
        private static Red_Buffer get_buf3(string who, RedFS_Inode wip, int level, long some_fbn, bool isquery)
        {
            List<Red_Buffer> list = null;

            switch (level)
            {
                case 0:
                    list = wip.L0list;
                    break;
                case 1:
                    list = wip.L1list;
                    break;
                case 2:
                    list = wip.L2list;
                    break;
            }
            DEFS.ASSERT(list != null, "List cannot be null in get_buf()");

            long start_fbn = OPS.SomeFBNToStartFBN(level, some_fbn);

            //some optimization, 10-12 mbps more.
            if (level == 1)
            {
                if (wip._lasthitbuf != null && wip._lasthitbuf.get_start_fbn() == start_fbn)
                {
                    //This must be touched so that its in memory and not GC'd
                    return wip._lasthitbuf;
                }
            }

            for (int idx = 0; idx < (list.Count); idx++)
            {
                int idx2 = (level == 0) ? (list.Count - idx - 1) : idx;
                Red_Buffer wb = (Red_Buffer)list[idx2];
                if (wb.get_start_fbn() == start_fbn)
                {
                    if (level == 1)
                    {
                        wip._lasthitbuf = wb;
                    }
                    else if (level == 0)
                    {
                        //we expect the :1 of this to be present in memory, but not updated correctly when dirty
                        //RedBufL1 wbl1 = (RedBufL1)get_buf3(who, wip, 1, start_fbn, isquery);
                        //int idx_t = ((RedBufL0)wb).myidx_in_myparent();
                        //DEFS.ASSERT(wbl1.get_child_dbn(idx_t) == wb.get_start_fbn(), "L1 should have the dbn of L0 correctly!");
                    }

                    return wb;
                }
            }

            DEFS.ASSERT(isquery, "who = " + who + ", get_buf() failed " + wip.get_ino() + "," + level + "," + some_fbn);
            return null;
        }


        private void redfs_grow_wip_internal(int fsid, RedFS_Inode wip, long newsize, bool dummy)
        {
            SPAN_TYPE spanType = wip.spanType;

            DEFS.ASSERT(newsize <= ((long)1024 * 1024 * 1024 * 128), "Max size for inode exceeded");

            //First lets figure out, how many blocks are required to fulfill this growth. if dummy then we dont
            //need to allocate L0's, so we just need dbns for L1's and L2's
            int numNewBlocksRequired = (!dummy) ? (OPS.NUML0(newsize) - OPS.NUML0(wip.get_filesize()) +
                                        OPS.NUML1(newsize) - OPS.NUML1(wip.get_filesize()) +
                                        OPS.NUML1(newsize) - OPS.NUML1(wip.get_filesize())) :
                                        (OPS.NUML1(newsize) - OPS.NUML1(wip.get_filesize()) +
                                        OPS.NUML1(newsize) - OPS.NUML1(wip.get_filesize()));
            int numBlocksUsedTillNow = 0;

            long[]  preAllocDbns = Array.Empty<long>();
            int     arrayIdx = 0;

            if (wip.spanType == SPAN_TYPE.RAID5)
            {
                int roundOffValue = (int)((numNewBlocksRequired % 4 == 0) ? numNewBlocksRequired : numNewBlocksRequired + 1);
                preAllocDbns = redfsBlockAllocator.allocateDBNSMultipleOfFour(fsid, SPAN_TYPE.RAID5, roundOffValue);
            } 

            while (true)
            {
                int currL0sincore = wip.L0list.Count;
                long nextstepsize = OPS.NEXT8KBOUNDARY(wip.get_filesize(), newsize);

                if (wip.spanType != SPAN_TYPE.RAID5 && (numNewBlocksRequired - numBlocksUsedTillNow) > OPS.NUM_DBNS_IN_1GB && 
                    arrayIdx == preAllocDbns.Length)
                {
                    preAllocDbns = redfsBlockAllocator.allocateDBNS(fsid, spanType, 0, OPS.NUM_DBNS_IN_1GB);
                    arrayIdx = 0;
                    numBlocksUsedTillNow += preAllocDbns.Length;
                }

                // We are done since, newsize is reached, so we can return.
                if (wip.get_filesize() == newsize)
                {
                    //We may not have alloc multiple of 4, so we still have some dbns which are not used. Make them zero'd dbns and mark them for freeing.
                    //It is possible to have a RAID5 stripe with differnt refcount numbers and even block with zero refcount. Zero refcount block is a zero
                    //block which act like padding. This can be recovered by a background scanner.
                    while (arrayIdx < preAllocDbns.Length)
                    {
                        RedBufL0 r = new RedBufL0(0);
                        r.m_dbn = preAllocDbns[arrayIdx++];
                        wip.ZerodBlocks.Add(r);
                    }
                    return;
                }


                // Load the indirects at the end of the filetree

                long xsfbn2 = OPS.OffsetToStartFBN(2, wip.get_filesize() - 1);
                long xsfbn1 = OPS.OffsetToStartFBN(1, wip.get_filesize() - 1);

                if (wip.get_inode_level() == 2)
                {
                    redfs_load_buf(wip, 2, xsfbn2, false);
                    redfs_load_buf(wip, 1, xsfbn1, false);
                }
                else if (wip.get_inode_level() == 1)
                {
                    redfs_load_buf(wip, 1, xsfbn1, false);
                }


                // Cover to the first 4k boundary, where there is no need to add any new L0.

                if (OPS.NUML0(wip.get_filesize()) == OPS.NUML0(nextstepsize))
                {
                    wip.set_filesize(nextstepsize);
                    continue;
                }

                long new_L0fbn = OPS.OffsetToFBN(nextstepsize - 1);

                if (wip.get_inode_level() == 0)
                {
                    bool growlevel = (OPS.FSIZETOILEVEL(nextstepsize) == 1) ? true : false;
                    RedBufL0 wbl0 = null;
                    long wbl0dbn = 0;

                    if (dummy == false)
                    {
                        wbl0 = (RedBufL0)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L0, new_L0fbn, false, preAllocDbns, ++arrayIdx);
                        wbl0dbn = wbl0.m_dbn;

                        wip.insert_buffer(mFreeBufCache, 0, wbl0);
                    }

                    if (!growlevel)
                    {
                        DEFS.ASSERT(new_L0fbn < 16, "Wrong computation for fbn 1, " + new_L0fbn + " fsiz :" + wip.get_filesize());
                        wip.set_child_dbn((int)new_L0fbn, wbl0dbn);
                    }
                    else if (growlevel)
                    {
                        redfs_levelincr_regularfile_wip(wip);
                        RedBufL1 wbl1 = (RedBufL1)get_buf3("WGI", wip, 1, new_L0fbn, false);
                        DEFS.ASSERT(wbl1 != null, "This should never fail, since the level was grown just now");
                        wbl1.set_child_dbn(16, wbl0dbn);
                    }

                    wip.set_filesize(nextstepsize);
                    wip.is_dirty = true;
                }
                else if (wip.get_inode_level() == 1)
                {
                    bool growlevel = (OPS.FSIZETOILEVEL(nextstepsize) == 2) ? true : false;

                    RedBufL0 wbl0 = null;

                    if (dummy == false)
                    {
                        wbl0 = (RedBufL0)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L0, new_L0fbn, false, preAllocDbns, ++arrayIdx);
                    }

                    if (!growlevel)
                    {
                        long lastfbn = OPS.OffsetToFBN(wip.get_filesize() - 1);
                        DEFS.ASSERT(new_L0fbn == (lastfbn + 1), "nextfbn must be 1 greater than the previous fbn " + new_L0fbn + " > " + lastfbn +
                                    " nextstepsize " + nextstepsize);

                        long sfbn1 = OPS.OffsetToStartFBN(1, nextstepsize - 1);

                        RedBufL1 wbl1 = null;
                        if (new_L0fbn % 1024 == 0)
                        {
                            wbl1 = (RedBufL1)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L1, sfbn1, false, preAllocDbns, ++arrayIdx);
                            int idx = wbl1.myidx_in_myparent();
                            wip.set_child_dbn(idx, wbl1.m_dbn);

                            wip.insert_buffer(mFreeBufCache, 1, wbl1);
                        }
                        else
                        {
                            wbl1 = (RedBufL1)redfs_load_buf(wip, 1, sfbn1, false);
                        }

                        if (dummy == false)
                        {
                            int xidx = wbl0.myidx_in_myparent();
                            wbl1.set_child_dbn(xidx, wbl0.m_dbn);

                            wip.insert_buffer(mFreeBufCache, 0, wbl0);
                        }
                        else
                        {
                            wbl1.set_child_dbn(OPS.myidx_in_myparent(0, new_L0fbn), 0);
                        }

                        wip.is_dirty = true;
                    }
                    else if (growlevel)
                    {
                        redfs_levelincr_regularfile_wip(wip);

                        RedBufL2 wbl2 = (RedBufL2)get_buf3("WGI", wip, 2, 0, false);
                        DEFS.ASSERT(wbl2 != null, "This can never be null since level was just grown 2");

                        RedBufL1 wbl1 = (RedBufL1)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L1, 16384, false, preAllocDbns, ++arrayIdx);
                        wbl2.set_child_dbn(16, wbl1.m_dbn);

                        wip.insert_buffer(mFreeBufCache, 1, wbl1);
                        //wip.L1list.Add(wbl1);

                        DEFS.ASSERT(new_L0fbn == 16384, "Incorrect fbn in evaluation");

                        if (dummy == false)
                        {
                            wbl1.set_child_dbn(0, wbl0.m_dbn);

                            wip.insert_buffer(mFreeBufCache, 0, wbl0);
                            //wip.L0list.Add(wbl0);
                        }
                        else
                        {
                            wbl1.set_child_dbn(0, 0);
                        }
                    }
                    wip.set_filesize(nextstepsize);
                }
                else if (wip.get_inode_level() == 2)
                {
                    RedBufL2 wbl2 = null;
                    bool needL2 = (OPS.NUML2(wip.get_filesize()) < OPS.NUML2(nextstepsize)) ? true : false;
                    long sfbn2 = OPS.OffsetToStartFBN(2, nextstepsize - 1);

                    if (needL2)
                    {
                        wbl2 = (RedBufL2)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L2, sfbn2, false, preAllocDbns, ++arrayIdx);

                        wip.insert_buffer(mFreeBufCache, 2, wbl2);
                        //wip.L2list.Add(wbl2);

                        int idx = wbl2.myidx_in_myparent();
                        wip.set_child_dbn(idx, wbl2.m_dbn);
                    }
                    else
                    {
                        wbl2 = (RedBufL2)redfs_load_buf(wip, 2, sfbn2, false);
                    }

                    bool needL1 = (OPS.NUML1(wip.get_filesize()) < OPS.NUML1(nextstepsize)) ? true : false;
                    RedBufL1 wbl1 = null;
                    long sfbn1 = OPS.OffsetToStartFBN(1, nextstepsize - 1);

                    if (needL1)
                    {
                        wbl1 = (RedBufL1)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L1,
                            sfbn1, false, preAllocDbns, ++arrayIdx);
                        int idx = wbl1.myidx_in_myparent();
                        wbl2.set_child_dbn(idx, wbl1.m_dbn);

                        //wip.L1list.Add(wbl1);
                        wip.insert_buffer(mFreeBufCache, 1, wbl1);
                    }
                    else
                    {
                        wbl1 = (RedBufL1)redfs_load_buf(wip, 1, sfbn1, false);
                    }

                    if (dummy == false)
                    {
                        //No need to allocate actual block for grow, we can load when required.
                        long L0dbn = redfsBlockAllocator.allocateDBN(wip, spanType);

                        int last_slot = OPS.myidx_in_myparent(0, new_L0fbn);
                        wbl1.set_child_dbn(last_slot, L0dbn);
                    }
                    else
                    {
                        wbl1.set_child_dbn(OPS.myidx_in_myparent(0, new_L0fbn), 0);
                    }
                    wip.set_filesize(nextstepsize);
                }

                DEFS.ASSERT(wip.L0list.Count <= (currL0sincore + 1), "L0's added more than one in loop " +
                        wip.L0list.Count + ", old = " + currL0sincore);
            }
        }

        //
        // First check incore, if not load and return.
        // Adjust refcounts accordingly. 
        //
        private Red_Buffer redfs_load_buf(RedFS_Inode wip, int level, long someL0fbn, bool forwrite)
        {
            Red_Buffer ret = (Red_Buffer)get_buf3("WLB", wip, level, someL0fbn, true);
            
            if (ret != null)
            {
                ret.touch();
                return ret;
            }

            if (level == 2)
            {
                if (wip.get_inode_level() == 2)
                {
                    long start_fbnl2 = OPS.SomeFBNToStartFBN(2, someL0fbn);

                    int l2_idx_wip = (int)(someL0fbn / (OPS.FS_SPAN_OUT * OPS.FS_SPAN_OUT));
                    long dbnl2 = wip.get_child_dbn(l2_idx_wip);

                    DEFS.ASSERT(dbnl2 != DBN.INVALID, "Invalid dbn found in a valid portion 1, fsize = " + wip.get_filesize());

                    RedBufL2 wbl2 = (RedBufL2)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L2, start_fbnl2, true, Array.Empty<long>(), 0);
                    wbl2.m_dbn = dbnl2;

                    ReadPlanElement  rpe = redfsBlockAllocator.PrepareReadPlanSingle(dbnl2);
                    redFSPersistantStorage.ExecuteReadPlanSingle(rpe, wbl2);

                    wip.insert_buffer(mFreeBufCache, 2, wbl2);

                    DEFS.ASSERT(wbl2.needtouchbuf, "This cannot be cleared out! 1");
                    DEFS.ASSERT(wbl2.needdbnreassignment, "we also need to reassign this dbn in case of overwrite 3");

                    redfsBlockAllocator.touch_refcount(wip.get_filefsid(), wip.get_ino(), wbl2, false);
                    return wbl2;
                }
                else
                {
                    DEFS.ASSERT(false, "Cannot load an l2 buf from a level " + wip.get_inode_level() + " file");
                }
            }
            else if (level == 1)
            {
                if (wip.get_inode_level() == 2)
                {
                    long start_fbnl1 = OPS.SomeFBNToStartFBN(1, someL0fbn);
                    RedBufL2 wbl2 = (RedBufL2)redfs_load_buf(wip, 2, someL0fbn, forwrite);

                    RedBufL1 wbl1 = (RedBufL1)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L1, start_fbnl1, true, Array.Empty<long>(), 0);

                    int idx = wbl1.myidx_in_myparent();
                    long dbnl1 = wbl2.get_child_dbn(idx);

                    DEFS.ASSERT(dbnl1 != DBN.INVALID, "Invalid dbn found in a valid portion 2, fsize = " + wip.get_filesize());
                    DEFS.ASSERT(wbl1.needtouchbuf, "This cannot be cleared out! 2");
                    DEFS.ASSERT(wbl1.needdbnreassignment, "we also need to reassign this dbn in case of overwrite 2");

                    wbl1.m_dbn = dbnl1;

                    ReadPlanElement rpe = redfsBlockAllocator.PrepareReadPlanSingle(dbnl1);
                    redFSPersistantStorage.ExecuteReadPlanSingle(rpe, wbl1);

                    wip.insert_buffer(mFreeBufCache, 1, wbl1);

                    redfsBlockAllocator.touch_refcount(wip.get_filefsid(), wip.get_ino(), wbl1, false);
                    return wbl1;
                }
                else if (wip.get_inode_level() == 1)
                {
                    long start_fbnl1 = OPS.SomeFBNToStartFBN(1, someL0fbn);

                    int idx = (int)(someL0fbn / OPS.FS_SPAN_OUT);
                    long dbnl1 = wip.get_child_dbn(idx);
                    DEFS.ASSERT(dbnl1 != DBN.INVALID, "Invalid dbn found in a valid portion 3, fsize = " + wip.get_filesize());
                    RedBufL1 wbl1 = (RedBufL1)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L1, start_fbnl1, true, Array.Empty<long>(), 0);
                    wbl1.m_dbn = dbnl1;

                    ReadPlanElement rpe = redfsBlockAllocator.PrepareReadPlanSingle(dbnl1);
                    redFSPersistantStorage.ExecuteReadPlanSingle(rpe, wbl1);

                    wip.insert_buffer(mFreeBufCache, 1, wbl1);

                    DEFS.ASSERT(wbl1.needdbnreassignment, "we also need to reassign this dbn in case of overwrite 1");
                    DEFS.ASSERT(wbl1.needtouchbuf, "This cannot be cleared out! 3");

                    redfsBlockAllocator.touch_refcount(wip.get_filefsid(), wip.get_ino(), wbl1, false); 
                    return wbl1;
                }
                else
                {
                    DEFS.ASSERT(false, "Cannot load a level 1 buf from a level 0 inode");
                }
            }
            else if (level == 0)
            {
                if (wip.get_inode_level() == 0)
                {
                    DEFS.ASSERT(someL0fbn < 16, "Requesting for large fbn in level-0 file");
                    int idx = (int)someL0fbn;
                    long dbn0 = wip.get_child_dbn(idx);
                    DEFS.ASSERT(dbn0 != DBN.INVALID, "Invalid dbn found in a valid portion 4, fsize = " + wip.get_filesize());

                    RedBufL0 wbl0 = (RedBufL0)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L0, someL0fbn, true, Array.Empty<long>(), 0);
                    wbl0.m_dbn = dbn0;

                    if (forwrite == false)
                    {
                        ReadPlanElement rpe = redfsBlockAllocator.PrepareReadPlanSingle(dbn0);
                        redFSPersistantStorage.ExecuteReadPlanSingle(rpe, wbl0);
                    }
                    else if (forwrite == true)
                    {

                    }

                    //
                    // WTF, i had put this before read, and had fuck trouble until i finally rootcaused the issue.
                    // Same with fast write case, i have to read and write from loadbuf for inode L0.
                    // 
                    if (forwrite == false && wip.get_wiptype() == WIP_TYPE.PUBLIC_INODE_FILE)
                    {
                        DEFS.ASSERT(wbl0.needtouchbuf, "This cannot be cleared out! 6");
                        DEFS.ASSERT(wip.get_ino() == 0, "ino for inodefile");
                        redfsBlockAllocator.touch_refcount(wip.get_filefsid(), wip.get_ino(), wbl0, true);
                        DEFS.ASSERT(false, "for inode wip, the wip level cannot be zero!");
                    }

                    wip.insert_buffer(mFreeBufCache, 0, wbl0);

                    return wbl0;
                }
                else
                {
                    RedBufL1 wbl1 = (RedBufL1)redfs_load_buf(wip, 1, someL0fbn, forwrite);

                    int idx = (int)(someL0fbn % OPS.FS_SPAN_OUT);
                    long dbn0 = wbl1.get_child_dbn( idx);

                    if (!forwrite)
                    {
                        DEFS.ASSERT(dbn0 != DBN.INVALID, "Invalid dbn found in a valid portion 5, fsize = " +
                            wip.get_filesize() + " wbl1.sfbn = " + wbl1.m_start_fbn + " somel0fbn = " + someL0fbn);
                    }

                    RedBufL0 wbl0 = (RedBufL0)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L0, someL0fbn, true, Array.Empty<long>(), 0); ;

                    wbl0.m_dbn = dbn0;

                    //XXX verify workflow.
                    if (forwrite == false && wbl0.m_dbn != 0)
                    {
                        ReadPlanElement rpe = redfsBlockAllocator.PrepareReadPlanSingle(dbn0);
                        redFSPersistantStorage.ExecuteReadPlanSingle(rpe, wbl0);
                    }

                    if (wip.get_wiptype() == WIP_TYPE.PUBLIC_INODE_FILE)
                    {
                        DEFS.ASSERT(wbl0.needtouchbuf, "This cannot be cleared out! 4");
                        redfsBlockAllocator.touch_refcount(wip.get_filefsid(), wip.get_ino(), wbl0, true);
                    }

                    wip.insert_buffer(mFreeBufCache, 0, wbl0);
                    
                    return wbl0;
                }
            }
            DEFS.ASSERT(false, "redfs_load_buf failed to find anything");
            return null;
        }

        private void redfs_levelincr_regularfile_wip(RedFS_Inode wip)
        {
            if (wip.get_inode_level() == 0)
            {
                DEFS.ASSERT(OPS.NUML0(wip.get_filesize()) == 16, "Unfull wip in level increment codepath ");

                RedBufL1 wbl1 = (RedBufL1)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L1, 0, false, Array.Empty<long>(), 0);
                for (int i = 0; i < 16; i++)
                {
                    wbl1.set_child_dbn(i, wip.get_child_dbn(i));
                    wip.set_child_dbn(i, DBN.INVALID);
                }
                wip.set_child_dbn(0, wbl1.m_dbn);

                wip.insert_buffer(mFreeBufCache, 1, wbl1);
            }
            else if (wip.get_inode_level() == 1)
            {
                DEFS.ASSERT(OPS.NUML1(wip.get_filesize()) == 16, "Unfull wip in level increment codepath2");

                RedBufL2 wbl2 = (RedBufL2)redfs_allocate_buffer(wip, BLK_TYPE.REGULAR_FILE_L2, 0, false, Array.Empty<long>(), 0);
                for (int i = 0; i < 16; i++)
                {
                    wbl2.set_child_dbn(i, wip.get_child_dbn(i));
                    wip.set_child_dbn(i, DBN.INVALID);
                }
                wip.set_child_dbn(0, wbl2.m_dbn);

                wip.insert_buffer(mFreeBufCache, 2, wbl2);
            }
            else
            {
                DEFS.ASSERT(false, "Cannot level increment a L2 level file");
            }
            wip.is_dirty = true;
        }


        /*
         * The caller must set the parent appropriately. This will just return a buffer from cache
         */ 
        private Red_Buffer redfs_allocate_buffer(RedFS_Inode wip, BLK_TYPE type, long startfbn, bool forread, long[] preAllocDbns, int arrayIdx)
        {
            Red_Buffer wb = null;

            int fsid = wip.get_filefsid();
            SPAN_TYPE spanType = wip.spanType;

            switch (type)
            {
                case BLK_TYPE.REGULAR_FILE_L0:
                    wb = mFreeBufCache.allocate(startfbn, "L0 for " + wip.get_ino());
                    break;
                case BLK_TYPE.REGULAR_FILE_L1:
                    wb = new RedBufL1(startfbn);
                    break;
                case BLK_TYPE.REGULAR_FILE_L2:
                    wb = new RedBufL2(startfbn);
                    break;
            }

            DEFS.ASSERT(wb != null, "Incorrect buffer type passed for allocate() in REDFS");

            if (forread == true)
            {
                wb.set_dirty(false);
                wb.set_ondisk_exist_flag(true);
            }
            else
            {
                wb.set_dirty(true);
                wb.set_ondisk_exist_flag(true);

                long dbn = -1;
                //We we are growing the file by a large amount, we could just preallocate dbns for this run and use it here.
                if (preAllocDbns.Length > 0 && arrayIdx >= 0 && arrayIdx < preAllocDbns.Length)
                {
                    dbn = preAllocDbns[arrayIdx];
                }
                else
                {
                    //Can be called for DEFAULT or MIRROR spantype. Otherwise we assert.
                    DEFS.ASSERT(spanType == SPAN_TYPE.DEFAULT || spanType == SPAN_TYPE.MIRRORED, "Incorrect type passed!");
                    dbn = redfsBlockAllocator.allocateDBN(wip, spanType);
                }

                wb.set_dbn_reassignment_flag(false); //this is just created!.
                wb.set_touchrefcnt_needed(false); //nobody is derived from this.
                REDFS_BUFFER_ENCAPSULATED wbe = new REDFS_BUFFER_ENCAPSULATED(wb);
                wbe.set_dbn(dbn);
            }

            return wb;
        }

    } //end of REDFSCore
}