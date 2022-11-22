using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using DokanNet;
using System.Collections;

namespace REDFS_ClusterMode
{
    public enum WRITE_TYPE
    {
        TRUNCATE_AND_OVERWRITE,
        OVERWRITE_IN_PLACE
    }

    public enum WIP_TYPE
    {
        UNDEFINED,
        PUBLIC_INODE_FILE, //Internal file that has all the inode heads/roots in the filesystem.
        PUBLIC_INODE_MAP,  //A bitmap of the public inode file to see which inode numbers are used/set and to create new ones in free slots
        DIRECTORY_FILE,    //A directory inode
        REGULAR_FILE       //A file inode
    };

    public class WIDOffsets
    {
        public static int wip_dbndata = 0;              //long[16] array
        public static int wip_inoloc = 128;              //int
        public static int wip_parent = 132;              //int - not used for the time being.
        public static int wip_size = 136;                //long

        public static int wip_created_from_fsid = 144;   //int
        public static int wip_modified_in_fsid = 148;    //int
        public static int wip_flags = 152;               //int
        public static int wip_cookie = 156;              //int
        public static int wip_ibflag = 160;              //int
    }

    public class wbufcomparator : IComparer<Red_Buffer>
    {
        public int Compare(Red_Buffer c1, Red_Buffer c2)
        {
            if (c1.get_start_fbn() < c2.get_start_fbn()) return -1;
            else if (c1.get_start_fbn() > c2.get_start_fbn()) return 1;
            else return 0;
        }
    }

    public class DebugSummaryOfFSID
    {
        public long totalLogicalData;

        public long numL2s, numL1s, numL0s;
        public int numFiles, numDirectories;
    }

    public class PrintableWIP
    {
        public int ino, pino;
        public long length;
        public string spanType;
        public string wipType;

        public int level;
        public long[] wipIdx;
        public long[] L1_DBNS;
        public long[] L0_DBNS;

        public long ondiskL2Blocks = 0;
        public long ondiskL1Blocks = 0;
        public long ondiskL0Blocks = 0;

        //read in all the blocks requested by the called. useful for examining ondisk data.
        public List<Red_Buffer> requestedBlocks = new List<Red_Buffer>();

        public string json = "";

        public Boolean IsSimilarTree(PrintableWIP other)
        {
            if (this.level != other.level || this.wipType != other.wipType || this.wipIdx.Length != other.wipIdx.Length)
            {
                return false;
            }

            if (this.L0_DBNS != null)
            {
                for (int l0 = 0; l0 < this.L0_DBNS.Length; l0++)
                {
                    if (this.L0_DBNS[l0] != other.L0_DBNS[l0])
                    {
                        return false;
                    }
                }
            }

            if (this.L1_DBNS != null)
            {
                for (int l1 = 0; l1 < this.L1_DBNS.Length; l1++)
                {
                    if (this.L1_DBNS[l1] != other.L1_DBNS[l1])
                    {
                        return false;
                    }
                }
            }

            if (this.wipIdx != null)
            {
                for (int w = 0; w < this.wipIdx.Length; w++)
                {
                    if (this.wipIdx[w] != other.wipIdx[w])
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        public long getTotalOndiskBlocks()
        {
            return ondiskL0Blocks + ondiskL1Blocks + ondiskL2Blocks;
        }
    }

    /*
      * Structure of 256 bytes.
      * 8*16 = 128       direct/indirect pointers.
      * 4 bytes          inode number (must corrospond to offset in file, otherwise considered 'free' slot)
      * 4 bytes          wigen (positive value if inode is used)
      * 8 bytes          file size
     ---- 144 bytes ---
      * 4 bytes          created fsid key/ touched fsid key.
      * 4 bytes          flags (for dedupe, file type etc) 
      * 4 bytes          cookie.
      * 4 bytes          ibflag.
     ---- 160 bytes ----
      * 96 bytes         reserved for future use.
     ---- 256 bytes---
      */

    public class RedFS_Inode
    {
        public Red_Buffer _lasthitbuf;
        public string fingerprint;

        private WIP_TYPE wiptype;

        public byte[] data = new byte[OPS.WIP_SIZE];

        public int dbnTreeReassignmentCount = 0;

        //For logging
        public List<string> logs = new List<string>();
        public List<string> hashOfFBN2 = new List<string>();

        //Logging, to be used only for inowip
        public IDictionary fbn_wise_fps = new Dictionary<long, string>();

        public bool isWipValid = false;

        public void log(string msg)
        {
            if (logs.Count == 100)
            {
                logs.RemoveAt(0);
            }
            logs.Add(msg);
        }

        /*
         * This list will contain all the childen, both L0's and indirects.
         * Resize cases must be carefully handled. Cleaner will always sort
         * and proceed from L0 to L1 to L2
         * 
         * XXX Use hash map in parallel to increase look up speed?
         */
        public List<Red_Buffer> L0list = new List<Red_Buffer>();
        public List<Red_Buffer> L1list = new List<Red_Buffer>();
        public List<Red_Buffer> L2list = new List<Red_Buffer>();

        public List<Red_Buffer> ZerodBlocks = new List<Red_Buffer>();

        public bool is_dirty;
        public ulong iohistory = 0;
        public int m_ino;
        public long size; //this is only for debugging.

        public SPAN_TYPE spanType = SPAN_TYPE.DEFAULT;

        public long quickSearchStartDbnCachedValue = 0;

        public void update_fingerprint()
        {
            DEFS.ASSERT(isWipValid, "wip must be valid and populated!");
            fingerprint = OPS.compute_hash_string(data, 0, 160);
        }

        public void mark_isWipValid(bool flag)
        {
            isWipValid = flag;
        }

        public RedFS_Inode(WIP_TYPE t, int ino, int pino)
        {
            Array.Clear(data, 0, OPS.WIP_SIZE);
            m_ino = ino;
            for (int i = 0; i < 16; i++)
            {
                set_child_dbn(i, DBN.INVALID);
            }
            wiptype = t;
            set_wiptype(wiptype);
            set_int(WIDOffsets.wip_parent, pino);
            set_int(WIDOffsets.wip_inoloc, ino);
            is_dirty = false;

            dbnTreeReassignmentCount = 0;
        }

        public Boolean Equals(RedFS_Inode other)
        {
            if (this.wiptype != other.wiptype || this.m_ino != other.m_ino)
            {
                return false;
            }

            if ((this.get_ino() != other.get_ino()) || 
                (this.get_filesize() != other.get_filesize()))
            {
                return false;
            }

            for (int i=0;i<OPS.NUM_PTRS_IN_WIP;i++)
            {
                if (this.get_child_dbn(i) != other.get_child_dbn(i))
                {
                    return false;
                }
            }
            return true;
        }

        public void set_ino(int pino, int ino)
        {
            set_int(WIDOffsets.wip_inoloc, ino);
            set_int(WIDOffsets.wip_parent, pino);
            //m_ino = ino;
        }

        public int get_ino()
        {
            return get_int(WIDOffsets.wip_inoloc);
        }

        public void insert_buffer(ZBufferCache mFreeBufCache, int level, Red_Buffer wb)
        {
            List < Red_Buffer > list = null;
            long start_fbn = wb.get_start_fbn();

            switch (level)
            {
                case 0:
                    list = L0list;
                    break;
                case 1:
                    list = L1list;
                    break;
                case 2:
                    list = L2list;
                    break;
            }
             
            foreach (Red_Buffer rb in list)
            {
                if (rb.get_start_fbn() == wb.get_start_fbn())
                {
                    DEFS.ASSERT(rb.is_dirty() == false, "cannot be dirty");
                    DEFS.ASSERT(rb.get_ondisk_dbn() != wb.get_ondisk_dbn(), "Not utility cheeck!");

                    if (level == 0)
                    {
                        mFreeBufCache.deallocate4((RedBufL0)rb, "insert_buffer:" + get_ino() + " cnt:" + L0list.Count);
                    }
                    list.Remove(rb);
                    insert_buffer(mFreeBufCache, level, wb);
                    return;
                }
            }

            list.Add(wb);
        }

        public void sort_buflists()
        {
            L0list.Sort(new wbufcomparator());
            L1list.Sort(new wbufcomparator());
            L2list.Sort(new wbufcomparator());
        }

        public int get_incore_cnt()
        {
            return (L0list.Count + L1list.Count + L2list.Count);
        }

        public int get_filefsid_created()
        {
            return get_int(WIDOffsets.wip_created_from_fsid);
        }

        public int get_filefsid()
        {
            return get_int(WIDOffsets.wip_modified_in_fsid);
        }

        public void setfilefsid_on_dirty(int fsid)
        {
            int cf = get_int(WIDOffsets.wip_created_from_fsid);
            if (cf == 0)
            { //new file
                set_int(WIDOffsets.wip_created_from_fsid, fsid);
            }
            int modfsid = get_int(WIDOffsets.wip_modified_in_fsid);
            if (modfsid != fsid)
            {
                set_int(WIDOffsets.wip_modified_in_fsid, fsid);
                //DEFS.DEBUG("FSID", "Set fsid for inode: " + get_ino() + " from fsid " + modfsid + " to fsid");
            }
        }

        public int get_inode_level()
        {
            return OPS.FSIZETOILEVEL(get_filesize());
        }

        public long get_child_dbn(int idx)
        {
            int offset = idx * 8;
            return get_long(offset);
        }

        public void set_child_dbn(int idx, long dbn)
        {
            set_long(idx * 8, dbn);
        }

        public long get_filesize()
        {
            size = get_long(WIDOffsets.wip_size);
            return size;
        }

        public void set_filesize(long s)
        {
            set_long(WIDOffsets.wip_size, s);
            size = s;
        }

        public int get_cookie()
        {
            return get_int(WIDOffsets.wip_cookie);
        }
        public void set_cookie(int c)
        {
            set_int(WIDOffsets.wip_cookie, c);
            is_dirty = true;
        }


        public int get_ibflag()
        {
            return get_int(WIDOffsets.wip_ibflag);
        }
        public void set_ibflag(int c)
        {
            set_int(WIDOffsets.wip_ibflag, c);
        }

        public void set_wiptype(WIP_TYPE type)
        {
            int flag = get_int(WIDOffsets.wip_flags);
            int value = -1;
            switch (type)
            {
                case WIP_TYPE.PUBLIC_INODE_FILE:
                    value = 0;
                    break;
                case WIP_TYPE.PUBLIC_INODE_MAP:
                    value = 1;
                    break;
                case WIP_TYPE.DIRECTORY_FILE:
                    value = 2;
                    break;
                case WIP_TYPE.REGULAR_FILE:
                    value = 3;
                    break;
                case WIP_TYPE.UNDEFINED:
                    value = 4;
                    break;
            }
            //DEFS.ASSERT(value != -1 && value <= 3, "Entry for wip type in the flag is incorrect : type=" + type + " value = " + value);
            flag &= 0x0FFFFFF0;
            flag |= value;
            set_int(WIDOffsets.wip_flags, flag);
        }

        public WIP_TYPE get_wiptype()
        {
            int flag = get_int(WIDOffsets.wip_flags);
            int value = flag & 0x00000007;
            WIP_TYPE type = WIP_TYPE.UNDEFINED;
            switch (value)
            {
                case 0:
                    type = WIP_TYPE.PUBLIC_INODE_FILE;
                    break;
                case 1:
                    type = WIP_TYPE.PUBLIC_INODE_MAP;
                    break;
                case 2:
                    type = WIP_TYPE.DIRECTORY_FILE;
                    break;
                case 3:
                    type = WIP_TYPE.REGULAR_FILE;
                    break;
                case 4:
                    type = WIP_TYPE.UNDEFINED;
                    break;
            }
            //DEFS.ASSERT(type != WIP_TYPE.UNDEFINED, "Entry for wip type in the flag is incorrect");
            return type;
        }

        public void parse_bytes(byte[] buf)
        {
            DEFS.ASSERT(buf.Length == OPS.WIP_SIZE, "parse_bytes will not work correctly for non-standard input");
            //DEFS.DEBUG("FSID", "Copying filedata to wip - direct parse, ino = " + m_ino);
            for (int i = 0; i < OPS.WIP_SIZE; i++)
            {
                data[i] = buf[i];
            }
            m_ino = get_ino();
            size = get_long(WIDOffsets.wip_size);

            //basic expectations, in future add checksum
            if (m_ino >= 0 && size >=0 && (wiptype == WIP_TYPE.PUBLIC_INODE_FILE || wiptype == WIP_TYPE.PUBLIC_INODE_MAP ||
                wiptype == WIP_TYPE.DIRECTORY_FILE || wiptype == WIP_TYPE.REGULAR_FILE))
            {
                isWipValid = true;
            }
        }

        public bool verify_inode_number()
        {
            bool retval = (get_int(WIDOffsets.wip_inoloc) == m_ino) ? true : false;
            if (get_int(WIDOffsets.wip_inoloc) != 0 && !retval)
            {
                DEFS.ASSERT(false, "ERROR Some error is detected!! retval = " + retval);
            }
            //DEFS.DEBUG("C", get_string_rep2());
            //DEFS.DEBUG("C", "in verify inode " + get_int(WIDOffsets.wip_inoloc) + "," + m_ino);
            return retval;
        }

        public void set_parent_ino(int pino)
        {
            set_int(WIDOffsets.wip_parent, pino);
        }

        public int get_parent_ino()
        {
            return get_int(WIDOffsets.wip_parent);
        }

        public void get_bytes(byte[] buf)
        {
            DEFS.ASSERT(isWipValid, "wip must be valid when writing out to disk");
            DEFS.ASSERT(buf.Length == OPS.WIP_SIZE, "get_bytes will not work correctly for non-standard input");
            //DEFS.DEBUG("FSID", "Copying wip to file data - direct get, ino = " + get_ino());
            for (int i = 0; i < OPS.WIP_SIZE; i++)
            {
                buf[i] = data[i];
            }
        }

        public string get_string_rep2()
        {
            string ret = "DETAILS:" + get_ino() + "," + get_parent_ino() + " : " + get_wiptype() + ",sz=" + get_filesize() + ":dbns=";
            for (int i = 0; i < 16; i++)
            {
                ret += " " + get_child_dbn(i);
            }
            return ret;
        }

        /*
         * Following four functions for setting and getting int,long
         * values from the wip->data.
         */
        private void set_int(int byteoffset, int value)
        {
            byte[] val = BitConverter.GetBytes(value);
            data[byteoffset] = val[0];
            data[byteoffset + 1] = val[1];
            data[byteoffset + 2] = val[2];
            data[byteoffset + 3] = val[3];
            is_dirty = true;
        }
        private int get_int(int byteoffset)
        {
            return BitConverter.ToInt32(data, byteoffset); ;
        }
        private void set_long(int byteoffset, long value)
        {
            byte[] val = BitConverter.GetBytes(value);
            data[byteoffset] = val[0];
            data[byteoffset + 1] = val[1];
            data[byteoffset + 2] = val[2];
            data[byteoffset + 3] = val[3];
            data[byteoffset + 4] = val[4];
            data[byteoffset + 5] = val[5];
            data[byteoffset + 6] = val[6];
            data[byteoffset + 7] = val[7];
            is_dirty = true;
        }
        private long get_long(int byteoffset)
        {
            return BitConverter.ToInt64(data, byteoffset); ;
        }
    }
}
