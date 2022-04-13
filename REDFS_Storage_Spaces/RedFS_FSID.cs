using System;
using System.Collections.Generic;
using System.Text;

namespace REDFS_ClusterMode
{
    public class CFSvalueoffsets
    {
        public static int fsid_ofs = 0;                //int
        public static int fsid_bofs = 4;               //int
        public static int fsid_flags = 8;              //long

        public static int fsid_logical_data = 16;      //long
        public static int fsid_unique_data = 24;       //long
        public static int fsid_start_inodenum = 32;     //int

        public static int fsid_created = 36;           //long
        public static int fsid_lastrefresh = 44;       //long

        //next 512 - 52 = 460 bytes are free

        //the 512bytes of the 2nd half of the first 1MB
        public static int fsid_inofile_data = 512;       //wip->data
        public static int fsid_inomap_data = 768;       //wip->data
    }

    //Volume data that resides inside the 4K FSID

    public class RedFS_FSID
    {
        public byte[] data = new byte[1024];
        private bool is_dirty;

        public int get_fsid() { return OPS.get_int(data, CFSvalueoffsets.fsid_ofs); ; }
        public int get_parent_fsid() { return OPS.get_int(data, CFSvalueoffsets.fsid_bofs); }

        private RedFS_Inode _ninowip;

        private RedFS_Inode _nimapwip;

        public void set_dirty(bool flag) { is_dirty = flag; }
        public RedFS_Inode get_inode_file_wip(string requester)
        {
            if (_ninowip == null)
            {
                _ninowip = new RedFS_Inode(WIP_TYPE.PUBLIC_INODE_FILE, 0, -1);
                for (int i = 0; i < OPS.WIP_SIZE; i++)
                {
                    _ninowip.data[i] = data[CFSvalueoffsets.fsid_inofile_data + i];
                }
                _ninowip.set_wiptype(WIP_TYPE.PUBLIC_INODE_FILE);
                _ninowip.setfilefsid_on_dirty( get_fsid() );
            }

            Console.WriteLine("Giving a inowip to " + requester);
            return _ninowip;
        }

        public Boolean Equals(RedFS_Inode other)
        {
            for (int i=0;i<data.Length;i++)
            {
                if (this.data[i] != other.data[i])
                {
                    return false;
                }
            }
            return true;
        }

        public RedFS_Inode get_inodemap_wip()
        {
            RedFS_Inode inowip = new RedFS_Inode(WIP_TYPE.PUBLIC_INODE_MAP, 0, -1);
            byte[] buf = new byte[OPS.WIP_SIZE];
            for (int i = 0; i < OPS.WIP_SIZE; i++)
            {
                buf[i] = data[CFSvalueoffsets.fsid_inomap_data + i];
            }
            inowip.parse_bytes(buf);
            return inowip;
        }

        public bool sync_internal()
        {
            if (_ninowip != null)
            {
                for (int i = 0; i < OPS.WIP_SIZE; i++)
                {
                    data[CFSvalueoffsets.fsid_inofile_data + i] = _ninowip.data[i];
                }
                is_dirty = true;
            }
            return true;
        }

        public bool set_inodemap_wip(RedFS_Inode wip)
        {
            byte[] buf = new byte[OPS.WIP_SIZE];
            wip.get_bytes(buf);

            for (int i = 0; i < OPS.WIP_SIZE; i++)
            {
                data[CFSvalueoffsets.fsid_inomap_data + i] = buf[i];
            }
            is_dirty = true;

            _nimapwip = wip;

            return true;
        }

        public bool set_inodefile_wip(RedFS_Inode wip)
        {
            byte[] buf = new byte[OPS.WIP_SIZE];
            wip.get_bytes(buf);

            for (int i = 0; i < OPS.WIP_SIZE; i++)
            {
                data[CFSvalueoffsets.fsid_inofile_data + i] = buf[i];
            }
            is_dirty = true;

            _ninowip = wip;
            return true;
        }

        private void init_internal2()
        {
            RedFS_Inode w2 = get_inodemap_wip();
            for (int i = 0; i < 16; i++)
            {
                w2.set_child_dbn(i, DBN.INVALID);
            }
            w2.set_filesize(0);
            set_inodemap_wip(w2);
            set_logical_data(0);
            is_dirty = true;
        }

        public RedFS_FSID(int id, int bid)
        {
            is_dirty = true;
            OPS.set_int(data, CFSvalueoffsets.fsid_ofs, id);
            OPS.set_int(data, CFSvalueoffsets.fsid_bofs, bid);
            OPS.set_long(data, CFSvalueoffsets.fsid_created, DateTime.Now.ToUniversalTime().Ticks);
            OPS.set_long(data, CFSvalueoffsets.fsid_lastrefresh, DateTime.Now.ToUniversalTime().Ticks);

            init_internal2();
        }

        public RedFS_FSID MakeCopy()
        {
            RedFS_FSID copy = new RedFS_FSID(get_fsid(), get_parent_fsid());
            Array.Copy(data, 0, copy.data, 0, OPS.FSID_BLOCK_SIZE);
            return copy;
        }

        /*
         * For duping.
         */
        public RedFS_FSID(int id, int bid, byte[] buffer)
        {
            for (int i = 0; i < OPS.FSID_BLOCK_SIZE; i++)
                data[i] = buffer[i];

            OPS.set_int(data, CFSvalueoffsets.fsid_ofs, id);
            OPS.set_int(data, CFSvalueoffsets.fsid_bofs, bid);

            OPS.set_long(data, CFSvalueoffsets.fsid_created, DateTime.Now.ToUniversalTime().Ticks);
            OPS.set_long(data, CFSvalueoffsets.fsid_lastrefresh, DateTime.Now.ToUniversalTime().Ticks);
            is_dirty = true;
        }

        public RedFS_FSID(int id, byte[] buffer)
        {
            for (int i = 0; i < OPS.FSID_BLOCK_SIZE; i++)
                data[i] = buffer[i];
            OPS.set_int(data, CFSvalueoffsets.fsid_ofs, id);
        }

        public void get_bytes(byte[] buffer)
        {
            for (int i = 0; i < OPS.FSID_BLOCK_SIZE; i++)
            {
                buffer[i] = data[i];
            }
        }
        public int get_start_inonumber()
        {
            return OPS.get_int(data, CFSvalueoffsets.fsid_start_inodenum);
        }

        public void set_start_inonumber(int sinon)
        {
            OPS.set_int(data, CFSvalueoffsets.fsid_start_inodenum, sinon);
            is_dirty = true;
        }

        public void diff_upadate_logical_data(long value)
        {
            long final = get_logical_data() + value;
            final = (final < 0) ? 0 : final;
            set_logical_data(final);
        }

        public void set_logical_data(long d)
        {
            OPS.set_long(data, CFSvalueoffsets.fsid_logical_data, d);
            is_dirty = true;
        }

        public long get_logical_data()
        {
            return OPS.get_long(data, CFSvalueoffsets.fsid_logical_data);
        }

        public Boolean isDirty()
        {
            return ((_nimapwip != null && _nimapwip.is_dirty) ||
                   (_ninowip != null && _ninowip.is_dirty) || is_dirty);
        }
    }
}
