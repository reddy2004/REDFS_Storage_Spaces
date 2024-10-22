﻿using System;
using System.Collections.Generic;
using System.Text;

namespace REDFS_ClusterMode
{
    public class ZBufferCache
    {
        //256Megabytes of cache.
        private RedBufL0[] iStack = new RedBufL0[1024 * 16 * 4];
        private int iStackTop = 0;

        public ZBufferCache()
        {

        }

        public int GetZBufferUsageInMB()
        {
            return 4 * (iStack.Length - iStackTop) / 1024;
        }

        public void init()
        {
            for (int i = 0; i < iStack.Length; i++)
            {
                iStack[i] = new RedBufL0(0);
            }
            iStackTop = iStack.Length - 1;
        }

        public void shutdown()
        {
            if (iStackTop != iStack.Length - 1)
                Console.WriteLine("Y", "Appears that not all bufs are recovered : " + iStackTop);
            DEFS.ASSERT(iStackTop == iStack.Length - 1, "Appears that not all bufs are recovered : " + iStackTop);
        }

        public RedBufL0 allocate(long sf, string msg)
        {
            lock (iStack)
            {
                RedBufL0 wb = iStack[iStackTop];
                iStack[iStackTop] = null;

                wb.tracker = iStackTop;
                wb.trackermsg = msg;
                iStackTop--;
                wb.reinitbuf(sf);
                return wb;
            }
        }

        public void deallocateList(List<Red_Buffer> wblist, string msg)
        {
            int count = wblist.Count;
            for (int i = 0; i < count; i++)
                deallocate4((RedBufL0)wblist[i], msg + "[" + i + "]");
            wblist.Clear();
        }

        public void deallocate4(RedBufL0 wb, string msg)
        {
            lock (iStack)
            {
                iStackTop++;
                iStack[iStackTop] = wb;
 
            }
        }
    }
}
