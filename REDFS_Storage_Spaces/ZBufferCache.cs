using System;
using System.Collections.Generic;
using System.Text;

namespace REDFS_ClusterMode
{
    public class ZBufferCache
    {
        //256Megabytes of cache.
        private RedBufL0[] iStack = new RedBufL0[1024 * 16 * 4];
        private int iStackTop = 0;

        IDictionary<int, RedBufL0> tracker = new Dictionary<int, RedBufL0>();

        List<string> msgs = new List<string>();
        public ZBufferCache()
        {

        }

        public int GetZBufferUsageInMB()
        {
            return 4 * (iStack.Length - iStackTop) / 1024;
        }
        public int getUsedZBufferCacheInMB()
        {
            return 4 * ((iStack.Length - iStackTop) / 1024);
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
                if (iStackTop == 65534)
                {
                    Console.WriteLine("for debug to hit!");
                }
                RedBufL0 wb = iStack[iStackTop];
                iStack[iStackTop] = null;

                wb.tracker = iStackTop;
                wb.trackermsg = msg;
                //tracker.Add(iStackTop, wb);
                iStackTop--;
                wb.reinitbuf(sf);
                msgs.Add("(+ " + wb.tracker +  " ) : " + msg + " (" + (iStackTop + 1) + " to " + iStackTop + ")");
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
                //tracker.Remove(wb.tracker);
                msgs.Add("(- " + wb.tracker + ") : " + msg + " (" + iStackTop + " to " + (iStackTop+1) + ")");
                iStackTop++;
                iStack[iStackTop] = wb;
            }
        }
    }
}
