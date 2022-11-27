using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using DokanNet;

namespace REDFS_ClusterMode
{ 
    public class DEFS
    {
        public static void ASSERT(bool value, String dbg)
        {
            if (!value)
            {
                Console.BackgroundColor = ConsoleColor.DarkRed;
                Console.WriteLine("ASSERT :: " + dbg);
                Console.BackgroundColor = ConsoleColor.Black;
                try
                {
                    Dokan.Unmount('X');
                }
                catch (Exception ed)
                {
                    Console.WriteLine("Exception " + ed.Message);
                }
                Console.WriteLine(System.Environment.StackTrace);
                REDFSContainer rc = REDFS.redfsContainer;
                throw new SystemException(dbg);
                /*
                System.Threading.Thread.Sleep(100000);
                System.Environment.Exit(0);
                */
            }
        }

        public static void DEBUG_RED(String dbg)
        {
            Console.BackgroundColor = ConsoleColor.Red;
            Console.WriteLine("DEBUG :: " + dbg);
            Console.BackgroundColor = ConsoleColor.Black;
        }

        public static void DEBUG_YELLOW(String dbg)
        {
            Console.BackgroundColor = ConsoleColor.Blue;
            Console.WriteLine("DEBUG :: " + dbg);
            Console.BackgroundColor = ConsoleColor.Black;
        }

        public static void DEBUG_GREEN(String dbg)
        {
            Console.BackgroundColor = ConsoleColor.Green;
            Console.WriteLine("ASSERT :: " + dbg);
            Console.BackgroundColor = ConsoleColor.Black;
        }
    }
}
