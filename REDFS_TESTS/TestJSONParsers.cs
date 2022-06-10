using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Threading;
using System.Text;

namespace REDFS_TESTS
{
    [TestClass]
    public class TestJSONParsers
    {
        public string testStr1 = "{\"ino\":2,\"fileInfo\":{\"FileName\":\"\\\\\",\"Attributes\":16,\"CreationTime\":\"2022-06-05T22:43:52.7584564+05:30\",\"LastAccessTime\":\"2022-06-05T22:43:52.7584594+05:30\",\"LastWriteTime\":\"2022-06-05T22:43:52.7584603+05:30\",\"Length\":0},\"inodes\":[{\"fileInfo\":{\"FileName\":\"Lenka - Everything At Once.mp4\",\"Attributes\":128,\"CreationTime\":\"2022-06-05T22:38:54.9498214+05:30\",\"LastAccessTime\":\"2022-06-05T22:38:54.9498264+05:30\",\"LastWriteTime\":\"2022-06-05T22:38:54.9498289+05:30\",\"Length\":28311552},\"ino\":64},{\"fileInfo\":{\"FileName\":\"Lenka - Everything At Once - Copy.mp4\",\"Attributes\":128,\"CreationTime\":\"2022-06-05T22:39:10.8832997+05:30\",\"LastAccessTime\":\"2022-06-05T22:39:10.8833024+05:30\",\"LastWriteTime\":\"2022-06-05T22:39:10.8833034+05:30\",\"Length\":28311552},\"ino\":65}]}";
        [TestMethod]
        public void json_1()
        {
            try
            {
                OnDiskDirectoryInfo oddi = JsonConvert.DeserializeObject<OnDiskDirectoryInfo>(testStr1);

                foreach (OnDiskInodeInfo item in oddi.inodes)
                {
                    Console.WriteLine(item);
                }
            }
            catch (Exception e)
            {
                Assert.IsTrue(false);
                throw new SystemException(e.Message);
            }
        }
    }
}
