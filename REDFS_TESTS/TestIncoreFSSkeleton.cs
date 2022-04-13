using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using DokanNet;
using DokanNet.Logging;
using static DokanNet.FormatProviders;
using FileAccess = DokanNet.FileAccess;
using System.Text;
using System.Collections;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using REDFS_ClusterMode;
using Newtonsoft.Json;

namespace REDFS_TESTS
{
    [TestClass]
    public class TestIncoreFSSkeleton
    {
        [TestMethod]
        public void TestIFSInit()
        {
            IncoreFSSkeleton ifs = new IncoreFSSkeleton(null);
            Assert.IsTrue(ifs != null);
        }
    }
}
