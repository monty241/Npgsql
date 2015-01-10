using Npgsql;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NpgsqlTests
{
    [TestFixture]
    public class LargeObjectTests : TestBase
    {
        public LargeObjectTests(string backendVersion) : base(backendVersion) { }

        [Test]
        public void Test()
        {
            using (var transaction = Conn.BeginTransaction())
            {
                var manager = new NpgsqlLargeObjectManager(Conn);
                uint oid = manager.Create();
                using (var stream = manager.OpenReadWrite(oid))
                {
                    var buf = Encoding.UTF8.GetBytes("Hello");
                    stream.Write(buf, 0, buf.Length);
                    stream.Seek(0, System.IO.SeekOrigin.Begin);
                    var buf2 = new byte[buf.Length];
                    stream.ReadAll(buf2, 0, buf2.Length);
                    Assert.That(buf.SequenceEqual(buf2));
                }

                transaction.Rollback();
            }
        }
    }
}
