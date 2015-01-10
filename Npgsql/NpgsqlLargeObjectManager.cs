using Npgsql.FrontendMessages;
using Npgsql.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Npgsql
{
    /// <summary>
    /// Large object manager. This class can be used to store very large files in a PostgreSQL database.
    /// </summary>
    public class NpgsqlLargeObjectManager
    {
        const int INV_WRITE = 0x00020000;
        const int INV_READ = 0x00040000;

        internal readonly NpgsqlConnection _connection;

        /// <summary>
        /// The largest chunk size read and write operations will read/write each roundtrip to the network. Default 4 MB.
        /// </summary>
        public int MaxTransferBlockSize { get; set; }

        /// <summary>
        /// Creates an NpgsqlLargeObjectManager for this connection. The connection must be opened to perform remote operations.
        /// </summary>
        /// <param name="connection"></param>
        public NpgsqlLargeObjectManager(NpgsqlConnection connection)
        {
            _connection = connection;
            MaxTransferBlockSize = 4 * 1024 * 1024; // 4MB
        }

        /// <summary>
        /// Execute a backend function with the Fastpath protocol
        /// </summary>
        /// <returns>The length of the return value (or -1 if NULL), to be read in the NpgsqlBuffer</returns>
        [GenerateAsync]
        internal int ExecuteFunction(Function function, int expectedLength = -1, params FastpathMessage.ByteArraySlice[] arguments)
        {
            var msg = new FastpathMessage((uint)function, arguments);
            
            _connection.Connector.SendMessage(msg);
            _connection.Connector.Buffer.Flush();

            var backendMsg = (FunctionCallResponseMessage)_connection.Connector.ReadSingleMessage(DataRowLoadingMode.Sequential);
            var len = backendMsg.Length;
            if (expectedLength != -1 && expectedLength != len)
                throw PGUtil.ThrowIfReached("Unexpected return value length");
            return len;
        }

        [GenerateAsync]
        internal void EatReadyForQuery()
        {
            var backendMsg = (ReadyForQueryMessage)_connection.Connector.ReadSingleMessage();
        }

        /// <summary>
        /// Execute a function that returns an Int32
        /// </summary>
        /// <param name="function"></param>
        /// <param name="arguments"></param>
        /// <returns></returns>
        [GenerateAsync]
        internal int ExecuteFunctionInt32(Function function, params FastpathMessage.ByteArraySlice[] arguments)
        {
            _connection.CheckConnectionReady();
            using (_connection.Connector.BlockNotifications())
            {
                ExecuteFunction(function, 4, arguments);
                _connection.Connector.Buffer.Ensure(4);
                var ret = _connection.Connector.Buffer.ReadInt32();
                EatReadyForQuery();
                return ret;
            }
        }

        /// <summary>
        /// Execute a function that returns an Int64
        /// </summary>
        /// <param name="function"></param>
        /// <param name="arguments"></param>
        /// <returns></returns>
        [GenerateAsync]
        internal long ExecuteFunctionInt64(Function function, params FastpathMessage.ByteArraySlice[] arguments)
        {
            _connection.CheckConnectionReady();
            using (_connection.Connector.BlockNotifications())
            {
                ExecuteFunction(function, 8, arguments);
                _connection.Connector.Buffer.Ensure(8);
                var ret = _connection.Connector.Buffer.ReadInt64();
                EatReadyForQuery();
                return ret;
            }
        }

        internal static byte[] GetInt32(int i)
        {
            return BitConverter.GetBytes(System.Net.IPAddress.HostToNetworkOrder(i));
        }

        internal static byte[] GetInt64(long i)
        {
            return BitConverter.GetBytes(System.Net.IPAddress.HostToNetworkOrder(i));
        }

        /// <summary>
        /// Create an empty large object in the database. If an oid is specified but is already in use, an NpgsqlException will be thrown.
        /// </summary>
        /// <param name="preferredOid">A preferred oid, or specify 0 if one should be automatically assigned</param>
        /// <returns>The oid for the large object created</returns>
        /// <exception cref="NpgsqlException">If an oid is already in use</exception>
        [GenerateAsync]
        [CLSCompliant(false)]
        public uint Create(uint preferredOid = 0)
        {
            return (uint)ExecuteFunctionInt32(Function.lo_create, GetInt32((int)preferredOid));
        }

        /// <summary>
        /// Opens a large object on the backend, returning a stream controlling this remote object.
        /// A transaction snapshot is taken by the backend when the object is opened with only read permissions.
        /// When reading from this object, the contents reflects the time when the snapshot was taken.
        /// Note that this method, as well as operations on the stream must be wrapped inside a transaction.
        /// </summary>
        /// <param name="oid">Oid of the object</param>
        /// <returns>An NpgsqlLargeObjectStream</returns>
        [GenerateAsync]
        public NpgsqlLargeObjectStream OpenRead(uint oid)
        {
            var fd = ExecuteFunctionInt32(Function.lo_open, GetInt32((int)oid), GetInt32(INV_READ));
            return new NpgsqlLargeObjectStream(this, oid, fd, false);
        }

        /// <summary>
        /// Opens a large object on the backend, returning a stream controlling this remote object.
        /// Note that this method, as well as operations on the stream must be wrapped inside a transaction.
        /// </summary>
        /// <param name="oid">Oid of the object</param>
        /// <returns>An NpgsqlLargeObjectStream</returns>
        [GenerateAsync]
        public NpgsqlLargeObjectStream OpenReadWrite(uint oid)
        {
            var fd = ExecuteFunctionInt32(Function.lo_open, GetInt32((int)oid), GetInt32(INV_READ | INV_WRITE));
            return new NpgsqlLargeObjectStream(this, oid, fd, true);
        }

        /// <summary>
        /// Deletes a large object on the backend.
        /// </summary>
        /// <param name="oid">Oid of the object to delete</param>
        [GenerateAsync]
        public void Unlink(uint oid)
        {
            ExecuteFunctionInt32(Function.lo_unlink, GetInt32((int)oid));
        }

        /// <summary>
        /// Exports a large object stored in the database to a file on the backend. This requires superuser permissions.
        /// </summary>
        /// <param name="oid">Oid of the object to export</param>
        /// <param name="path">Path to write the file on the backend</param>
        [GenerateAsync]
        public void ExportRemote(uint oid, string path)
        {
            ExecuteFunctionInt32(Function.lo_export, GetInt32((int)oid), Encoding.UTF8.GetBytes(path));
        }

        /// <summary>
        /// Imports a large object to be stored as a large object in the database from a file stored on the backend. This requires superuser permissions.
        /// </summary>
        /// <param name="path">Path to read the file on the backend</param>
        /// <param name="oid">A preferred oid, or specify 0 if one should be automatically assigned</param>
        [GenerateAsync]
        public void ImportRemote(string path, uint oid = 0)
        {
            ExecuteFunctionInt32(Function.lo_import, Encoding.UTF8.GetBytes(path), GetInt32((int)oid));
        }

        /// <summary>
        /// Since PostgreSQL 9.3, large objects larger than 2GB can be handled, up to 4TB.
        /// This property returns true whether the PostgreSQL version is >= 9.3.
        /// </summary>
        public bool Has64BitSupport { get { return _connection.PostgreSqlVersion >= new Version(9, 3); } }

        internal enum Function : uint
        {
            lo_open = 952,
            lo_close = 953,
            loread = 954,
            lowrite = 955,
            lo_lseek = 956,
            lo_lseek64 = 3170, // Since PostgreSQL 9.3
            lo_creat = 957,
            lo_create = 715,
            lo_tell = 958,
            lo_tell64 = 3171, // Since PostgreSQL 9.3
            lo_truncate = 1004,
            lo_truncate64 = 3172, // Since PostgreSQL 9.3

            // These four are available since PostgreSQL 9.4
            lo_from_bytea = 3457,
            lo_get = 3458,
            lo_get_fragment = 3459,
            lo_put = 3460,

            lo_unlink = 964,

            lo_import = 764,
            lo_import_with_oid = 767,
            lo_export = 765,
        }
    }
}
