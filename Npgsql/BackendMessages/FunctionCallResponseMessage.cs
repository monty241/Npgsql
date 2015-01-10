using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Npgsql.Messages
{
    class FunctionCallResponseMessage : BackendMessage
    {
        internal override BackendMessageCode Code
        {
            get { return BackendMessageCode.FunctionCallResponse; }
        }

        internal int Length { get; private set; }
        internal NpgsqlBuffer Buf { get; private set; }

        [GenerateAsync]
        internal FunctionCallResponseMessage Load(NpgsqlBuffer buf)
        {
            buf.Ensure(4);
            Length = buf.ReadInt32();
            Buf = buf;
            return this;
        }
    }
}
