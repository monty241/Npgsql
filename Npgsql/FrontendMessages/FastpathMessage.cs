using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;

namespace Npgsql.FrontendMessages
{
    class FastpathMessage : ComplexFrontendMessage
    {
        const byte Code = (byte)'F';

        internal uint FunctionOID { get; set; }

        internal ByteArraySlice[] Arguments { get; set; }

        State _state;
        int _currentParameter;
        int _currentParameterPos;

        internal FastpathMessage(uint functionOID, ByteArraySlice[] arguments)
        {
            FunctionOID = functionOID;
            Arguments = arguments;
            _state = State.WroteNothing;
            _currentParameter = 0;
            _currentParameterPos = -1;
        }

        internal override bool Write(NpgsqlBuffer buf)
        {
            Contract.Requires(Arguments != null, "Arguments are missing");

            switch (_state)
            {
                case State.WroteNothing:
                    var headerLength = 4 + 4 + 2 + 1 * 2 + 2;
                    var messageLength = headerLength + Arguments.Sum(a => 4 + a.Length) + 2;
                    if (1 + headerLength > buf.WriteSpaceLeft)
                        return false;

                    buf.WriteByte(Code);
                    buf.WriteInt32(messageLength);
                    buf.WriteInt32((int)FunctionOID);
                    buf.WriteInt16(1); // One argument format code following
                    buf.WriteInt16((short)FormatCode.Binary);
                    buf.WriteInt16(Arguments.Length);
                    goto case State.WroteHeader;

                case State.WroteHeader:
                    _state = State.WroteHeader;
                    for (; _currentParameter < Arguments.Length; _currentParameter++, _currentParameterPos = -1)
                    {
                        
                        ByteArraySlice param = Arguments[_currentParameter];
                        if (_currentParameterPos == -1)
                        {
                            if (buf.WriteSpaceLeft < 4)
                                return false;

                            buf.WriteInt32(param.Array == null ? -1 : param.Length);
                            _currentParameterPos = 0;
                        }
                        if (param.Array != null)
                        {
                            var bytes = Math.Min(param.Length, buf.WriteSpaceLeft);
                            buf.WriteBytesSimple(param.Array, param.Offset + _currentParameterPos, bytes);
                            _currentParameterPos += bytes;
                            if (_currentParameterPos != param.Length)
                                return false;
                        }
                    }

                    if (buf.WriteSpaceLeft < 2)
                        return false;

                    buf.WriteInt16((short)FormatCode.Binary); // Result format code
                    _state = State.WroteAll;
                    return true;

                default:
                    throw PGUtil.ThrowIfReached();
            }
        }

        internal struct ByteArraySlice
        {
            public byte[] Array;
            public int Offset;
            public int Length;

            public static implicit operator ByteArraySlice(byte[] array) {
                return new ByteArraySlice(array, 0, array == null ? 0 : array.Length);
            }

            public ByteArraySlice(byte[] array, int offset, int length)
            {
                Array = array;
                Offset = offset;
                Length = length;
            }
        }

        private enum State
        {
            WroteNothing,
            WroteHeader,
            WroteParameters,
            WroteAll
        }
    }
}
