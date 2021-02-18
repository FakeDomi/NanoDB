using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Domi.NanoDB
{
    public abstract class NanoDBElement
    {
        public static ReadOnlyArray<NanoDBElement> Elements { get; }

        public static BoolElement Bool { get; }
        public static ByteElement Byte { get; }
        public static ShortElement Short { get; }
        public static IntElement Int { get; }
        public static LongElement Long { get; }
        public static StringElement String8 { get; }
        public static StringElement String16 { get; }
        public static StringElement String32 { get; }
        public static StringElement String64 { get; }
        public static StringElement String128 { get; }
        public static StringElement String256 { get; }
        public static DataBlobElement DataBlob8 { get; }
        public static DataBlobElement DataBlob16 { get; }
        public static DataBlobElement DataBlob32 { get; }
        public static DataBlobElement DataBlob64 { get; }
        public static DataBlobElement DataBlob128 { get; }
        public static DataBlobElement DataBlob256 { get; }
        public static DateTimeElement DateTime { get; }

        public byte Id { get; }
        public int Size { get; }

        internal NanoDBElement(byte id, int size)
        {
            this.Id = id;
            this.Size = size;

            Elements[id] = this;
        }

        public virtual string Serialize(object obj)
        {
            return obj?.ToString();
        }

        public virtual object Deserialize(string str)
        {
            return null;
        }

        public virtual string GetName()
        {
            return "AbstractObject";
        }

        public virtual bool IsValidElement(object obj)
        {
            return false;
        }

        internal virtual object Parse(FileStream fs)
        {
            return null;
        }

        internal virtual void Write(object obj, byte[] data, int position)
        {
        }

        internal virtual void Write(object obj, FileStream fs)
        {
        }

        static NanoDBElement()
        {
            Elements = new ReadOnlyArray<NanoDBElement>(256);

            Bool = new BoolElement(0, 1);
            Byte = new ByteElement(1, 1);
            Short = new ShortElement(2, 2);
            Int = new IntElement(3, 4);
            Long = new LongElement(4, 8);
            String8 = new StringElement(32, 9);
            String16 = new StringElement(33, 17);
            String32 = new StringElement(34, 33);
            String64 = new StringElement(35, 65);
            String128 = new StringElement(36, 129);
            String256 = new StringElement(37, 257);
            DataBlob8 = new DataBlobElement(80, 9);
            DataBlob16 = new DataBlobElement(81, 17);
            DataBlob32 = new DataBlobElement(82, 33);
            DataBlob64 = new DataBlobElement(83, 65);
            DataBlob128 = new DataBlobElement(84, 129);
            DataBlob256 = new DataBlobElement(85, 257);
            DateTime = new DateTimeElement(128, 7);
        }
    }

    public class BoolElement : NanoDBElement
    {
        internal BoolElement(byte id, int size) : base(id, size)
        {
        }

        public override object Deserialize(string str)
        {
            return str != null && str.ToLower() == "true";
        }

        public override string GetName()
        {
            return "Boolean";
        }

        public override bool IsValidElement(object obj)
        {
            return obj is bool;
        }

        internal override object Parse(FileStream fs)
        {
            return fs.ReadByte() == 0x01;
        }

        internal override void Write(object obj, byte[] data, int position)
        {
            if ((bool)obj)
            {
                data[position] = 0x01;
            }
            else
            {
                data[position] = 0x00;
            }
        }

        internal override void Write(object obj, FileStream fs)
        {
            if ((bool)obj)
            {
                fs.WriteByte(0x01);
            }
            else
            {
                fs.WriteByte(0x00);
            }
        }
    }

    public class ByteElement : NanoDBElement
    {
        internal ByteElement(byte id, int size) : base(id, size)
        {
        }

        public override object Deserialize(string str)
        {
            byte result;

            return byte.TryParse(str, out result) ? result : (byte)0;
        }

        public override string GetName()
        {
            return "Byte";
        }

        public override bool IsValidElement(object obj)
        {
            return obj is byte;
        }

        internal override object Parse(FileStream fs)
        {
            return (byte)fs.ReadByte();
        }

        internal override void Write(object obj, byte[] data, int position)
        {
            data[position] = (byte)obj;
        }

        internal override void Write(object obj, FileStream fs)
        {
            fs.WriteByte((byte)obj);
        }
    }

    public class ShortElement : NanoDBElement
    {
        internal ShortElement(byte id, int size) : base(id, size)
        {
        }

        public override object Deserialize(string str)
        {
            short result;

            return short.TryParse(str, out result) ? result : (short)0;
        }

        public override string GetName()
        {
            return "Short";
        }

        public override bool IsValidElement(object obj)
        {
            return obj is short;
        }

        internal override object Parse(FileStream fs)
        {
            byte[] bytes = new byte[2];

            fs.Read(bytes, 0, bytes.Length);

            return (short)(bytes[0] << 8 | bytes[1]);
        }

        internal override void Write(object obj, byte[] data, int position)
        {
            short value = (short)obj;

            data[position] = (byte)(value >> 8);
            data[position + 1] = (byte)value;
        }

        internal override void Write(object obj, FileStream fs)
        {
            short value = (short)obj;
            byte[] data = { (byte)(value >> 8), (byte)value };

            fs.Write(data, 0, data.Length);
        }
    }

    public class IntElement : NanoDBElement
    {
        internal IntElement(byte id, int size) : base(id, size)
        {
        }

        public override object Deserialize(string str)
        {
            int result;

            return int.TryParse(str, out result) ? result : 0;
        }

        public override string GetName()
        {
            return "Integer";
        }

        public override bool IsValidElement(object obj)
        {
            return obj is int;
        }

        internal override object Parse(FileStream fs)
        {
            byte[] bytes = new byte[4];

            fs.Read(bytes, 0, bytes.Length);

            return bytes[0] << 24 | bytes[1] << 16 | bytes[2] << 8 | bytes[3];
        }

        internal override void Write(object obj, byte[] data, int position)
        {
            int value = (int)obj;

            data[position] = (byte)(value >> 24);
            data[position + 1] = (byte)(value >> 16);
            data[position + 2] = (byte)(value >> 8);
            data[position + 3] = (byte)value;
        }

        internal override void Write(object obj, FileStream fs)
        {
            int value = (int)obj;
            byte[] data = { (byte)(value >> 24), (byte)(value >> 16), (byte)(value >> 8), (byte)value };

            fs.Write(data, 0, data.Length);
        }
    }

    public class LongElement : NanoDBElement
    {
        internal LongElement(byte id, int size) : base(id, size)
        {
        }

        public override object Deserialize(string str)
        {
            long result;

            return long.TryParse(str, out result) ? result : 0L;
        }

        public override string GetName()
        {
            return "Long";
        }

        public override bool IsValidElement(object obj)
        {
            return obj is long;
        }

        internal override object Parse(FileStream fs)
        {
            byte[] bytes = new byte[8];

            fs.Read(bytes, 0, bytes.Length);

            return (long)bytes[0] << 56 | (long)bytes[1] << 48 | (long)bytes[2] << 40 | (long)bytes[3] << 32 | (long)bytes[4] << 24 | (long)bytes[5] << 16 | (long)bytes[6] << 8 | bytes[7];
        }

        internal override void Write(object obj, byte[] data, int position)
        {
            long value = (long)obj;

            data[position] = (byte)(value >> 56);
            data[position + 1] = (byte)(value >> 48);
            data[position + 2] = (byte)(value >> 40);
            data[position + 3] = (byte)(value >> 32);
            data[position + 4] = (byte)(value >> 24);
            data[position + 5] = (byte)(value >> 16);
            data[position + 6] = (byte)(value >> 8);
            data[position + 7] = (byte)value;
        }

        internal override void Write(object obj, FileStream fs)
        {
            long value = (long)obj;
            byte[] data = { (byte)(value >> 56), (byte)(value >> 48), (byte)(value >> 40), (byte)(value >> 32), (byte)(value >> 24), (byte)(value >> 16), (byte)(value >> 8), (byte)value };

            fs.Write(data, 0, data.Length);
        }
    }

    public class StringElement : NanoDBElement
    {
        internal StringElement(byte id, int size) : base(id, size)
        {
        }

        public override object Deserialize(string str)
        {
            return str;
        }

        public override string GetName()
        {
            return "String" + (this.Size - 1);
        }

        public override bool IsValidElement(object obj)
        {
            return obj is string str && Encoding.UTF8.GetByteCount(str) < this.Size;
        }

        internal override object Parse(FileStream fs)
        {
            int length = fs.ReadByte();

            if (length > 0 && length < this.Size)
            {
                byte[] bytes = new byte[length];

                int bytesRead = fs.Read(bytes, 0, length);
                int offset = this.Size - bytesRead - 1;

                if (offset > 0)
                {
                    fs.Seek(offset, SeekOrigin.Current);
                }

                return Encoding.UTF8.GetString(bytes);
            }

            fs.Seek(this.Size - 1, SeekOrigin.Current);

            return string.Empty;
        }

        internal override void Write(object obj, byte[] data, int position)
        {
            byte[] bytes = Encoding.UTF8.GetBytes((string)obj);

            data[position] = (byte)bytes.Length;

            for (int i = 0; i < bytes.Length; i++)
            {
                data[position + 1 + i] = bytes[i];
            }
        }

        internal override void Write(object obj, FileStream fs)
        {
            byte[] bytes = Encoding.UTF8.GetBytes((string)obj);
            int offset = this.Size - bytes.Length - 1;

            fs.WriteByte((byte)bytes.Length);
            fs.Write(bytes, 0, bytes.Length);

            if (offset > 0)
            {
                fs.Seek(offset, SeekOrigin.Current);
            }
        }
    }

    public class DataBlobElement : NanoDBElement
    {
        internal DataBlobElement(byte id, int size) : base(id, size)
        {
        }

        public override string Serialize(object obj)
        {
            return obj == null ? null : string.Join(" ", (byte[])obj);
        }

        public override object Deserialize(string str)
        {
            if (str != null)
            {
                string[] split = str.Split(' ', ';', ',', '-', ':');
                List<byte> values = new List<byte>();

                for (int i = 0; i < split.Length && i < 256; i++)
                {
                    byte parseTemp;

                    if (byte.TryParse(split[i], out parseTemp))
                    {
                        values.Add(parseTemp);
                    }
                }

                return values.ToArray();
            }

            return new byte[0];
        }

        public override string GetName()
        {
            return "DataBlob" + (this.Size - 1);
        }

        public override bool IsValidElement(object obj)
        {
            return obj is byte[] bytes && bytes.Length < this.Size;
        }

        internal override object Parse(FileStream fs)
        {
            int length = fs.ReadByte();

            if (length > 0 && length < this.Size)
            {
                byte[] values = new byte[length];

                int bytesRead = fs.Read(values, 0, length);
                int offset = this.Size - bytesRead - 1;

                if (offset > 0)
                {
                    fs.Seek(offset, SeekOrigin.Current);
                }

                return values;
            }

            fs.Seek(this.Size - 1, SeekOrigin.Current);

            return new byte[0];
        }

        internal override void Write(object obj, byte[] data, int position)
        {
            byte[] values = (byte[])obj;

            data[position] = (byte)values.Length;

            for (int i = 0; i < values.Length; i++)
            {
                data[position + i + 1] = values[i];
            }
        }

        internal override void Write(object obj, FileStream fs)
        {
            byte[] bytes = (byte[])obj;
            int offset = this.Size - bytes.Length - 1;

            fs.WriteByte((byte)bytes.Length);
            fs.Write(bytes, 0, bytes.Length);

            if (offset > 0)
            {
                fs.Seek(offset, SeekOrigin.Current);
            }
        }
    }

    public class DateTimeElement : NanoDBElement
    {
        internal DateTimeElement(byte id, int size) : base(id, size)
        {
        }

        public override string Serialize(object obj)
        {
            if (obj == null)
            {
                return null;
            }

            DateTime dt = (DateTime)obj;

            return dt.Year + "-" + dt.Month + "-" + dt.Day + " " + dt.Hour + ":" + (dt.Minute < 10 ? "0" : "") + dt.Minute + ":" + (dt.Second < 10 ? "0" : "") + dt.Second;
        }

        public override object Deserialize(string str)
        {
            if (str != null)
            {
                string[] splitBase = str.Split(' ');
                string[] splitDate = splitBase[0].Split('-', '.');

                if (splitDate.Length == 3)
                {
                    int hour = 0, minute = 0, second = 0;
                    int year, month, day;

                    if (int.TryParse(splitDate[0], out year) && int.TryParse(splitDate[1], out month) && int.TryParse(splitDate[2], out day))
                    {
                        if (splitBase.Length > 1)
                        {
                            string[] splitTime = splitBase[1].Split(':');

                            int.TryParse(splitTime[0], out hour);

                            if (splitTime.Length > 1)
                            {
                                int.TryParse(splitTime[1], out minute);
                            }

                            if (splitTime.Length > 2)
                            {
                                int.TryParse(splitTime[2], out second);
                            }
                        }

                        return new DateTime(year, month, day, hour, minute, second);
                    }
                }
            }

            return default(DateTime);
        }

        public override string GetName()
        {
            return "DateTime";
        }

        public override bool IsValidElement(object obj)
        {
            return obj is DateTime;
        }

        internal override object Parse(FileStream fs)
        {
            short year = (short)Short.Parse(fs);
            byte[] data = new byte[5];

            fs.Read(data, 0, data.Length);

            return new DateTime(year, data[0], data[1], data[2], data[3], data[4]);
        }

        internal override void Write(object obj, byte[] data, int position)
        {
            DateTime dt = (DateTime)obj;

            Short.Write((short)dt.Year, data, position);

            data[position + 2] = (byte)dt.Month;
            data[position + 3] = (byte)dt.Day;
            data[position + 4] = (byte)dt.Hour;
            data[position + 5] = (byte)dt.Minute;
            data[position + 6] = (byte)dt.Second;
        }

        internal override void Write(object obj, FileStream fs)
        {
            DateTime dt = (DateTime)obj;
            byte[] data = { (byte)dt.Month, (byte)dt.Day, (byte)dt.Hour, (byte)dt.Minute, (byte)dt.Second };

            Short.Write((short)dt.Year, fs);

            fs.Write(data, 0, data.Length);
        }
    }
}
