using System;
using System.IO;
using System.Text;

namespace domi1819.NanoDB
{
    public abstract class NanoDBElement
    {
        public byte Id { get; private set; }
        public int Size { get; private set; }

        public static NanoDBElement[] Elements { get; set; }

        public static BoolElement Bool { get; private set; }
        public static ByteElement Byte { get; private set; }
        public static ShortElement Short { get; private set; }
        public static IntElement Int { get; private set; }
        public static LongElement Long { get; private set; }
        public static StringElement String8 { get; private set; }
        public static StringElement String16 { get; private set; }
        public static StringElement String32 { get; private set; }
        public static StringElement String64 { get; private set; }
        public static StringElement String128 { get; private set; }
        public static StringElement String256 { get; private set; }
        public static DateTimeElement DateTime { get; private set; }

        internal NanoDBElement(byte id, int size)
        {
            this.Id = id;
            this.Size = size;

            Elements[id] = this;
        }

        internal virtual bool IsValidElement(object obj)
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

        public virtual object Deserialize(string str)
        {
            return null;
        }

        public virtual string Serialize(object obj)
        {
            return obj == null ? null : obj.ToString();
        }

        public virtual string GetName()
        {
            return "AbstractObject";
        }

        static NanoDBElement()
        {
            Elements = new NanoDBElement[256];

            Bool = new BoolElement();
            Byte = new ByteElement();
            Short = new ShortElement();
            Int = new IntElement();
            Long = new LongElement();
            String8 = new StringElement(32, 9);
            String16 = new StringElement(33, 17);
            String32 = new StringElement(34, 33);
            String64 = new StringElement(35, 65);
            String128 = new StringElement(36, 129);
            String256 = new StringElement(37, 257);
            DateTime = new DateTimeElement();
        }
    }

    public class BoolElement : NanoDBElement
    {
        internal BoolElement()
            : base(0, 1)
        {
        }

        internal override bool IsValidElement(object obj)
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

        public override object Deserialize(string str)
        {
            return str != null && str.ToLower() == "true";
        }

        public override string GetName()
        {
            return "Boolean";
        }
    }

    public class ByteElement : NanoDBElement
    {
        internal ByteElement()
            : base(1, 1)
        {
        }

        internal override bool IsValidElement(object obj)
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

        public override object Deserialize(string str)
        {
            byte result;

            if (byte.TryParse(str, out result))
            {
                return result;
            }

            return (byte)0;
        }

        public override string GetName()
        {
            return "Byte";
        }
    }

    public class ShortElement : NanoDBElement
    {
        internal ShortElement()
            : base(2, 2)
        {
        }

        internal override bool IsValidElement(object obj)
        {
            return obj is short;
        }

        #pragma warning disable 675
        internal override object Parse(FileStream fs)
        {
            byte[] bData = new byte[2];

            fs.Read(bData, 0, bData.Length);

            short[] data = { bData[0], bData[1] };

            return (short)(data[0] << 8 | data[1]);
        }
        #pragma warning restore 675

        internal override void Write(object obj, byte[] data, int position)
        {
            short s = (short)obj;

            data[position] = (byte)(s >> 8);
            data[position + 1] = (byte)s;
        }

        internal override void Write(object obj, FileStream fs)
        {
            short s = (short)obj;

            byte[] data = { (byte)(s >> 8), (byte)s };

            fs.Write(data, 0, data.Length);
        }

        public override object Deserialize(string str)
        {
            short result;

            if (short.TryParse(str, out result))
            {
                return result;
            }

            return (short)0;
        }

        public override string GetName()
        {
            return "Short";
        }
    }

    public class IntElement : NanoDBElement
    {
        internal IntElement()
            : base(3, 4)
        {
        }

        internal override bool IsValidElement(object obj)
        {
            return obj is int;
        }
        
        internal override object Parse(FileStream fs)
        {
            byte[] bData = new byte[4];

            fs.Read(bData, 0, bData.Length);

            int[] data = { bData[0], bData[1], bData[2], bData[3] };

            return data[0] << 24 | data[1] << 16 | data[2] << 8 | data[3];
        }

        internal override void Write(object obj, byte[] data, int position)
        {
            int i = (int)obj;

            data[position] = (byte)(i >> 24);
            data[position + 1] = (byte)(i >> 16);
            data[position + 2] = (byte)(i >> 8);
            data[position + 3] = (byte)i;
        }

        internal override void Write(object obj, FileStream fs)
        {
            int i = (int)obj;

            byte[] data = { (byte)(i >> 24), (byte)(i >> 16), (byte)(i >> 8), (byte)i };

            fs.Write(data, 0, data.Length);
        }

        public override object Deserialize(string str)
        {
            int result;

            if (int.TryParse(str, out result))
            {
                return result;
            }

            return 0;
        }

        public override string GetName()
        {
            return "Integer";
        }
    }

    public class LongElement : NanoDBElement
    {
        internal LongElement()
            : base(4, 8)
        {
        }

        internal override bool IsValidElement(object obj)
        {
            return obj is long;
        }

        internal override object Parse(FileStream fs)
        {
            byte[] bData = new byte[8];

            fs.Read(bData, 0, bData.Length);

            long[] data = { bData[0], bData[1], bData[2], bData[3], bData[4], bData[5], bData[6], bData[7] };

            return data[0] << 56 | data[1] << 48 | data[2] << 40 | data[3] << 32 | data[4] << 24 | data[5] << 16 | data[6] << 8 | data[7];
        }

        internal override void Write(object obj, byte[] data, int position)
        {
            long l = (long)obj;

            data[position] = (byte)(l >> 56);
            data[position + 1] = (byte)(l >> 48);
            data[position + 2] = (byte)(l >> 40);
            data[position + 3] = (byte)(l >> 32);
            data[position + 4] = (byte)(l >> 24);
            data[position + 5] = (byte)(l >> 16);
            data[position + 6] = (byte)(l >> 8);
            data[position + 7] = (byte)l;
        }

        internal override void Write(object obj, FileStream fs)
        {
            long l = (long)obj;

            byte[] data = { (byte)(l >> 56), (byte)(l >> 48), (byte)(l >> 40), (byte)(l >> 32), (byte)(l >> 24), (byte)(l >> 16), (byte)(l >> 8), (byte)l };

            fs.Write(data, 0, data.Length);
        }

        public override object Deserialize(string str)
        {
            long result;

            if (long.TryParse(str, out result))
            {
                return result;
            }

            return 0L;
        }

        public override string GetName()
        {
            return "Long";
        }
    }

    public class StringElement : NanoDBElement
    {
        internal StringElement(byte id, int size)
            : base(id, size)
        {
        }

        internal override bool IsValidElement(object obj)
        {
            return obj is string && Encoding.UTF8.GetByteCount((string)obj) <= this.Size - 1;
        }

        internal override object Parse(FileStream fs)
        {
            int length = fs.ReadByte();

            if (length > 0)
            {
                byte[] data = new byte[length];

                int bytesRead = fs.Read(data, 0, length);
                int offset = this.Size - bytesRead - 1;

                if (offset > 0)
                {
                    fs.Seek(offset, SeekOrigin.Current);
                }

                return Encoding.UTF8.GetString(data);
            }

            fs.Seek(this.Size - 1, SeekOrigin.Current);

            return string.Empty;
        }

        internal override void Write(object obj, byte[] data, int position)
        {
            string str = (string)obj;

            byte[] bytes = Encoding.UTF8.GetBytes(str);

            for(int i = 0; i < bytes.Length; i++)
            {
                data[position + 1 + i] = bytes[i];
            }

            data[position] = (byte)bytes.Length;
        }

        internal override void Write(object obj, FileStream fs)
        {
            string str = (string)obj;
            byte[] data = Encoding.UTF8.GetBytes(str);

            fs.WriteByte((byte)data.Length);
            fs.Write(data, 0, data.Length);

            int offset = this.Size - data.Length - 1;

            if (offset > 0)
            {
                fs.Seek(offset, SeekOrigin.Current);
            }
        }

        public override object Deserialize(string str)
        {
            return str;
        }

        public override string GetName()
        {
            return "String" + (this.Size - 1);
        }
    }

    public class DateTimeElement : NanoDBElement
    {
        internal DateTimeElement()
            : base(128, 7)
        {
        }

        internal override bool IsValidElement(object obj)
        {
            return obj is DateTime;
        }

        internal override object Parse(FileStream fs)
        {
            short year = (short)NanoDBElement.Short.Parse(fs);

            byte[] data = new byte[5];

            fs.Read(data, 0, data.Length);

            return new DateTime(year, data[0], data[1], data[2], data[3], data[4]);
        }

        internal override void Write(object obj, byte[] data, int position)
        {
            DateTime dt = (DateTime)obj;

            NanoDBElement.Short.Write((short)dt.Year, data, position);

            data[position + 2] = (byte)dt.Month;
            data[position + 3] = (byte)dt.Day;
            data[position + 4] = (byte)dt.Hour;
            data[position + 5] = (byte)dt.Minute;
            data[position + 6] = (byte)dt.Second;
        }

        internal override void Write(object obj, FileStream fs)
        {
            DateTime dt = (DateTime)obj;

            NanoDBElement.Short.Write((short)dt.Year, fs);

            byte[] data = { (byte)dt.Month, (byte)dt.Day, (byte)dt.Hour, (byte)dt.Minute, (byte)dt.Second };

            fs.Write(data, 0, data.Length);
        }

        public override object Deserialize(string str)
        {
            if (str != null)
            {
                string[] splitBase = str.Split(' ');

                int year, month, day;
                int hour = 0, minute = 0, second = 0;

                string[] splitDate = splitBase[0].Split('-', '.');

                if (splitDate.Length == 3)
                {
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

        public override string Serialize(object obj)
        {
            if (obj == null)
            {
                return null;
            }

            DateTime dt = (DateTime)obj;
            return dt.Year + "-" + dt.Month + "-" + dt.Day + " " + dt.Hour + ":" + (dt.Minute < 10 ? "0" : "") + dt.Minute + ":" + (dt.Second < 10 ? "0" : "") + dt.Second;
        }

        public override string GetName()
        {
            return "DateTime";
        }
    }
}
