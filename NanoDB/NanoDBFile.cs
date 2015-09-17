using System.Collections.Generic;
using System.IO;

namespace domi1819.NanoDB
{
    public class NanoDBFile
    {
        public NanoDBLayout Layout { get; private set; }

        public NanoDBIndexAccess IndexAccess { get; private set; }

        public bool Accessible { get { return this.initialized && this.dbAccessStream != null; } }

        public int RecommendedIndex { get; private set; }

        public double StorageEfficiency { get { return (double)this.emptyLines / this.totalLines; } }

        private readonly string path;
        private bool initialized;

        private Dictionary<string, int> index;
        private int indexedBy;

        private int totalLines;
        private int emptyLines;

        private FileStream dbAccessStream;

        public NanoDBFile(string path)
        {
            this.path = path;
        }

        public bool Init()
        {
            using (FileStream fs = new FileStream(this.path, FileMode.OpenOrCreate, FileAccess.Read))
            {
                int layoutSize = fs.ReadByte();

                if (layoutSize > 0)
                {
                    this.RecommendedIndex = fs.ReadByte();

                    if (this.RecommendedIndex >= 0 && this.RecommendedIndex < layoutSize)
                    {
                        byte[] layoutIDs = new byte[layoutSize];

                        if (fs.Read(layoutIDs, 0, layoutSize) == layoutSize)
                        {
                            NanoDBElement[] elements = new NanoDBElement[layoutSize];
                            NanoDBElement element;

                            for (int i = 0; i < layoutSize; i++)
                            {
                                element = NanoDBElement.Elements[layoutIDs[i]];

                                if (element == null)
                                {
                                    return false;
                                }

                                elements[i] = element;
                            }

                            this.Layout = new NanoDBLayout(elements);

                            if ((fs.Length - this.Layout.HeaderSize) % this.Layout.RowSize == 0)
                            {
                                this.initialized = true;
                                return true;
                            }
                        }
                    }
                }
            }

            return false;
        }

        public bool CreateNew(NanoDBLayout layout, byte layoutIndex)
        {
            int layoutSize = layout.LayoutElements.Length;

            if (layoutSize > 0 && layoutSize < 256 && layoutIndex < layout.LayoutElements.Length)
            {
                using (FileStream fs = new FileStream(this.path, FileMode.OpenOrCreate, FileAccess.Write))
                {
                    fs.WriteByte((byte)layoutSize);
                    fs.WriteByte(layoutIndex);

                    byte[] layoutIds = new byte[layoutSize];

                    for (int i = 0; i < layoutSize; i++)
                    {
                        layoutIds[i] = layout.LayoutElements[i].Id;
                    }

                    fs.Write(layoutIds, 0, layoutSize);

                    this.Layout = layout;
                    this.index = new Dictionary<string, int>();
                    this.indexedBy = layoutIndex;
                    this.IndexAccess = new NanoDBIndexAccess(this.index);
                    this.initialized = true;

                    return true;
                }
            }

            return false;
        }

        public int IndexBy(int layoutIndex)
        {
            bool hasDuplicates = false;

            if (this.initialized)
            {
                if (layoutIndex >= 0 && layoutIndex < this.Layout.LayoutSize && this.Layout.LayoutElements[layoutIndex] is StringElement)
                {
                    StringElement indexElement = (StringElement)this.Layout.LayoutElements[layoutIndex];

                    this.index = new Dictionary<string, int>();

                    int leadingBytes = this.Layout.Offsets[layoutIndex];
                    int trailingBytes = this.Layout.RowSize - leadingBytes - indexElement.Size - 1;

                    using (FileStream fs = new FileStream(this.path, FileMode.Open, FileAccess.Read))
                    {
                        fs.Seek(this.Layout.HeaderSize, SeekOrigin.Current);

                        int lineFlag;
                        string identifier;

                        while (true)
                        {
                            lineFlag = fs.ReadByte();

                            if (lineFlag == NanoDBConstants.LineFlagActive)
                            {
                                if (leadingBytes > 0)
                                {
                                    fs.Seek(leadingBytes, SeekOrigin.Current);
                                }

                                identifier = (string)indexElement.Parse(fs);

                                if (this.index.ContainsKey(identifier))
                                {
                                    hasDuplicates = true;
                                }
                                else
                                {
                                    this.index[identifier] = this.totalLines;
                                }

                                if (trailingBytes > 0)
                                {
                                    fs.Seek(trailingBytes, SeekOrigin.Current);
                                }
                            }
                            else if (lineFlag == -1)
                            {
                                break;
                            }
                            else
                            {
                                fs.Seek(this.Layout.RowSize - 1, SeekOrigin.Current);

                                this.emptyLines++;
                            }

                            this.totalLines++;
                        }
                    }

                    this.indexedBy = layoutIndex;
                    this.IndexAccess = new NanoDBIndexAccess(this.index);

                    if (hasDuplicates)
                    {
                        return 1;
                    }

                    return 0;
                }
            }

            return -1;
        }

        public bool Bind()
        {
            if (this.initialized && this.dbAccessStream == null)
            {
                this.dbAccessStream = new FileStream(this.path, FileMode.Open, FileAccess.ReadWrite);

                return true;
            }

            return false;
        }

        public bool Unbind()
        {
            if (this.Accessible)
            {
                this.dbAccessStream.Close();
                this.dbAccessStream.Dispose();

                return true;
            }

            return false;
        }

        public bool AddLine(params object[] objects)
        {
            if (this.Accessible)
            {
                if (objects.Length == this.Layout.LayoutSize)
                {
                    byte[] data = new byte[this.Layout.RowSize];
                    data[0] = NanoDBConstants.LineFlagActive;

                    string key = objects[this.indexedBy] as string;

                    if (key != null && !this.index.ContainsKey(key))
                    {
                        int position = 1;
                        NanoDBElement element;

                        for (int i = 0; i < objects.Length; i++)
                        {
                            element = this.Layout.LayoutElements[i];

                            if (element.IsValidElement(objects[i]))
                            {
                                element.Write(objects[i], data, position);
                                position += element.Size;
                            }
                            else
                            {
                                return false;
                            }
                        }

                        this.index[key] = this.totalLines;
                        this.totalLines++;

                        this.dbAccessStream.Seek(0, SeekOrigin.End);
                        this.dbAccessStream.Write(data, 0, data.Length);

                        return true;
                    }
                }
            }

            return false;
        }

        public object[] GetLine(string key)
        {
            if (this.Accessible)
            {
                if (this.index.ContainsKey(key))
                {
                    object[] objects = new object[this.Layout.LayoutSize];

                    this.dbAccessStream.Seek(this.Layout.HeaderSize + (this.Layout.RowSize * this.index[key]) + 1, SeekOrigin.Begin);

                    for (int i = 0; i < objects.Length; i++)
                    {
                        objects[i] = this.Layout.LayoutElements[i].Parse(this.dbAccessStream);
                    }

                    return objects;
                }
            }

            return new object[] { };
        }

        public object GetObject(string key, int layoutIndex)
        {
            if (this.Accessible)
            {
                if (this.index.ContainsKey(key) && layoutIndex >= 0 && layoutIndex < this.Layout.LayoutSize)
                {
                    this.dbAccessStream.Seek(this.Layout.HeaderSize + (this.Layout.RowSize * this.index[key]) + 1 + this.Layout.Offsets[layoutIndex], SeekOrigin.Begin);

                    return this.Layout.LayoutElements[layoutIndex].Parse(this.dbAccessStream);
                }
            }

            return null;
        }

        public bool UpdateLine(string key, params object[] objects)
        {
            if (this.Accessible)
            {
                if (this.index.ContainsKey(key) && objects.Length == this.Layout.LayoutSize)
                {
                    bool keyUpdateFailed = false;
                    NanoDBElement element;

                    this.dbAccessStream.Seek(this.Layout.HeaderSize + (this.Layout.RowSize * this.index[key]) + 1, SeekOrigin.Begin);

                    for (int i = 0; i < objects.Length; i++)
                    {
                        element = this.Layout.LayoutElements[i];

                        if (element.IsValidElement(objects[i]))
                        {
                            if (i == this.indexedBy)
                            {
                                string newKey = (string)objects[i];

                                if (!this.index.ContainsKey(newKey))
                                {
                                    this.index[newKey] = this.index[key];
                                    this.index.Remove(key);

                                    element.Write(objects[i], this.dbAccessStream);
                                }
                                else
                                {
                                    keyUpdateFailed = true;
                                    this.dbAccessStream.Seek(element.Size, SeekOrigin.Current);
                                }
                            }
                            else
                            {
                                element.Write(objects[i], this.dbAccessStream);
                            }
                        }
                        else
                        {
                            this.dbAccessStream.Seek(element.Size, SeekOrigin.Current);
                        }
                    }

                    return !keyUpdateFailed;
                }
            }

            return false;
        }

        public bool RemoveLine(string key, bool allowRecycle = true)
        {
            if (this.Accessible)
            {
                if (this.index.ContainsKey(key))
                {
                    this.dbAccessStream.Seek(this.Layout.HeaderSize + this.index[key] * this.Layout.RowSize, SeekOrigin.Begin);

                    this.dbAccessStream.WriteByte(allowRecycle ? NanoDBConstants.LineFlagInactive : NanoDBConstants.LineFlagNoRecycle);

                    this.index.Remove(key);

                    return true;
                }
            }

            return false;
        }
    }
}
