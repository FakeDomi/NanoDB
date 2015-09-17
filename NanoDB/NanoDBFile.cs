using System.Collections.Generic;
using System.IO;

namespace domi1819.NanoDB
{
    public class NanoDBFile
    {
        public NanoDBLayout Layout { get; private set; }

        public bool Accessible { get { return this.initialized && this.dbAccessStream != null; } set { } }

        public int RecommendedIndex { get; private set; }

        private string path;
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
                    RecommendedIndex = fs.ReadByte();

                    if (RecommendedIndex >= 0 && RecommendedIndex < layoutSize)
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

            if (layoutSize > 0 && layoutSize < 256 && layoutIndex >= 0 && layoutIndex < layout.LayoutElements.Length)
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
                    index = new Dictionary<string, int>();
                    initialized = true;

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

                    index = new Dictionary<string, int>();

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

                                if (index.ContainsKey(identifier))
                                {
                                    hasDuplicates = true;
                                }
                                else
                                {
                                    index[identifier] = totalLines;
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

                            totalLines++;
                        }
                    }

                    this.indexedBy = layoutIndex;

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

                    string key = objects[indexedBy] as string;

                    if (key != null && !index.ContainsKey(key))
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

                        dbAccessStream.Seek(0, SeekOrigin.End);
                        dbAccessStream.Write(data, 0, data.Length);

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
                if (index.ContainsKey(key) && objects.Length == this.Layout.LayoutSize)
                {
                    bool keyUpdateFailed = false;
                    NanoDBElement element;

                    this.dbAccessStream.Seek(this.Layout.HeaderSize + (this.Layout.RowSize * index[key]) + 1, SeekOrigin.Begin);

                    for (int i = 0; i < objects.Length; i++)
                    {
                        element = this.Layout.LayoutElements[i];

                        if (element.IsValidElement(objects[i]))
                        {
                            if (i == this.indexedBy)
                            {
                                string newKey = (string)objects[i];

                                if (!index.ContainsKey(newKey))
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
                if (index.ContainsKey(key))
                {
                    dbAccessStream.Seek(this.Layout.HeaderSize + index[key] * this.Layout.RowSize, SeekOrigin.Begin);
                    
                    if (allowRecycle)
                    {
                        dbAccessStream.WriteByte(NanoDBConstants.LineFlagInactive);
                    }
                    else
                    {
                        dbAccessStream.WriteByte(NanoDBConstants.LineFlagNoRecycle);
                    }

                    index.Remove(key);

                    return true;
                }
            }

            return false;
        }
    }
}
