using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace domi1819.NanoDB
{
    public class NanoDBFile
    {
        public NanoDBLayout Layout { get; private set; }

        public bool Accessible
        {
            get { return this.initialized && this.accessStream != null; }
        }

        public double StorageEfficiency
        {
            get { return (double)this.emptyLines / this.totalLines; }
        }

        public bool Sorted { get; private set; }
        public int RecommendedIndex { get; private set; }

        private readonly string path;
        private bool initialized;

        private Dictionary<string, NanoDBLine> contentIndex;
        private Dictionary<string, List<NanoDBLine>> sortIndex;

        private int indexedBy;
        private int sortedBy;

        private int totalLines;
        private int emptyLines;

        private FileStream accessStream;
        private readonly object accessLock = new object();

        public NanoDBFile(string path)
        {
            this.path = path;
        }

        public InitializeResult Initialize()
        {
            using (FileStream fs = new FileStream(this.path, FileMode.OpenOrCreate, FileAccess.Read))
            {
                int version = fs.ReadByte();

                if (version == NanoDBConstants.Version)
                {
                    int layoutSize = fs.ReadByte();

                    if (layoutSize > 0)
                    {
                        int recommendedIndex = fs.ReadByte();

                        if (recommendedIndex >= 0 && this.RecommendedIndex < layoutSize)
                        {
                            this.RecommendedIndex = recommendedIndex;

                            byte[] layoutIds = new byte[layoutSize];

                            if (fs.Read(layoutIds, 0, layoutSize) == layoutSize)
                            {
                                NanoDBElement[] elements = new NanoDBElement[layoutSize];

                                for (int i = 0; i < layoutSize; i++)
                                {
                                    NanoDBElement element = NanoDBElement.Elements[layoutIds[i]];

                                    if (element == null)
                                    {
                                        return InitializeResult.UnknownDataType;
                                    }

                                    elements[i] = element;
                                }

                                this.Layout = new NanoDBLayout(elements);

                                if ((fs.Length - this.Layout.HeaderSize) % this.Layout.RowSize == 0)
                                {
                                    this.initialized = true;
                                    return InitializeResult.Success;
                                }

                                return InitializeResult.FileCorrupt;
                            }
                        }
                    }

                    return InitializeResult.FileCorrupt;
                }

                return version >= 0 ? InitializeResult.VersionMismatch : InitializeResult.FileEmpty;
            }
        }

        public bool CreateNew(NanoDBLayout layout, byte indexBy, int sortBy = -1)
        {
            int layoutSize = layout.LayoutElements.Length;

            if (layoutSize > 0 && layoutSize < 256 && indexBy < layout.LayoutElements.Length)
            {
                using (FileStream fs = new FileStream(this.path, FileMode.Create, FileAccess.Write))
                {
                    fs.WriteByte((byte)NanoDBConstants.Version);
                    fs.WriteByte((byte)layoutSize);
                    fs.WriteByte(indexBy);

                    byte[] layoutIds = new byte[layoutSize];

                    for (int i = 0; i < layoutSize; i++)
                    {
                        layoutIds[i] = layout.LayoutElements[i].Id;
                    }

                    fs.Write(layoutIds, 0, layoutSize);

                    fs.WriteByte(0x00);
                    fs.WriteByte(NanoDBConstants.LineFlagBackup);

                    for (int i = 0; i < layout.RowSize - 1; i++)
                    {
                        fs.WriteByte(0x00);
                    }

                    this.Layout = layout;
                    this.contentIndex = new Dictionary<string, NanoDBLine>();
                    this.indexedBy = indexBy;
                    this.initialized = true;

                    if (sortBy >= 0 && sortBy < layoutSize && sortBy != indexBy)
                    {
                        this.sortIndex = new Dictionary<string, List<NanoDBLine>>();

                        this.sortedBy = sortBy;
                        this.Sorted = true;
                    }

                    return true;
                }
            }

            return false;
        }

        public LoadResult Load(int indexBy, int sortBy = -1)
        {
            if (this.initialized)
            {
                if (indexBy >= 0 && indexBy < this.Layout.LayoutSize && this.Layout.LayoutElements[indexBy] is StringElement)
                {
                    bool sort = sortBy >= 0 && sortBy < this.Layout.LayoutSize && sortBy != indexBy && this.Layout.LayoutElements[sortBy] is StringElement;

                    this.contentIndex = new Dictionary<string, NanoDBLine>();
                    this.sortIndex = sort ? new Dictionary<string, List<NanoDBLine>>() : null;

                    using (FileStream fs = new FileStream(this.path, FileMode.Open, FileAccess.Read))
                    {
                        bool hasDuplicates = false;

                        fs.Seek(this.Layout.HeaderSize, SeekOrigin.Current);

                        while (true)
                        {
                            int lineFlag = fs.ReadByte();

                            if (lineFlag == NanoDBConstants.LineFlagActive)
                            {
                                object[] objects = new object[this.Layout.LayoutSize];

                                for (int i = 0; i < objects.Length; i++)
                                {
                                    objects[i] = this.Layout.LayoutElements[i].Parse(fs);
                                }

                                string key = (string)objects[indexBy];
                                string sortKey = sort ? (string)objects[sortBy] : null;

                                if (this.contentIndex.ContainsKey(key))
                                {
                                    hasDuplicates = true;
                                }
                                else
                                {
                                    NanoDBLine line = new NanoDBLine(this, NanoDBConstants.LineFlagActive, this.totalLines, key, objects);

                                    this.contentIndex[key] = line;

                                    if (sort)
                                    {
                                        if (this.sortIndex.ContainsKey(sortKey))
                                        {
                                            this.sortIndex[sortKey].Add(line);
                                        }
                                        else
                                        {
                                            this.sortIndex[sortKey] = new List<NanoDBLine> { line };
                                        }
                                    }
                                }
                            }
                            else if (lineFlag == -1)
                            {
                                // End of file
                                break;
                            }
                            else
                            {
                                fs.Seek(this.Layout.RowSize - 1, SeekOrigin.Current);

                                this.emptyLines++;
                            }

                            this.totalLines++;
                        }

                        this.indexedBy = indexBy;
                        this.sortedBy = sortBy;
                        this.Sorted = sort;

                        return hasDuplicates ? LoadResult.HasDuplicates : LoadResult.Okay;
                    }
                }

                return LoadResult.NotIndexable;
            }

            return LoadResult.GenericFailed;
        }

        public bool Bind()
        {
            if (this.initialized && this.accessStream == null)
            {
                lock (this.accessLock)
                {
                    this.accessStream = new FileStream(this.path, FileMode.Open, FileAccess.ReadWrite);
                }

                return true;
            }

            return false;
        }

        public bool Unbind()
        {
            if (this.Accessible)
            {
                lock (this.accessLock)
                {
                    this.accessStream.Close();
                    this.accessStream.Dispose();
                }

                return true;
            }

            return false;
        }

        public NanoDBLine AddLine(params object[] objects)
        {
            if (this.Accessible && objects.Length == this.Layout.LayoutSize)
            {
                lock (this.accessLock)
                {
                    string key = objects[this.indexedBy] as string;

                    if (key != null && !this.contentIndex.ContainsKey(key))
                    {
                        int position = 1;
                        byte[] data = new byte[this.Layout.RowSize];

                        data[0] = NanoDBConstants.LineFlagIncomplete;

                        for (int i = 0; i < objects.Length; i++)
                        {
                            NanoDBElement element = this.Layout.LayoutElements[i];

                            if (element.IsValidElement(objects[i]))
                            {
                                element.Write(objects[i], data, position);
                                position += element.Size;
                            }
                            else
                            {
                                return null;
                            }
                        }

                        NanoDBLine line = new NanoDBLine(this, NanoDBConstants.LineFlagActive, this.totalLines, key, objects);

                        this.totalLines++;
                        this.contentIndex[key] = line;

                        if (this.Sorted)
                        {
                            string sortKey = (string)objects[this.sortedBy];

                            if (this.sortIndex.ContainsKey(sortKey))
                            {
                                this.sortIndex[sortKey].Add(line);
                            }
                            else
                            {
                                this.sortIndex[sortKey] = new List<NanoDBLine> { line };
                            }
                        }

                        this.accessStream.Seek(0, SeekOrigin.End);

                        long lineLocation = this.accessStream.Position;

                        this.accessStream.SetLength(this.accessStream.Length + this.Layout.RowSize);
                        this.accessStream.Write(data, 0, data.Length);
                        this.accessStream.Seek(lineLocation, SeekOrigin.Begin);
                        this.accessStream.WriteByte(NanoDBConstants.LineFlagActive);

                        return line;
                    }
                }
            }

            return null;
        }

        public NanoDBLine GetLine(string key)
        {
            if (this.Accessible)
            {
                lock (this.accessLock)
                {
                    if (this.contentIndex.ContainsKey(key))
                    {
                        return this.contentIndex[key];
                    }
                }
            }

            return null;
        }

        public bool UpdateLine(NanoDBLine line, params object[] objects)
        {
            if (this.Accessible)
            {
                lock (this.accessLock)
                {
                    string key = line.Key;

                    if (this.contentIndex.ContainsKey(key) && objects.Length == this.Layout.LayoutSize)
                    {
                        bool keyUpdateSuccess = false;
                        long linePosition = this.Layout.HeaderSize + ((long)this.Layout.RowSize * line.LineNumber);

                        this.BackupLine(linePosition);

                        this.accessStream.Seek(linePosition, SeekOrigin.Begin);
                        this.accessStream.WriteByte(NanoDBConstants.LineFlagCorrupt);

                        for (int i = 0; i < objects.Length; i++)
                        {
                            NanoDBElement element = this.Layout.LayoutElements[i];

                            if (element.IsValidElement(objects[i]))
                            {
                                if (i == this.indexedBy)
                                {
                                    string newKey = (string)objects[i];

                                    if (this.contentIndex.ContainsKey(newKey))
                                    {
                                        // TODO
                                        this.accessStream.Seek(element.Size, SeekOrigin.Current);
                                    }
                                    else
                                    {
                                        this.contentIndex[newKey] = line;
                                        this.contentIndex.Remove(line.Key);
                                        line.Key = newKey;

                                        element.Write(objects[i], this.accessStream);
                                        line.Content[i] = objects[i];

                                        keyUpdateSuccess = true;
                                    }
                                }
                                else if (this.Sorted && i == this.sortedBy)
                                {
                                    string newSortKey = (string)objects[i];

                                    if (this.sortIndex.ContainsKey(newSortKey))
                                    {
                                        this.sortIndex[newSortKey].Add(line);
                                    }
                                    else
                                    {
                                        this.sortIndex[newSortKey] = new List<NanoDBLine> { line };
                                    }

                                    line.SortKey = newSortKey;
                                    line.Content[i] = objects[i];
                                }
                                else
                                {
                                    element.Write(objects[i], this.accessStream);

                                    line.Content[i] = objects[i];
                                }
                            }
                            else
                            {
                                // TODO: More detailed output
                                this.accessStream.Seek(element.Size, SeekOrigin.Current);
                            }
                        }

                        this.accessStream.Seek(linePosition, SeekOrigin.Begin);
                        this.accessStream.WriteByte(NanoDBConstants.LineFlagActive);

                        return keyUpdateSuccess;
                    }
                }
            }

            return false;
        }

        public bool UpdateObject(NanoDBLine line, int layoutIndex, object obj)
        {
            if (this.Accessible)
            {
                lock (this.accessLock)
                {
                    string key = line.Key;

                    if (this.contentIndex.ContainsKey(key) && layoutIndex >= 0 && layoutIndex < this.Layout.LayoutSize)
                    {
                        NanoDBElement element = this.Layout.LayoutElements[layoutIndex];

                        if (element.IsValidElement(obj))
                        {
                            long linePosition = this.Layout.HeaderSize + ((long)this.Layout.RowSize * line.LineNumber);
                            long elementPosition = linePosition + 1 + this.Layout.Offsets[layoutIndex];

                            if (layoutIndex == this.indexedBy)
                            {
                                string newKey = (string)obj;

                                if (this.contentIndex.ContainsKey(newKey))
                                {
                                    // TODO: More detailed output
                                    return false;
                                }

                                this.contentIndex[newKey] = line;
                                this.contentIndex.Remove(key);
                                line.Key = newKey;
                            }
                            else
                            {
                                if (this.Sorted && layoutIndex == this.sortedBy)
                                {
                                    string newSortKey = (string)obj;

                                    if (this.sortIndex.ContainsKey(newSortKey))
                                    {
                                        this.sortIndex[newSortKey].Add(line);
                                    }
                                    else
                                    {
                                        this.sortIndex[newSortKey] = new List<NanoDBLine> { line };
                                    }

                                    line.SortKey = newSortKey;
                                }
                            }

                            element.Write(obj, this.accessStream);
                            line.Content[layoutIndex] = obj;

                            this.BackupObject(elementPosition, layoutIndex);

                            this.accessStream.Seek(linePosition, SeekOrigin.Begin);
                            this.accessStream.WriteByte(NanoDBConstants.LineFlagCorrupt);

                            this.accessStream.Seek(elementPosition, SeekOrigin.Begin);

                            this.accessStream.Seek(linePosition, SeekOrigin.Begin);
                            this.accessStream.WriteByte(NanoDBConstants.LineFlagActive);

                            return true;
                        }
                    }
                }
            }

            return false;
        }

        public bool RemoveLine(NanoDBLine line, bool allowRecycle = true)
        {
            if (this.Accessible)
            {
                lock (this.accessLock)
                {
                    string key = line.Key;

                    if (this.contentIndex.ContainsKey(key))
                    {
                        byte lineFlag = allowRecycle ? NanoDBConstants.LineFlagInactive : NanoDBConstants.LineFlagNoRecycle;

                        this.accessStream.Seek(this.Layout.HeaderSize + this.contentIndex[key].LineNumber * this.Layout.RowSize, SeekOrigin.Begin);
                        this.accessStream.WriteByte(lineFlag);

                        this.contentIndex.Remove(key);

                        if (this.Sorted)
                        {
                            string sortKey = line.SortKey;

                            if (this.sortIndex.ContainsKey(sortKey))
                            {
                                this.sortIndex[sortKey].Remove(line);
                            }
                        }

                        line.LineFlag = lineFlag;

                        return true;
                    }
                }
            }

            return false;
        }

        public List<string> GetAllKeys()
        {
            return this.contentIndex.Keys.ToList();
        }

        public bool ContainsKey(string key)
        {
            return this.contentIndex.ContainsKey(key);
        }

        public List<NanoDBLine> GetSortedLines(string sortKey)
        {
            return this.Sorted ? this.sortIndex.ContainsKey(sortKey) ? new List<NanoDBLine>(this.sortIndex[sortKey]) : new List<NanoDBLine>() : null;
        }

        private void BackupLine(long position)
        {
            byte[] data = new byte[this.Layout.RowSize - 1];

            this.accessStream.Seek(position + 1, SeekOrigin.Begin);
            this.accessStream.Read(data, 0, data.Length);

            this.accessStream.Seek(this.Layout.HeaderSize - this.Layout.RowSize, SeekOrigin.Begin);
            this.accessStream.WriteByte(NanoDBConstants.LineFlagBackup);
            this.accessStream.Write(data, 0, data.Length);
        }

        private void BackupObject(long position, int layoutIndex)
        {
            byte[] data = new byte[this.Layout.LayoutElements[layoutIndex].Size];

            this.accessStream.Seek(position, SeekOrigin.Begin);
            this.accessStream.Read(data, 0, data.Length);

            this.accessStream.Seek(this.Layout.HeaderSize - 1 - this.Layout.RowSize, SeekOrigin.Begin);
            this.accessStream.WriteByte((byte)layoutIndex);
            this.accessStream.WriteByte(NanoDBConstants.LineFlagBackupObject);
            this.accessStream.Write(data, 0, data.Length);
        }
    }
}
