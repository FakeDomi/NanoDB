using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace domi1819.NanoDB
{
    public class NanoDBFile
    {
        public NanoDBLayout Layout { get; private set; }

        public bool Accessible
        {
            get { return this.initialized && this.AccessStream != null; }
        }

        public double StorageEfficiency
        {
            get { return (double)(this.TotalLines - this.EmptyLines) / this.TotalLines; }
        }

        public int TotalLines { get; private set; }
        public int EmptyLines { get; private set; }

        public bool Sorted { get; private set; }
        public int RecommendedIndex { get; private set; }

        public string Path { get; private set; }

        internal int RunningTasks;
        internal FileStream AccessStream;
        
        private bool initialized;
        private bool shutdown;

        private NanoDBWriter writer;

        private Dictionary<string, NanoDBLine> contentIndex;
        private Dictionary<string, List<NanoDBLine>> sortIndex;

        private int indexedBy;
        private int sortedBy;

        private readonly object readLock = new object();
        private readonly object fileLock = new object();

        public NanoDBFile(string path)
        {
            this.Path = path;
        }

        public InitializeResult Initialize()
        {
            using (FileStream stream = new FileStream(this.Path, FileMode.OpenOrCreate, FileAccess.Read))
            {
                byte[] magicBytes = new byte[NanoDBConstants.MagicBytes.Length];

                if (stream.Read(magicBytes, 0, NanoDBConstants.MagicBytes.Length) != NanoDBConstants.MagicBytes.Length)
                {
                    return InitializeResult.FileEmpty;
                }

                int version = stream.ReadByte();

                if (version != NanoDBConstants.DatabaseStructureVersion)
                {
                    return version > 0 ? InitializeResult.VersionMismatch : InitializeResult.FileEmpty;
                }

                int layoutSize = stream.ReadByte();

                if (layoutSize <= 0)
                {
                    return InitializeResult.FileCorrupt;
                }

                int recommendedIndex = stream.ReadByte();

                if (recommendedIndex < 0 || recommendedIndex >= layoutSize)
                {
                    return InitializeResult.FileCorrupt;
                }

                this.RecommendedIndex = recommendedIndex;

                byte[] layoutIds = new byte[layoutSize];

                if (stream.Read(layoutIds, 0, layoutSize) < layoutSize)
                {
                    return InitializeResult.FileCorrupt;
                }

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

                if ((stream.Length - this.Layout.HeaderSize) % this.Layout.RowSize != 0)
                {
                    return InitializeResult.UnexpectedFileEnd;
                }

                this.initialized = true;

                return InitializeResult.Success;
            }
        }

        public bool CreateNew(NanoDBLayout layout, int indexBy, int sortBy = -1)
        {
            int layoutSize = layout.Elements.Length;

            if (layoutSize <= 0 || layoutSize > 255 || indexBy >= layout.Elements.Length)
            {
                return false;
            }

            using (FileStream stream = new FileStream(this.Path, FileMode.Create, FileAccess.Write))
            {
                stream.Write(NanoDBConstants.MagicBytes.GetArray(), 0, NanoDBConstants.MagicBytes.Length);

                stream.WriteByte((byte)NanoDBConstants.DatabaseStructureVersion);
                stream.WriteByte((byte)layoutSize);
                stream.WriteByte((byte)indexBy);

                byte[] layoutIds = new byte[layoutSize];

                for (int i = 0; i < layoutSize; i++)
                {
                    layoutIds[i] = layout.Elements[i].Id;
                }

                stream.Write(layoutIds, 0, layoutSize);

                stream.WriteByte(0x00);
                stream.WriteByte(NanoDBConstants.LineFlagBackup);

                for (int i = 0; i < layout.RowSize - 1; i++)
                {
                    stream.WriteByte(0x00);
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
            }

            return true;
        }

        public LoadResult Load(int indexBy, int sortBy = -1)
        {
            if (!this.initialized)
            {
                return LoadResult.NotIndexable;
            }

            if (indexBy < 0 || indexBy >= this.Layout.LayoutSize || !(this.Layout.Elements[indexBy] is StringElement))
            {
                return LoadResult.NotIndexable;
            }

            bool sort = sortBy >= 0 && sortBy < this.Layout.LayoutSize && sortBy != indexBy && this.Layout.Elements[sortBy] is StringElement;
            bool hasDuplicates = false;

            this.contentIndex = new Dictionary<string, NanoDBLine>();
            this.sortIndex = sort ? new Dictionary<string, List<NanoDBLine>>() : null;

            using (FileStream stream = new FileStream(this.Path, FileMode.Open, FileAccess.Read))
            {
                stream.Seek(this.Layout.HeaderSize, SeekOrigin.Begin);

                int lineFlag = stream.ReadByte();

                while (lineFlag != -1)
                {
                    if (lineFlag == NanoDBConstants.LineFlagActive)
                    {
                        object[] objects = new object[this.Layout.LayoutSize];

                        for (int i = 0; i < objects.Length; i++)
                        {
                            objects[i] = this.Layout.Elements[i].Parse(stream);
                        }

                        string key = (string)objects[indexBy];
                        string sortKey = sort ? (string)objects[sortBy] : null;

                        if (this.contentIndex.ContainsKey(key))
                        {
                            hasDuplicates = true;
                        }
                        else
                        {
                            NanoDBLine line = new NanoDBLine(this, NanoDBConstants.LineFlagActive, this.TotalLines, key, objects, sortKey);

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
                    else
                    {
                        stream.Seek(this.Layout.RowSize - 1, SeekOrigin.Current);

                        this.EmptyLines++;
                    }

                    lineFlag = stream.ReadByte();
                    this.TotalLines++;
                }
            }

            this.indexedBy = indexBy;
            this.sortedBy = sortBy;
            this.Sorted = sort;

            return hasDuplicates ? LoadResult.HasDuplicates : LoadResult.Okay;
        }

        public bool Bind()
        {
            if (!this.initialized || this.AccessStream != null)
            {
                return false;
            }

            lock (this.readLock)
            lock (this.fileLock)
            {
                this.AccessStream = new FileStream(this.Path, FileMode.Open, FileAccess.ReadWrite);
                this.writer = new NanoDBWriter(this);
            }

            return true;
        }

        public bool Unbind()
        {
            if (!this.Accessible)
            {
                return false;
            }

            this.shutdown = true;
            this.writer.Shutdown();

            int tries = 0;

            while (this.RunningTasks > 0 && tries < 50)
            {
                Thread.Sleep(100);
                tries++;
            }

            lock (this.readLock)
            lock (this.fileLock)
            {
                this.AccessStream.Close();
                this.AccessStream.Dispose();
            }

            return true;
        }

        public NanoDBLine AddLine(params object[] objects)
        {
            string key;

            lock (this.readLock)
            {
                if (!this.Accessible || objects.Length != this.Layout.LayoutSize || this.shutdown)
                {
                    return null;
                }

                this.RunningTasks++;

                key = (string)objects[this.indexedBy];

                if (key == null || this.contentIndex.ContainsKey(key))
                {
                    this.RunningTasks--;
                    return null;
                }
            }

            int position = 1;
            byte[] data = new byte[this.Layout.RowSize];

            data[0] = NanoDBConstants.LineFlagIncomplete;

            for (int i = 0; i < objects.Length; i++)
            {
                NanoDBElement element = this.Layout.Elements[i];

                if (element.IsValidElement(objects[i]))
                {
                    element.Write(objects[i], data, position);
                    position += element.Size;
                }
                else
                {
                    this.RunningTasks--;
                    return null;
                }
            }

            NanoDBLine line = new NanoDBLine(this, NanoDBConstants.LineFlagActive, this.TotalLines, key, objects);

            lock (this.readLock)
            {
                if (!this.Accessible || this.contentIndex.ContainsKey(key))
                {
                    this.RunningTasks--;
                    return null;
                }

                this.TotalLines++;
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

                this.writer.AddTask(new NanoDBTask { Type = TaskType.AddLine, Data = data });
            }

            return line;
        }

        public NanoDBLine GetLine(string key)
        {
            lock (this.readLock)
            {
                if (this.Accessible && this.contentIndex.ContainsKey(key))
                {
                    return this.contentIndex[key];
                }
            }

            return null;
        }

        public bool UpdateLine(NanoDBLine line, params object[] objects)
        {
            lock (this.readLock)
            {
                if (!this.Accessible || !this.contentIndex.ContainsKey(line.Key) || objects.Length != this.Layout.LayoutSize || this.shutdown)
                {
                    return false;
                }

                this.RunningTasks++;

                int position = 1;
                byte[] data = new byte[this.Layout.RowSize];

                data[0] = NanoDBConstants.LineFlagCorrupt;

                for (int i = 0; i < objects.Length; i++)
                {
                    NanoDBElement element = this.Layout.Elements[i];

                    if (element.IsValidElement(objects[i]))
                    {
                        if (i == this.indexedBy)
                        {
                            string newKey = (string)objects[i];

                            if (newKey != line.Key && this.contentIndex.ContainsKey(newKey))
                            {
                                this.RunningTasks--;
                                return false;
                            }

                            this.contentIndex.Remove(line.Key);
                            this.contentIndex[newKey] = line;

                            line.Key = newKey;
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

                            this.sortIndex[line.SortKey].Remove(line);

                            line.SortKey = newSortKey;
                        }

                        element.Write(objects[i], data, position);
                        position += element.Size;

                        line.Content[i] = objects[i];
                    }
                }

                this.writer.AddTask(new NanoDBTask { Type = TaskType.UpdateLine, LineNumber = line.LineNumber, Data = data });
            }

            return true;
        }

        public bool UpdateObject(NanoDBLine line, int layoutIndex, object obj)
        {
            lock (this.readLock)
            {
                if (!this.Accessible || !this.contentIndex.ContainsKey(line.Key) || layoutIndex < 0 || layoutIndex >= this.Layout.LayoutSize || this.shutdown)
                {
                    return false;
                }

                this.RunningTasks++;

                NanoDBElement element = this.Layout.Elements[layoutIndex];

                if (!element.IsValidElement(obj))
                {
                    this.RunningTasks--;
                    return false;
                }

                if (layoutIndex == this.indexedBy)
                {
                    string newKey = (string)obj;

                    if (this.contentIndex.ContainsKey(newKey))
                    {
                        this.RunningTasks--;
                        return false;
                    }

                    this.contentIndex[newKey] = line;
                    this.contentIndex.Remove(line.Key);

                    line.Key = newKey;
                }
                else if (this.Sorted && layoutIndex == this.sortedBy)
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

                    this.sortIndex[line.SortKey].Remove(line);

                    line.SortKey = newSortKey;
                }

                line.Content[layoutIndex] = obj;

                byte[] data = new byte[element.Size];
                element.Write(obj, data, 0);

                this.writer.AddTask(new NanoDBTask { Type = TaskType.UpdateObject, LineNumber = line.LineNumber, Data = data, LayoutIndex = layoutIndex });
            }

            return true;
        }

        public bool RemoveLine(NanoDBLine line, bool allowRecycle = true)
        {
            byte lineFlag = allowRecycle ? NanoDBConstants.LineFlagInactive : NanoDBConstants.LineFlagNoRecycle;

            lock (this.readLock)
            {
                if (!this.Accessible || !this.contentIndex.ContainsKey(line.Key) || this.shutdown)
                {
                    return false;
                }

                this.RunningTasks++;

                this.contentIndex.Remove(line.Key);
                this.EmptyLines++;

                if (this.Sorted)
                {
                    string sortKey = line.SortKey;

                    if (this.sortIndex.ContainsKey(sortKey))
                    {
                        this.sortIndex[sortKey].Remove(line);
                    }
                }

                line.LineFlag = lineFlag;

                this.writer.AddTask(new NanoDBTask { Type = TaskType.RemoveLine, LineNumber = line.LineNumber, LineFlag = lineFlag });
            }
            
            return true;
        }

        public List<string> GetAllKeys()
        {
            return this.contentIndex.Keys.ToList();
        }

        public bool ContainsKey(string key)
        {
            return this.contentIndex.ContainsKey(key);
        }
    }
}
