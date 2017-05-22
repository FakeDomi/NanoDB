namespace domi1819.NanoDB
{
    public class NanoDBLine
    {
        public byte LineFlag { get; internal set; }
        public int LineNumber { get; private set; }
        public string Key { get; internal set; }
        public string SortKey { get; internal set; }

        public int ElementCount => this.Content.Length;

        internal object[] Content { get; private set; }

        private readonly NanoDBFile parent;

        internal NanoDBLine(NanoDBFile parent, byte flag, int line, string key, object[] content, string sortKey = null)
        {
            this.parent = parent;
            this.LineFlag = flag;
            this.LineNumber = line;
            this.Key = key;
            this.Content = content;
            this.SortKey = sortKey;
        }

        public object this[int index]
        {
            get => this.Content[index];
            set => this.parent.UpdateObject(this, index, value);
        }

        public bool SetValues(params object[] objects)
        {
            return this.parent.UpdateLine(this, objects);
        }

        public bool Remove(bool allowRecycle = true)
        {
            return this.parent.RemoveLine(this, allowRecycle);
        }
    }
}
