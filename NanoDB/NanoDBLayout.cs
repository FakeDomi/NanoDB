using System.Linq;

namespace domi1819.NanoDB
{
    public class NanoDBLayout
    {
        public ReadOnlyArray<NanoDBElement> Elements { get; private set; }

        public int LayoutSize
        {
            get { return this.Elements.Length; }
        }

        public int HeaderSize
        {
            get { return this.LayoutSize + 8 + this.RowSize; }
        }

        public int RowSize { get; private set; }

        internal int[] Offsets { get; private set; }

        public NanoDBLayout(params NanoDBElement[] elements)
        {
            this.Elements = new ReadOnlyArray<NanoDBElement>(elements);
            this.Offsets = new int[elements.Length];

            int offset = 0;

            for (int i = 0; i < elements.Length; i++)
            {
                this.Offsets[i] = offset;
                offset += elements[i].Size;
            }

            this.RowSize = offset + 1;
        }

        public bool Compare(NanoDBLayout otherLayout)
        {
            if (this.Elements.Length == otherLayout.Elements.Length)
            {
                return !this.Elements.Where((t, i) => t != otherLayout.Elements[i]).Any();
            }

            return false;
        }
    }
}
