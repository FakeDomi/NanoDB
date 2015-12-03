using System.Linq;

namespace domi1819.NanoDB
{
    public class NanoDBLayout
    {
        public NanoDBElements LayoutElements { get; private set; }

        public int LayoutSize { get { return this.LayoutElements.Length; } }
        public int HeaderSize { get { return this.LayoutSize + 4 + this.RowSize; } }
        public int RowSize { get; private set; }

        internal int[] Offsets { get; private set; }

        public NanoDBLayout(params NanoDBElement[] elements)
        {
            this.LayoutElements = new NanoDBElements(elements);
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
            return this.LayoutElements.Compare(otherLayout);
        }
    }

    public class NanoDBElements
    {
        public int Length
        {
            get
            {
                return this.elements.Length;
            }
        }

        private readonly NanoDBElement[] elements;

        public NanoDBElements(int length)
        {
            this.elements = new NanoDBElement[length];
        }

        public NanoDBElements(NanoDBElement[] elements)
        {
            this.elements = elements;
        }

        public NanoDBElement this[int index]
        {
            get
            {
                return this.elements[index];
            }
            internal set
            {
                this.elements[index] = value;
            }
        }

        public bool Compare(NanoDBLayout compareLayout)
        {
            if (this.elements.Length != compareLayout.LayoutElements.elements.Length)
            {
                return false;
            }

            return !this.elements.Where((t, i) => t.Id != compareLayout.LayoutElements.elements[i].Id).Any();
        }
    }

}
