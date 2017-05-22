using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace domi1819.NanoDB
{
    public class NanoDBLayout
    {
        public ReadOnlyArray<NanoDBElement> Elements { get; private set; }

        public int LayoutSize => this.Elements.Length;

        public int HeaderSize => this.LayoutSize + 8 + this.RowSize;

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

    public class ReadOnlyArray<T> : IEnumerable<T>
    {
        public int Length => this.listMode ? this.list.Count : this.array.Length;

        public T this[int index]
        {
            get { return this.listMode ? this.list[index] : this.array[index]; }
            internal set
            {
                if (this.listMode)
                {
                    this.list[index] = value;
                }
                else
                {
                    this.array[index] = value;
                }
            }
        }

        private readonly T[] array;
        private readonly List<T> list;

        private readonly bool listMode;

        internal ReadOnlyArray(T[] elements)
        {
            this.array = elements;
        }

        internal ReadOnlyArray(List<T> elements)
        {
            this.list = elements;
            this.listMode = true;
        }

        internal ReadOnlyArray(int size)
        {
            this.array = new T[size];
        }

        public IEnumerator<T> GetEnumerator()
        {
            return this.listMode ? this.list.GetEnumerator() : ((IEnumerable<T>)this.array).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        internal T[] GetArray()
        {
            return this.array;
        }
    }
}
