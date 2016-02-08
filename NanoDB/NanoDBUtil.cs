using System.Collections;
using System.Collections.Generic;

namespace domi1819.NanoDB
{
    public class ReadOnlyArray<T> : IEnumerable<T>
    {
        public int Length
        {
            get { return this.listMode ? this.list.Count : this.array.Length; }
        }

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
