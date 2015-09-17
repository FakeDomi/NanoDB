namespace domi1819.NanoDB
{
    public class NanoDBLayout
    {
        public NanoDBElement[] LayoutElements { get; private set; }

        public int[] Offsets { get; private set; }

        public int LayoutSize { get { return this.LayoutElements.Length; } private set { } }
        public int HeaderSize { get { return this.LayoutElements.Length + 2; } private set { } }
        public int RowSize { get; private set; }

        public NanoDBLayout(params NanoDBElement[] layout)
        {
            this.LayoutElements = layout;

            int offset = 0;
            Offsets = new int[layout.Length];

            for (int i = 0; i < layout.Length; i++)
            {
                Offsets[i] = offset;
                offset += layout[i].Size;
            }

            this.RowSize = offset + 1;
        }

        public bool Compare(NanoDBLayout checkLayout)
        {
            return this.Compare(checkLayout.LayoutElements);
        }

        public bool Compare(params NanoDBElement[] checkLayout)
        {
            if (this.LayoutElements.Length != checkLayout.Length)
            {
                return false;
            }

            for (int i = 0; i < checkLayout.Length; i++)
            {
                if (checkLayout[i].Id != this.LayoutElements[i].Id)
                {
                    return false;
                }
            }

            return true;
        }
    }
}
