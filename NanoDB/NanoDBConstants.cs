namespace domi1819.NanoDB
{
    public static class NanoDBConstants
    {
        public static byte LineFlagActive { get; private set; }
        public static byte LineFlagInactive { get; private set; }
        public static byte LineFlagNoRecycle { get; private set; }

        static NanoDBConstants()
        {
            LineFlagActive = 0x20;
            LineFlagInactive = 0x40;
            LineFlagNoRecycle = 0x48;
        }
    }
}
