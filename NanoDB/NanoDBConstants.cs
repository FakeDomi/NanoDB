namespace domi1819.NanoDB
{
    public static class NanoDBConstants
    {
        public static byte LineFlagActive { get; private set; }
        public static byte LineFlagInactive { get; private set; }
        public static byte LineFlagNoRecycle { get; private set; }
        public static byte LineFlagBackup { get; private set; }
        public static byte LineFlagBackupObject { get; private set; }
        public static byte LineFlagIncomplete { get; private set; }
        public static byte LineFlagCorrupt { get; private set; }

        public static int DatabaseStructureVersion { get; private set; }

        public static ReadOnlyArray<byte> MagicBytes { get; private set; }

        static NanoDBConstants()
        {
            LineFlagActive = 0x20;
            LineFlagInactive = 0x40;
            LineFlagNoRecycle = 0x48;
            LineFlagBackup = 0x80;
            LineFlagBackupObject = 0x82;
            LineFlagIncomplete = 0x84;
            LineFlagCorrupt = 0x88;

            DatabaseStructureVersion = 4;

            MagicBytes = new ReadOnlyArray<byte>(new byte[] { 0x4E, 0x41, 0x4E, 0x4F });
        }
    }

    public enum InitializeResult
    {
        Success = 0,
        FileEmpty = 1,
        VersionMismatch = 2,
        FileCorrupt = 3,
        UnknownDataType = 4,
        UnexpectedFileEnd = 5
    }

    public enum LoadResult
    {
        Okay = 0,
        HasDuplicates = 1,
        NotInitialized = 2,
        NotIndexable = 3
    }
}
