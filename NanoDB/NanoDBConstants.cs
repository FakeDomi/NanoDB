using System;

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

        public static int Version { get; private set; }

        static NanoDBConstants()
        {
            LineFlagActive = 0x20;
            LineFlagInactive = 0x40;
            LineFlagNoRecycle = 0x48;
            LineFlagBackup = 0x80;
            LineFlagBackupObject = 0x82;
            LineFlagIncomplete = 0x84;
            LineFlagCorrupt = 0x88;

            Version = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.Build;
        }
    }
}
