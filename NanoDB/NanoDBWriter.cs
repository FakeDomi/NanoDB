using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace domi1819.NanoDB
{
    internal class NanoDBWriter
    {
        private bool shutdown;
        private Thread worker;
        private readonly Queue<NanoDBTask> queue = new Queue<NanoDBTask>();
        private readonly NanoDBFile parent;

        private FileStream AccessStream
        {
            get { return this.parent.AccessStream; }
        }

        private NanoDBLayout Layout
        {
            get { return this.parent.Layout; }
        }

        private readonly object queueLock = new object();

        internal NanoDBWriter(NanoDBFile parent)
        {
            this.parent = parent;

            this.worker = new Thread(this.Run);
            this.worker.Start();
        }

        internal void Shutdown()
        {
            this.shutdown = true;


        }

        internal void AddTask(NanoDBTask task)
        {
            lock (this.queueLock)
            {
                this.queue.Enqueue(task);
            }
        }

        private void Run()
        {
            while (!this.shutdown || this.queue.Count > 0)
            {
                if (this.queue.Count > 0)
                {
                    NanoDBTask task;

                    lock (this.queueLock)
                    {
                        task = this.queue.Dequeue();
                    }
                    
                    switch (task.Type)
                    {
                        case TaskType.AddLine:
                            this.AccessStream.Seek(0, SeekOrigin.End);

                            long lineLocation = this.AccessStream.Position;

                            this.AccessStream.SetLength(this.AccessStream.Length + this.Layout.RowSize);
                            this.AccessStream.Write(task.Data, 0, task.Data.Length);

                            this.AccessStream.Seek(lineLocation, SeekOrigin.Begin);
                            this.AccessStream.WriteByte(NanoDBConstants.LineFlagActive);

                            break;
                        case TaskType.UpdateLine:

                            long linePosition = this.Layout.HeaderSize + ((long)this.Layout.RowSize * task.LineNumber);

                            this.BackupLine(linePosition);

                            this.AccessStream.Seek(linePosition, SeekOrigin.Begin);
                            this.AccessStream.Write(task.Data, 0, task.Data.Length);

                            this.AccessStream.Seek(linePosition, SeekOrigin.Begin);
                            this.AccessStream.WriteByte(NanoDBConstants.LineFlagActive);

                            break;
                        case TaskType.UpdateObject:

                            long linePosition2 = this.Layout.HeaderSize + ((long)this.Layout.RowSize * task.LineNumber);
                            long elementPosition = linePosition2 + 1 + this.Layout.Offsets[task.LayoutIndex];

                            this.BackupObject(elementPosition, task.LayoutIndex);

                            this.AccessStream.Seek(linePosition2, SeekOrigin.Begin);
                            this.AccessStream.WriteByte(NanoDBConstants.LineFlagCorrupt);

                            this.AccessStream.Seek(elementPosition, SeekOrigin.Begin);
                            this.AccessStream.Write(task.Data, 0, task.Data.Length);

                            this.AccessStream.Seek(linePosition2, SeekOrigin.Begin);
                            this.AccessStream.WriteByte(NanoDBConstants.LineFlagActive);

                            break;
                        case TaskType.RemoveLine:

                            this.AccessStream.Seek(this.Layout.HeaderSize + task.LineNumber * this.Layout.RowSize, SeekOrigin.Begin);
                            this.AccessStream.WriteByte(task.LineFlag);

                            break;
                    }

                    this.parent.RunningTasks--;
                }
                else
                {
                    Thread.Sleep(125);
                }
            }
        }

        private void BackupLine(long position)
        {
            byte[] data = new byte[this.Layout.RowSize - 1];

            this.AccessStream.Seek(position + 1, SeekOrigin.Begin);
            this.AccessStream.Read(data, 0, data.Length);

            this.AccessStream.Seek(this.Layout.HeaderSize - this.Layout.RowSize, SeekOrigin.Begin);
            this.AccessStream.WriteByte(NanoDBConstants.LineFlagBackup);
            this.AccessStream.Write(data, 0, data.Length);
        }

        private void BackupObject(long position, int layoutIndex)
        {
            byte[] data = new byte[this.Layout.Elements[layoutIndex].Size];

            this.AccessStream.Seek(position, SeekOrigin.Begin);
            this.AccessStream.Read(data, 0, data.Length);

            this.AccessStream.Seek(this.Layout.HeaderSize - 1 - this.Layout.RowSize, SeekOrigin.Begin);
            this.AccessStream.WriteByte((byte)layoutIndex);
            this.AccessStream.WriteByte(NanoDBConstants.LineFlagBackupObject);
            this.AccessStream.Write(data, 0, data.Length);
        }
    }

    internal class NanoDBTask
    {
        internal TaskType Type { get; set; }
        internal byte[] Data { get; set; }
        internal int LayoutIndex { get; set; }
        internal int LineNumber { get; set; }
        internal byte LineFlag { get; set; }
    }

    internal enum TaskType
    {
        AddLine = 0,
        UpdateLine = 1,
        UpdateObject = 2,
        RemoveLine = 3
    }
}
