using System;
using System.Threading;
using System.Threading.Tasks;

namespace Lucene.Net.Store
{
    internal static class TaskExtensions
    {
        internal static void RunWithoutSynchronizationContext(Action actionToRun)
        {
            SynchronizationContext oldContext = SynchronizationContext.Current;
            try
            {
                SynchronizationContext.SetSynchronizationContext(null);
                actionToRun();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(oldContext);
            }
        }

        internal static T RunWithoutSynchronizationContext<T>(Func<T> actionToRun)
        {
            SynchronizationContext oldContext = SynchronizationContext.Current;
            try
            {
                SynchronizationContext.SetSynchronizationContext(null);
                return actionToRun();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(oldContext);
            }
        }

        public static void SafeWait(this Task task)
        {
            RunWithoutSynchronizationContext(() => task).GetAwaiter().GetResult();
            // Task.Run(async () => await task).GetAwaiter().GetResult();
            // task.GetAwaiter().GetResult();
        }

        public static T SafeWait<T>(this Task<T> task)
        {
            try
            {
                return RunWithoutSynchronizationContext(() => task).GetAwaiter().GetResult();
                // return Task.Run(async () => await task).GetAwaiter().GetResult();
                // return task.GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"***** {nameof(TaskExtensions)}.{nameof(SafeWait)}: Unhandled exception: {ex}");
                throw;
            }
        }

        public static void Ignore(this Task task)
        {
            if (task.IsCompleted)
            {
                _ = task.Exception;
            }
            else
            {
                task.ContinueWith(
                    IgnoreTaskContinuation,
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
        }

        private static void IgnoreTaskContinuation(Task task)
        {
            _ = task.Exception;
        }
    }
}
