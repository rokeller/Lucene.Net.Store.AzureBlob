using System;
using System.Threading;
using System.Threading.Tasks;

namespace Lucene.Net.Store
{
    internal static class TaskExtensions
    {
        public static void SafeWait(this Task task)
        {
            task.GetAwaiter().GetResult();
        }

        public static T SafeWait<T>(this Task<T> task)
        {
            try
            {
                return task.GetAwaiter().GetResult();
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
