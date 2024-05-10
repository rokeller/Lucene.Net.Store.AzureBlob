namespace Lucene.Net.Store
{
    /// <summary>
    /// Defines options for a Lucene.net directory stored in Azure blobs.
    /// </summary>
    public class AzureBlobDirectoryOptions
    {
        /// <summary>
        /// A flag which indicates whether the <c>segments.gen</c> file should
        /// be cached whenever possible.
        /// </summary>
        public bool CacheSegmentsGen { get; set; }
    }
}
