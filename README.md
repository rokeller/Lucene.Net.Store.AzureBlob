# Lucene.Net.Store.AzureBlob

Persists Lucene.Net index files in Azure Blob Storage.

## Description

Provides a `Lucene.Net.Store.Directory` implementation that persists index files in Azure Blob Storage. Unlike similar
projects such as the [Lucene.Net.Store.Azure Nuget Package](https://www.nuget.org/packages/Lucene.Net.Store.Azure),
the `Lucene.Net.Store.AzureBlobDirectory` does **not** automatically cache index files in a local `FSDirectory` or
`RAMDirectory`. It can therefore be used much more flexibly and combined with other `Directory` implementations such
as [Lucene.Net.Store.CachedRemote](https://www.nuget.org/packages/Lucene.Net.Store.CachedRemote/) as needed *without*
code changes on the `Directory` to match your requirements. In addition, it allows for greater configurational
flexibility and it can be used to keep index files in folders as opposed to the root of a container.
