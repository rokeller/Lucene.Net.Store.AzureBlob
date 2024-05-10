# Azure Blob Storage-backed Directory for Lucene.net

Provides two `Lucene.Net.Store.Directory` implementations that persist index
files in Azure Blob Storage. Compared to other packages which have limitations
such as storing index files only at the root of containers, the directories in
this packages do allow for greater flexibility and they can be used to keep
index files for multiple indices in folders in the same container.

The class `Lucene.Net.Store.FileBackedAzureBlobDirectory` always stores
downloaded files in the `FSDirectory` passed to the constructor. This is the
preferred way of using Azure blob based indices, as it results in fewer calls to
Azure Storage Blob APIs. It always locks the index through Azure blob locks, and
always writes on the FSDirectory first and only syncs changes to blobs on commit.

The class `Lucene.Net.Store.AzureBlobDirectory` on the other hand does **not**
automatically cache index files in a local `FSDirectory` or `RAMDirectory`. It
can therefore be used much more flexibly and combined with other `Directory`
implementations, such as
[Lucene.Net.Store.CachedRemote](https://www.nuget.org/packages/Lucene.Net.Store.CachedRemote/),
as needed *without* code changes on the `Directory` to match your requirements,
including write-through or local locking. That does however come with more calls
to Azure Storage Blob APIs. Tests have shown that the number of calls can be
between 2 and 10 times as high as with the `FileBackedAzureBlobDirectory`.
