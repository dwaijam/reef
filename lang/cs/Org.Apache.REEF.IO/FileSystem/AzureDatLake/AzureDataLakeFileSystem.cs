using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.DataLake.Store;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.FileSystem.AzureDataLake
{
    internal sealed class AzureDataLakeFileSystem : IFileSystem
    {
        AdlsClient _client;

        [Inject]
        private AzureDataLakeFileSystem(AdlsClient client)
        {
            _client = client;
        }

        public void Copy(Uri sourceUri, Uri destinationUri)
        {
            throw new NotImplementedException();
        }

        public void CopyFromLocal(string localFileName, Uri remoteFileUri)
        {
            throw new NotImplementedException();
        }

        public void CopyToLocal(Uri remoteFileUri, string localName)
        {
            throw new NotImplementedException();
        }

        public Stream Create(Uri fileUri)
        {
            throw new NotImplementedException();
        }

        public void CreateDirectory(Uri directoryUri)
        {
            throw new NotImplementedException();
        }

        public Uri CreateUriForPath(string path)
        {
            throw new NotImplementedException();
        }

        public void Delete(Uri fileUri)
        {
            throw new NotImplementedException();
        }

        public void DeleteDirectory(Uri directoryUri)
        {
            _client.DeleteRecursive(directoryUri.ToString());
        }

        public bool Exists(Uri fileUri)
        {
            return _client.CheckExists(fileUri.ToString());
        }

        public IEnumerable<Uri> GetChildren(Uri directoryUri)
        {
            throw new NotImplementedException();
        }

        public FileStatus GetFileStatus(Uri remoteFileUri)
        {
            throw new NotImplementedException();
        }

        public Stream Open(Uri fileUri)
        {
            return _client.GetReadStream(fileUri.ToString());
        }
    }
}
