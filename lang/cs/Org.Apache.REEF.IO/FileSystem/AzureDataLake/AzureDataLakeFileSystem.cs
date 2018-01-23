// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// 
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Azure.DataLake.Store;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.FileSystem.AzureDataLake
{
    internal sealed class AzureDataLakeFileSystem : IFileSystem
    {
        AdlsClient _client;

        [Inject]
        private AzureDataLakeFileSystem(IDataLakeStoreClient client)
        {
            _client = client.GetReference();
        }

        public Stream Open(Uri fileUri)
        {
            return _client.GetReadStream(fileUri.AbsolutePath);
        }

        public Stream Create(Uri fileUri)
        {
            return _client.CreateFile(fileUri.AbsolutePath, IfExists.Overwrite);
        }

        public void Delete(Uri fileUri)
        {
            _client.Delete(fileUri.AbsolutePath);
        }

        public bool Exists(Uri fileUri)
        {
            return _client.CheckExists(fileUri.AbsolutePath);
        }

        public void Copy(Uri sourceUri, Uri destinationUri)
        {
            throw new NotImplementedException();
        }

        public void CopyToLocal(Uri remoteFileUri, string localName)
        {
            _client.BulkDownload(remoteFileUri.AbsolutePath, localName);
        }

        public void CopyFromLocal(string localFileName, Uri remoteFileUri)
        {
            _client.BulkUpload(localFileName, remoteFileUri.AbsolutePath);
        }

        public void CreateDirectory(Uri directoryUri)
        {
            _client.CreateDirectory(directoryUri.AbsolutePath);
        }

        public void DeleteDirectory(Uri directoryUri)
        {
            _client.DeleteRecursive(directoryUri.AbsolutePath);
        }

        public IEnumerable<Uri> GetChildren(Uri directoryUri)
        {
            foreach (var entry in _client.EnumerateDirectory(directoryUri.AbsolutePath))
            {
                yield return new Uri($"{GetUriPrefix()}{entry.FullName}");
            }
        }

        public Uri CreateUriForPath(string path)
        {
            return new Uri($"{GetUriPrefix()}/{path.TrimStart('/')}");
        }

        public FileStatus GetFileStatus(Uri remoteFileUri)
        {
            var entrySummary = _client.GetDirectoryEntry(remoteFileUri.AbsolutePath);
            if (!entrySummary.LastModifiedTime.HasValue)
            {
                throw new AdlsException("File/Directory at " + remoteFileUri + " does not have a last modified" +
                                           "time. It may have been deleted.");
            }

            return new FileStatus(entrySummary.LastModifiedTime.Value, entrySummary.Length);
        }

        private string GetUriPrefix()
        {
            return $"adl://{_client.AccountFQDN}";
        }
    }
}
