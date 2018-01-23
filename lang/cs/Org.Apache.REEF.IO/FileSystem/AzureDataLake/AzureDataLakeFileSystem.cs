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
using Microsoft.Azure.DataLake.Store.FileTransfer;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.FileSystem.AzureDataLake
{
    /// <summary>
    /// An IFileSystem implementation for Azure Data Lake Store.
    /// </summary>
    internal sealed class AzureDataLakeFileSystem : IFileSystem
    {
        IDataLakeStoreClient _client;

        [Inject]
        private AzureDataLakeFileSystem(IDataLakeStoreClient client)
        {
            _client = client;
        }

        /// <summary>
        /// Opens the given URI for reading
        /// </summary>
        /// <exception cref="AdlsException">If the URI couldn't be opened.</exception>
        public Stream Open(Uri fileUri)
        {
            return _client.GetReference().GetReadStream(fileUri.AbsolutePath);
        }

        /// <summary>
        /// Creates a new file under the given URI.
        /// </summary>
        /// <exception cref="AdlsException">If the URI couldn't be created.</exception>
        public Stream Create(Uri fileUri)
        {
            return _client.GetReference().CreateFile(fileUri.AbsolutePath, IfExists.Overwrite);
        }

        /// <summary>
        /// Deletes the file under the given URI.
        /// </summary>
        /// <exception cref="AdlsException">If the specified file cannot be deleted</exception>
        public void Delete(Uri fileUri)
        {
            bool deleteStatus = _client.GetReference().Delete(fileUri.AbsolutePath);
            if (!deleteStatus)
            {
                throw new AdlsException($"Cannot delete directory/file specified by {fileUri.ToString()}");
            }
        }

        /// <summary>
        /// Determines whether a file exists under the given URI.
        /// </summary>
        public bool Exists(Uri fileUri)
        {
            return _client.GetReference().CheckExists(fileUri.AbsolutePath);
        }

        /// <summary>
        /// Copies the file referenced by sourceUri to destinationUri.
        /// </summary>
        /// <exception cref="AdlsException">If copy process encounters any exceptions</exception>
        public void Copy(Uri sourceUri, Uri destinationUri)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Copies the remote file to a local file.
        /// </summary>
        /// <exception cref="AdlsException">If copy process encounters any exceptions</exception>
        public void CopyToLocal(Uri remoteFileUri, string localName)
        {
            TransferStatus status = _client.GetReference().BulkDownload(remoteFileUri.AbsolutePath, localName);
            if (status.EntriesFailed.Count != 0)
            {
                throw new AdlsException($"{status.EntriesFailed.Count} entries did not get transferred correctly");
            }
        }

        /// <summary>
        /// Copies the specified file to the remote location.
        /// </summary>
        /// <exception cref="AdlsException">If copy process encounters any exception</exception>
        public void CopyFromLocal(string localFileName, Uri remoteFileUri)
        {
            TransferStatus status = _client.GetReference().BulkUpload(localFileName, remoteFileUri.AbsolutePath);
            if (status.EntriesFailed.Count != 0)
            {
                throw new AdlsException($"{status.EntriesFailed.Count} entries did not get transferred correctly");
            }
        }

        /// <summary>
        /// Creates a new directory.
        /// </summary>
        /// <exception cref="AdlsException">If directory cannot be created</exception>
        public void CreateDirectory(Uri directoryUri)
        {
            bool createDirStatus = _client.GetReference().CreateDirectory(directoryUri.AbsolutePath);
            if (!createDirStatus)
            {
                throw new AdlsException($"Cannot create directory specified by {directoryUri.ToString()}");
            }
        }

        /// <summary>
        /// Deletes a directory.
        /// </summary>
        /// <exception cref="AdlsException">If directory cannot be deleted</exception>
        public void DeleteDirectory(Uri directoryUri)
        {
            bool deleteStatus = Exists(directoryUri) && 
                _client.GetReference().GetDirectoryEntry(directoryUri.AbsolutePath).Type == DirectoryEntryType.DIRECTORY && 
                _client.GetReference().DeleteRecursive(directoryUri.AbsolutePath);
            if (!deleteStatus)
            {
                throw new AdlsException($"Cannot delete directory specified by {directoryUri.ToString()}");
            }
        }

        /// <summary>
        /// Get the children on the given URI, if that refers to a directory.
        /// </summary>
        /// <exception cref="AdlsException">If directory does not exist</exception>
        public IEnumerable<Uri> GetChildren(Uri directoryUri)
        {
            if (Exists(directoryUri) && _client.GetReference().GetDirectoryEntry(directoryUri.AbsolutePath).Type == DirectoryEntryType.DIRECTORY)
            {
                foreach (var entry in _client.GetReference().EnumerateDirectory(directoryUri.AbsolutePath))
                {
                    yield return new Uri($"{GetUriPrefix()}{entry.FullName}");
                }
            }
            else
            {
                throw new AdlsException($"Cannot find directory specified by {directoryUri.ToString()}");
            }
        }

        /// <summary>
        /// Create Uri from a given file path.
        /// The file path can be full with prefix or relative without prefix.
        /// If null is passed as the path, ArgumentException will be thrown.
        /// </summary>
        /// <exception cref="ArgumentNullException">If specified path is null</exception>
        public Uri CreateUriForPath(string path)
        {
            if (path == null)
            {
                throw new ArgumentNullException("Specified path is null");
            }
            return new Uri($"{GetUriPrefix()}/{path.TrimStart('/')}");
        }

        /// <summary>
        /// Gets the FileStatus for remote file.
        /// </summary>
        /// <exception cref="ArgumentNullException">If remote file URI is null</exception>
        /// <returns>FileStatus</returns>
        public FileStatus GetFileStatus(Uri remoteFileUri)
        {
            if (remoteFileUri == null)
            {
                throw new ArgumentNullException("Specified uri is null");
            }
            var entrySummary = _client.GetReference().GetDirectoryEntry(remoteFileUri.AbsolutePath);
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
