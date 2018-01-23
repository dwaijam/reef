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
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;
using System.Threading;
using Org.Apache.REEF.IO.FileSystem.AzureDataLake;
using Microsoft.Azure.DataLake.Store;
using Org.Apache.REEF.Tang.Interface;
using System.IO;
using System.Linq;

namespace Org.Apache.REEF.IO.Tests
{
    /// <summary>
    /// E2E tests for AzureDataLakeFileSystem.
    /// These tests require the person running the test to fill in credentials.
    /// </summary>
    public sealed class TestAzureDataLakeFileSystemE2E : IDisposable
    {
        private const string SkipMessage = "Fill in credentials before running test"; // Use null to run tests
        private const string ContentsText = "hello";
        private IFileSystem _fileSystem;
        private AdlsClient _adlsClient;
        private string defaultFolderName;

        public TestAzureDataLakeFileSystemE2E()
        {
            // Service principal / appplication authentication with client secret / key
            // Use the client ID of an existing AAD "Web App" application.
            // Fill in before running test!
            const string adlsAccountName = "#####.azuredatalakestore.net";
            const string tenant = "microsoft.onmicrosoft.com";
            const string tokenAudience = @"https://datalake.azure.net/";
            const string clientId = "#############################";
            const string secretKey = "##########";

            defaultFolderName = "reef-test-folder-" + Guid.NewGuid();

            IConfiguration conf = AzureDataLakeFileSystemConfiguration.ConfigurationModule
                .Set(AzureDataLakeFileSystemConfiguration.DataLakeStorageAccountName, adlsAccountName)
                .Set(AzureDataLakeFileSystemConfiguration.Tenant, tenant)
                .Set(AzureDataLakeFileSystemConfiguration.ClientId, clientId)
                .Set(AzureDataLakeFileSystemConfiguration.SecretKey, secretKey)
                .Build();

            _fileSystem = TangFactory.GetTang().NewInjector(conf).GetInstance<AzureDataLakeFileSystem>();

            ServiceClientCredentials adlCreds = GetCreds_SPI_SecretKey(tenant, new Uri(tokenAudience), clientId, secretKey);
            _adlsClient = AdlsClient.CreateClient(adlsAccountName, adlCreds);
        }

        public void Dispose()
        {
            if (_adlsClient != null)
            {
                _adlsClient.DeleteRecursive($"/{defaultFolderName}");
            }
        }

        [Fact(Skip = SkipMessage)]
        public void TestOpenE2E()
        {
            string fileName = UploadFromString(ContentsText);
            var stream = _fileSystem.Open(PathToFile(fileName));
            StreamReader reader = new StreamReader(stream);
            string streamText = reader.ReadToEnd();
            Assert.Equal(ContentsText, streamText);
        }

        [Fact(Skip = SkipMessage)]
        public void TestCreateE2E()
        {
            string fileName = $"/{defaultFolderName}/TestCreateE2E.txt";
            var stream = _fileSystem.Create(PathToFile(fileName));
            Assert.True(_adlsClient.CheckExists(fileName));
            Assert.Equal(typeof(AdlsOutputStream), stream.GetType());
        }

        [Fact(Skip = SkipMessage)]
        public void TestDeleteE2E()
        {
            string fileName = UploadFromString(ContentsText);
            Assert.True(_adlsClient.CheckExists(fileName));
            _fileSystem.Delete(PathToFile(fileName));
            Assert.False(_adlsClient.CheckExists(fileName));
        }

        [Fact(Skip = SkipMessage)]
        public void TestExistsE2E()
        {
            string fileName = UploadFromString(ContentsText);
            Assert.True(_fileSystem.Exists(PathToFile(fileName)));
            _adlsClient.Delete(fileName);
            Assert.False(_fileSystem.Exists(PathToFile(fileName)));
        }

        [Fact(Skip = SkipMessage)]
        public void TestCopyE2E()
        {
            throw new NotImplementedException();
        }

        [Fact(Skip = SkipMessage)]
        public void TestCopyToLocalE2E()
        {         
            var tempFilePath = Path.GetTempFileName();
            try
            {
                string fileName = UploadFromString(ContentsText);
                _fileSystem.CopyToLocal(PathToFile(fileName), tempFilePath);
                Assert.True(File.Exists(tempFilePath));
                Assert.Equal(ContentsText, File.ReadAllText(tempFilePath));
            }
            finally
            {
                File.Delete(tempFilePath);
            }
        }

        [Fact(Skip = SkipMessage)]
        public void TestCopyFromLocalE2E()
        {
            var tempFilePath = Path.GetTempFileName();
            var tempFileName = Path.GetFileName(tempFilePath);
            try
            {
                File.WriteAllText(tempFilePath, ContentsText);
                Uri remoteFileUri = PathToFile($"/{defaultFolderName}/{tempFileName}");
                _fileSystem.CopyFromLocal(tempFilePath, remoteFileUri);
                Assert.True(_adlsClient.CheckExists($"/{defaultFolderName}/{tempFileName}"));
                var stream = _fileSystem.Open(remoteFileUri);
                StreamReader reader = new StreamReader(stream);
                string streamText = reader.ReadToEnd();
                Assert.Equal(ContentsText, streamText);
            }
            finally
            {
                File.Delete(tempFilePath);
            }
        }

        [Fact(Skip = SkipMessage)]
        public void TestCreateDirectoryE2E()
        {
            string dirName = $"/{defaultFolderName}";
            _fileSystem.CreateDirectory(PathToFile(dirName));
            Assert.True(_adlsClient.CheckExists(dirName));
        }

        [Fact(Skip = SkipMessage)]
        public void TestDeleteDirectoryE2E()
        {
            string dirName = $"/{defaultFolderName}";
            _adlsClient.CreateDirectory(dirName);
            Assert.True(_adlsClient.CheckExists(dirName));
            _fileSystem.Delete(PathToFile(dirName));
            Assert.False(_adlsClient.CheckExists(dirName));
        }

        [Fact(Skip = SkipMessage)]
        public void TestGetChildrenE2E()
        {
            string fileName1 = UploadFromString("file1", 1);
            string fileName2 = UploadFromString("file2", 2);
            string dirName = $"/{defaultFolderName}";
            var childUris = _fileSystem.GetChildren(PathToFile(dirName)).ToList();
            Assert.True(childUris.Count() == 2);
            Assert.Equal(fileName2,  childUris.First().AbsolutePath.Equals(fileName1) ? childUris.Last().AbsolutePath : childUris.First().AbsolutePath);
        }

        [Fact(Skip = SkipMessage)]
        public void TestGetFileStatusE2E()
        {
            string fileName = UploadFromString(ContentsText);
            var fileStatus = _fileSystem.GetFileStatus(PathToFile(fileName));
            Assert.True(fileStatus.LengthBytes == ContentsText.Length);
            Assert.True(fileStatus.ModificationTime > DateTime.Now.AddSeconds(-60));
        }

        private static ServiceClientCredentials GetCreds_SPI_SecretKey(string tenant, Uri tokenAudience, string clientId, string secretKey)
        {
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());

            var serviceSettings = ActiveDirectoryServiceSettings.Azure;
            serviceSettings.TokenAudience = tokenAudience;

            var creds = ApplicationTokenProvider.LoginSilentAsync(
             tenant,
             clientId,
             secretKey,
             serviceSettings).GetAwaiter().GetResult();
            return creds;
        }

        private string UploadFromString(string str, int fileIndex = 1)
        {
            string fileName = $"/{defaultFolderName}/testFile{fileIndex}.txt";
            using (var streamWriter = new StreamWriter(_adlsClient.CreateFile(fileName, IfExists.Overwrite)))
            {
                streamWriter.Write(str);
            }
            return fileName;
        }

        private Uri PathToFile(string filePath)
        {
            return _fileSystem.CreateUriForPath(filePath);
        }
    }
}
