using System;
using System.IO;
using System.Linq;
using Microsoft.Azure.DataLake.Store;
using NSubstitute;
using Org.Apache.REEF.IO.FileSystem.AzureDataLake;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.IO.Tests
{
    public sealed class TestAzureDataLakeFileSystem
    {
        private readonly static Uri FakeBaseUri = new Uri("http://fakeadls.com");
        private readonly static Uri FakeDirUri = new Uri(FakeBaseUri, "dir");
        private readonly static Uri FakeFileUri = new Uri($"{FakeDirUri}/fakefile");
        private readonly TestContext context;
        private readonly AzureDataLakeFileSystem fs;

        public TestAzureDataLakeFileSystem()
        {
            context = new TestContext();
            fs = context.GetAdlsFileSystem();
        }

        [Fact]
        public void TestOpen()
        {
            context.MockAdlsClient.CreateFile(FakeFileUri.AbsolutePath, IfExists.Overwrite);
            var stream = fs.Open(FakeBaseUri);
            Assert.Equal(typeof(AdlsInputStream), stream.GetType().BaseType);
        }

        [Fact]
        public void TestCreate()
        {
            fs.Create(FakeFileUri);
            Assert.True(context.MockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));
            var directoryEntry = context.MockAdlsClient.GetDirectoryEntry(FakeFileUri.AbsolutePath);
            Assert.Equal(DirectoryEntryType.FILE, directoryEntry.Type);
        }

        [Fact]
        public void TestCreateFileUnderDirectory()
        {
            // Checks when file is created, directory in path was properly created too
            fs.Create(FakeFileUri);
            Assert.True(context.MockAdlsClient.CheckExists(FakeDirUri.AbsolutePath));
            var directoryEntry = context.MockAdlsClient.GetDirectoryEntry(FakeDirUri.AbsolutePath);
            Assert.Equal(DirectoryEntryType.DIRECTORY, directoryEntry.Type);
        }

        [Fact]
        public void TestDelete()
        {
            context.MockAdlsClient.CreateFile(FakeFileUri.AbsolutePath, IfExists.Overwrite);
            fs.Delete(FakeFileUri);
            Assert.False(context.MockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));
        }
        
        [Fact]
        public void TestDeleteException()
        {
            // Delete a file that doesn't exist.
            Exception ex = Assert.Throws<AdlsException>(() => fs.Delete(FakeFileUri));         
            Assert.Equal(typeof(IOException), ex.GetType().BaseType);
        }

        [Fact]
        public void TestFileDoesNotExists()
        {
            Assert.False(context.GetAdlsFileSystem().Exists(FakeFileUri));
        }

        [Fact]
        public void TestExists()
        {
            context.MockAdlsClient.CreateFile(FakeFileUri.AbsolutePath, IfExists.Overwrite);
            Assert.True(fs.Exists(FakeFileUri));
        }

        [Fact]
        public void TestCopy()
        {
            // Setup
            Uri src = new Uri($"{FakeDirUri}/copyfile");
            context.MockAdlsClient.CreateFile(src.AbsolutePath, IfExists.Fail);
            Assert.True(context.MockAdlsClient.CheckExists(src.AbsolutePath));
            Assert.False(context.MockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));

            fs.Copy(src, FakeFileUri);
            Assert.True(context.MockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));
        }

        [Fact]
        public void TestCopyFromLocal()
        {
            Assert.False(context.MockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));
            fs.CopyFromLocal("fakefile", FakeFileUri);
            Assert.True(context.MockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));
        }

        [Fact]
        public void TestCopyToLocal()
        {
            context.MockAdlsClient.CreateFile(FakeFileUri.AbsolutePath, IfExists.Overwrite);
            fs.CopyToLocal(FakeFileUri, Path.GetFileName(FakeFileUri.LocalPath));
            Assert.True(File.Exists(Path.GetFileName(FakeFileUri.LocalPath)));
        }

        [Fact]
        public void TestCreateDirectory()
        {
            fs.CreateDirectory(FakeDirUri);
            Assert.True(context.MockAdlsClient.CheckExists(FakeDirUri.AbsolutePath));
            
            // check if it is a directory and not a file
            var directoryEntry = context.MockAdlsClient.GetDirectoryEntry(FakeDirUri.AbsolutePath);
            Assert.Equal(DirectoryEntryType.DIRECTORY, directoryEntry.Type); 
        }

        [Fact]
        public void TestDeleteDirectory()
        {
            context.MockAdlsClient.CreateDirectory(FakeDirUri.AbsolutePath);
            Assert.True(context.MockAdlsClient.CheckExists(FakeDirUri.AbsolutePath), "Test setup failed: did not successfully create directory to delete.");
            fs.Delete(FakeDirUri);
            Assert.False(context.MockAdlsClient.CheckExists(FakeDirUri.AbsolutePath), "Test to delete adls directory failed.");
        }

        [Fact]
        public void TestDeleteDirectoryException()
        {
            // Delete a directory that doesn't exist.
            Exception ex = Assert.Throws<AdlsException>(() => fs.DeleteDirectory(FakeDirUri));
            Assert.Equal(typeof(IOException), ex.GetType().BaseType);
        }

        [Fact]
        public void TestGetChildren()
        {
            context.MockAdlsClient.CreateDirectory(FakeDirUri.AbsolutePath);
            var children = fs.GetChildren(FakeDirUri);
            int count = children.Count();
            Assert.Equal(0, count);

            context.MockAdlsClient.CreateFile(FakeFileUri.AbsolutePath, IfExists.Overwrite);
            children = fs.GetChildren(FakeDirUri);
            count = children.Count();
            Assert.Equal(1, count);
        }
        
        [Fact]
        public void TestCreateUriForPath()
        {
            string dirStructure = FakeFileUri.AbsolutePath;
            Uri createdUri = fs.CreateUriForPath(dirStructure);
            Assert.Equal(createdUri, new Uri($"adl://{context.AdlAccountName}{dirStructure}"));
        }

        [Fact]
        public void TestGetFileStatusThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => fs.GetFileStatus(null));
        }

        private sealed class TestContext
        {
            public readonly string AdlAccountName = "adlAccount";
            public readonly AdlsClient MockAdlsClient = Microsoft.Azure.DataLake.Store.MockAdlsFileSystem.MockAdlsClient.GetMockClient();
            public readonly IDataLakeStoreClient TestDataLakeStoreClient = Substitute.For<IDataLakeStoreClient>();

            public AzureDataLakeFileSystem GetAdlsFileSystem()
            {
                var conf = AzureDataLakeFileSystemConfiguration.ConfigurationModule
                     .Set(AzureDataLakeFileSystemConfiguration.DataLakeStorageAccountName, "adlsAccountName")
                    .Set(AzureDataLakeFileSystemConfiguration.Tenant, "tenant")
                    .Set(AzureDataLakeFileSystemConfiguration.ClientId, "clientId")
                    .Set(AzureDataLakeFileSystemConfiguration.SecretKey, "secretKey")
                    .Build();
                var injector = TangFactory.GetTang().NewInjector(conf);
                injector.BindVolatileInstance(TestDataLakeStoreClient);
                TestDataLakeStoreClient.GetReference().ReturnsForAnyArgs(MockAdlsClient);
                TestDataLakeStoreClient.AccountFQDN.Returns(AdlAccountName);
                var fs = injector.GetInstance<AzureDataLakeFileSystem>();
                return fs;
            }
        }
    }
}
