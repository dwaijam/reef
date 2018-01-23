using System;
using System.IO;
using System.Linq;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.DataLake.Store.MockAdlsFileSystem;
using NSubstitute;
using Org.Apache.REEF.IO.FileSystem.AzureDataLake;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.IO.Tests
{
    public sealed class TestAzureDataLakeFileSystem
    {
        private readonly static Uri FakeBaseUri = new Uri("http://fakeadls.com");
        private readonly static Uri FakeFileUri = new Uri("http://fakeadls.com/dir/fakefile");
        private readonly static Uri FakeDirUri = new Uri("http://fakeadls.com/dir");
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
            context.mockAdlsClient.CreateFile(FakeFileUri.AbsolutePath, IfExists.Overwrite);
            var stream = fs.Open(FakeBaseUri);
            Assert.Equal(typeof(AdlsInputStream), stream.GetType().BaseType);
        }

        [Fact]
        public void TestCreate()
        {
            fs.Create(FakeFileUri);
            Assert.True(context.mockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));
            var directoryEntry = context.mockAdlsClient.GetDirectoryEntry(FakeFileUri.AbsolutePath);
            Assert.Equal(DirectoryEntryType.FILE, directoryEntry.Type);
        }

        [Fact]
        public void TestCreateFileUnderDirectory()
        {
            // Checks when file is created, directory in path was properly created too
            fs.Create(FakeFileUri);
            Assert.True(context.mockAdlsClient.CheckExists(FakeDirUri.AbsolutePath));
            var directoryEntry = context.mockAdlsClient.GetDirectoryEntry(FakeDirUri.AbsolutePath);
            Assert.Equal(DirectoryEntryType.DIRECTORY, directoryEntry.Type);
        }

        [Fact]
        public void TestDelete()
        {
            context.mockAdlsClient.CreateFile(FakeFileUri.AbsolutePath, IfExists.Overwrite);
            fs.Delete(FakeFileUri);
            Assert.False(context.mockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));
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
            context.mockAdlsClient.CreateFile(FakeFileUri.AbsolutePath, IfExists.Overwrite);
            Assert.True(fs.Exists(FakeFileUri));
        }

        [Fact]
        public void TestCopy()
        {
            // Setup
            Uri src = new Uri("http://fakeadls.src.com/dir/copyfile");
            context.mockAdlsClient.CreateFile(src.AbsolutePath, IfExists.Fail);
            Assert.True(context.mockAdlsClient.CheckExists(src.AbsolutePath));
            Assert.False(context.mockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));

            fs.Copy(src, FakeBaseUri);
            Assert.True(context.mockAdlsClient.CheckExists(FakeFileUri.AbsolutePath));
        }

        [Fact]
        public void TestCopyFromLocal()
        {
            fs.CopyFromLocal("fakefile", FakeFileUri);
            Assert.True(context.mockAdlsClient.CheckExists("/dir/fakefile"));
        }

        [Fact]
        public void TestCopyToLocal()
        {
            context.mockAdlsClient.CreateFile(FakeFileUri.AbsolutePath, IfExists.Overwrite);
            fs.CopyToLocal(FakeFileUri, "fakefile");
            Assert.True(File.Exists("fakefile"));
        }

        [Fact]
        public void TestCreateDirectory()
        {
            fs.CreateDirectory(FakeDirUri);
            Assert.True(context.mockAdlsClient.CheckExists(FakeDirUri.AbsolutePath));
            
            // check if it is a directory and not a file
            var directoryEntry = context.mockAdlsClient.GetDirectoryEntry(FakeDirUri.AbsolutePath);
            Assert.Equal(DirectoryEntryType.DIRECTORY, directoryEntry.Type); 
        }

        [Fact]
        public void TestDeleteDirectory()
        {
            context.mockAdlsClient.CreateDirectory(FakeDirUri.AbsolutePath);
            Assert.True(context.mockAdlsClient.CheckExists(FakeDirUri.AbsolutePath), "Test setup failed: did not successfully create directory to delete.");
            fs.Delete(FakeDirUri);
            Assert.False(context.mockAdlsClient.CheckExists(FakeDirUri.AbsolutePath), "Test to delete adls directory failed.");
        }

        [Fact]
        public void TestDeleteDirectoryException()
        {
            // Delete a directory that doesn't exist.
            Assert.Throws<AdlsException>(() => fs.DeleteDirectory(FakeDirUri));
        }

        [Fact]
        public void TestGetChildren()
        {
            context.mockAdlsClient.CreateDirectory(FakeDirUri.AbsolutePath);
            var children = fs.GetChildren(FakeDirUri);
            int count = children.Count();
            Assert.Equal(0, count);

            context.mockAdlsClient.CreateFile(FakeFileUri.AbsolutePath, IfExists.Overwrite);
            children = fs.GetChildren(FakeDirUri);
            count = children.Count();
            Assert.Equal(1, count);
        }
        
        [Fact]
        public void TestCreateUriForPath()
        {
            const string dirStructure = "dir/fakefile";
            Uri createdUri = fs.CreateUriForPath(dirStructure);
            Assert.Equal(createdUri, new Uri($"adl://adlAccount/{dirStructure}"));
        }

        [Fact]
        public void TestGetFileStatusThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => fs.GetFileStatus(null));
        }

        private sealed class TestContext
        {
            public readonly AdlsClient mockAdlsClient = MockAdlsClient.GetMockClient();
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
                TestDataLakeStoreClient.GetReference().ReturnsForAnyArgs(mockAdlsClient);
                TestDataLakeStoreClient.AccountFQDN.Returns("adlAccount");
                var fs = injector.GetInstance<AzureDataLakeFileSystem>();
                return fs;
            }
        }
    }
}
