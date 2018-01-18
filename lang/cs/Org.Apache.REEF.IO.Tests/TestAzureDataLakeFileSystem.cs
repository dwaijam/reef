using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.DataLake.Store.MockAdlsFileSystem;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.AzureDataLake;
using Org.Apache.REEF.IO.FileSystem.AzureDatLake;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.IO.Tests
{
    public sealed class TestAzureDataLakeFileSystem
    {
        private readonly static Uri FakeBaseUri = new Uri("http://fakeadls.com");
        private readonly static Uri FakeFileUri = new Uri("http://fakeadls.com/dir/fakefile");
        private readonly static Uri FakeDirUri = new Uri("http://fakeadls.com/dir/");
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
            Assert.True(false, "Test not implemented.");
        }

        [Fact]
        public void TestCreate()
        {
            fs.Create(FakeFileUri);
            Assert.True(context.mockAdlsClient.CheckExists(FakeFileUri.ToString()));
            var directoryEntry = context.mockAdlsClient.GetDirectoryEntry(FakeDirUri.ToString());
            Assert.Equal(DirectoryEntryType.FILE, directoryEntry.Type);
        }

        [Fact]
        public void TestCreateFileUnderDirectory()
        {
            // Checks when file is created, directory in path was properly created too
            fs.Create(FakeFileUri);
            Assert.True(context.mockAdlsClient.CheckExists(FakeDirUri.ToString()));
            var directoryEntry = context.mockAdlsClient.GetDirectoryEntry(FakeDirUri.ToString());
            Assert.Equal(DirectoryEntryType.DIRECTORY, directoryEntry.Type);
        }

        [Fact]
        public void TestDelete()
        {
            context.mockAdlsClient.CreateFile(FakeFileUri.ToString(), IfExists.Overwrite);
            fs.Delete(FakeFileUri);
            Assert.False(context.mockAdlsClient.CheckExists(FakeFileUri.ToString()));
        }
        
        [Fact]
        public void TestDeleteException()
        {
            // Delete a file that doesn't exist.
            Assert.Throws<IOException>(() => fs.Delete(FakeFileUri));
        }

        [Fact]
        public void TestFileDoesNotExists()
        {
            Assert.False(context.GetAdlsFileSystem().Exists(FakeFileUri));
        }

        [Fact]
        public void TestExists()
        {
            context.mockAdlsClient.CreateFile(FakeFileUri.ToString(), IfExists.Overwrite);
            Assert.True(fs.Exists(FakeFileUri));
        }

        [Fact]
        public void TestCopy()
        {
            // Setup
            Uri src = new Uri("http://fakeadls.src.com/dir/fakefile");
            context.mockAdlsClient.CreateFile(src.ToString(), IfExists.Fail);
            Assert.True(context.mockAdlsClient.CheckExists(src.ToString()));
            Assert.False(context.mockAdlsClient.CheckExists(FakeFileUri.ToString()));

            fs.Copy(src, FakeBaseUri);
            Assert.True(context.mockAdlsClient.CheckExists(FakeFileUri.ToString()));
        }

        [Fact]
        public void TestCopyFromLocal()
        {
            fs.CopyFromLocal("dir/fakefile", FakeBaseUri);
            Assert.True(context.mockAdlsClient.CheckExists(FakeFileUri.ToString()));
        }

        [Fact]
        public void TestCopyToLocal()
        {
            context.mockAdlsClient.CreateFile(FakeFileUri.ToString(), IfExists.Overwrite);
            Assert.True(File.Exists("dir/fakefile"));
        }

        [Fact]
        public void TestCreateDirectory()
        {
            fs.CreateDirectory(FakeDirUri);
            Assert.True(context.mockAdlsClient.CheckExists(FakeDirUri.ToString()));
            
            // check if it is a directory and not a file
            var directoryEntry = context.mockAdlsClient.GetDirectoryEntry(FakeDirUri.ToString());
            Assert.Equal(DirectoryEntryType.DIRECTORY, directoryEntry.Type); 
        }

        [Fact]
        public void TestDeleteDirectory()
        {
            context.mockAdlsClient.CreateDirectory(FakeDirUri.ToString());
            Assert.True(context.mockAdlsClient.CheckExists(FakeDirUri.ToString()), "Test setup failed: did not successfully create directory to delete.");
            fs.Delete(FakeDirUri);
            Assert.False(context.mockAdlsClient.CheckExists(FakeDirUri.ToString()), "Test to delete adls directory failed.");
        }

        [Fact]
        public void TestDeleteDirectoryException()
        {
            // Delete a directory that doesn't exist.
            Assert.Throws<IOException>(() => fs.DeleteDirectory(FakeDirUri));
        }

        [Fact]
        public void TestGetChildren()
        {
            context.mockAdlsClient.CreateDirectory(FakeDirUri.ToString());
            var children = fs.GetChildren(FakeDirUri);
            int count = children.Count();
            Assert.Equal(0, count);

            context.mockAdlsClient.CreateFile(FakeFileUri.ToString(), IfExists.Overwrite);
            children = fs.GetChildren(FakeDirUri);
            count = children.Count();
            Assert.Equal(1, count);
        }
        
        [Fact]
        public void TestCreateUriForPath()
        {
            const string dirStructure = "dir/fakefile";
            Uri createdUri = fs.CreateUriForPath(dirStructure);
            Assert.Equal(FakeFileUri, new Uri(FakeBaseUri, createdUri));
        }

        [Fact]
        public void TestGetFileStatusThrowsException()
        {
            Assert.Throws<ArgumentException>(() => fs.GetFileStatus(null));
        }

        private sealed class TestContext
        {
            public readonly AdlsClient mockAdlsClient = MockAdlsClient.GetMockClient();

            public AzureDataLakeFileSystem GetAdlsFileSystem()
            {
                var conf = AzureDataLakeFileSystemConfiguration.ConfigurationModule.Build();
                var injector = TangFactory.GetTang().NewInjector(conf);
                injector.BindVolatileInstance(mockAdlsClient);
                var fs = injector.GetInstance<AzureDataLakeFileSystem>();
                return fs;
            }
        }
    }
}
