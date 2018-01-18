using System;
using System.Collections.Generic;
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
        private readonly static Uri FakeDirUri = new Uri("http://fakeadls.com/fakedir/");
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
        
        //// TO TEST: if file is under a directory, create directory => check that directory is created

        [Fact]
        public void TestDelete()
        {
            context.mockAdlsClient.CreateFile(FakeFileUri.ToString(), IfExists.Overwrite);
            fs.Delete(FakeFileUri);
            Assert.False(context.mockAdlsClient.CheckExists(FakeFileUri.ToString()));
        }
        
        //// TO TEST: trying to delete something that doesn't exists => throw exception (IOException)

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
            Assert.True(false, "Test not implemented.");
        }

        [Fact]
        public void TestCopyToLocal()
        {
            Assert.True(false, "Test not implemented.");
        }

        [Fact]
        public void TestCopyFromLocal()
        {
            Assert.True(false, "Test not implemented.");
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
        
        //// TO TEST: delete directory that doesn't exist => io exception

        [Fact]
        public void TestGetChildren()
        {
            context.mockAdlsClient.CreateFile(FakeFileUri.ToString(), IfExists.Overwrite);
            var children = fs.GetChildren(FakeDirUri);
            int count = children.Count();
            Assert.Equal(1, count);
        }
        
        //// TO TEST: 0 children, file does not exist.

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
