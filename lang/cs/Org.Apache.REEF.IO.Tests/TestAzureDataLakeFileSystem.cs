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
        private readonly static Uri FakeUri = new Uri("http://fake.com");

        [Fact]
        public void TestDoesNotExists()
        {
            var testContext = new TestContext();
            Assert.False(testContext.GetAdlsFileSystem().Exists(FakeUri));
        }

        [Fact]
        public void TestExists()
        {
            var testContext = new TestContext();

            testContext.mockAdlsClient.CreateFile(FakeUri.ToString(), IfExists.Overwrite);
            Assert.True(testContext.GetAdlsFileSystem().Exists(FakeUri));
        }

        private sealed class TestContext
        {
            public readonly AdlsClient mockAdlsClient = MockAdlsClient.GetMockClient();

            public IFileSystem GetAdlsFileSystem()
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
