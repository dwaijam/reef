using System;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.DataLake.Store.MockAdlsFileSystem;
using NSubstitute;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.AzureDataLake;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.IO.Tests
{
    public sealed class TestAzureDataLakeFileSystem
    {
        private readonly static Uri FakeUri = new Uri("http://fake.com/f1");

        [Fact]
        public void TestDoesNotExist()
        {
            var testContext = new TestContext();
            Assert.False(testContext.GetAdlsFileSystem().Exists(FakeUri));
        }

        [Fact]
        public void TestExists()
        {
            var testContext = new TestContext();
            testContext.mockAdlsClient.CreateFile(FakeUri.AbsolutePath, IfExists.Overwrite);
            Assert.True(testContext.GetAdlsFileSystem().Exists(FakeUri));
        }

        private sealed class TestContext
        {
            public readonly AdlsClient mockAdlsClient = MockAdlsClient.GetMockClient();
            public readonly IDataLakeStoreClient TestDataLakeStoreClient = Substitute.For<IDataLakeStoreClient>();

            public IFileSystem GetAdlsFileSystem()
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
                var fs = injector.GetInstance<AzureDataLakeFileSystem>();
                return fs;
            }
        }
    }
}
