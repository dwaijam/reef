using Org.Apache.REEF.Common.Evaluator.Parameters;
using Org.Apache.REEF.IO.FileSystem.AzureBlob.Parameters;
using Org.Apache.REEF.IO.FileSystem.AzureBlob.RetryPolicy;
using Org.Apache.REEF.IO.FileSystem.AzureDataLake;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IO.FileSystem.AzureDatLake
{
        internal sealed class AzureDataLakeFileSystemConfigurationProvider : IConfigurationProvider
        {
            private readonly IConfiguration _configuration;

            [Inject]
            private AzureDataLakeFileSystemConfigurationProvider()
            {
                _configuration = TangFactory.GetTang().NewConfigurationBuilder()
                    .BindImplementation(GenericType<IFileSystem>.Class, GenericType<AzureDataLakeFileSystem>.Class)
                    .BindSetEntry<EvaluatorConfigurationProviders, AzureDataLakeFileSystemConfigurationProvider, IConfigurationProvider>()
                    .Build();
            }

            public IConfiguration GetConfiguration()
            {
                return _configuration;
            }
        }
}
